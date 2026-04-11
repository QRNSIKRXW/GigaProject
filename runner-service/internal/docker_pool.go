package internal

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type Worker struct {
	ContainerName string
	Lang          string
	APIEndpoint   string
	sem           chan struct{}
	HttpClient    *http.Client
}

type Pool struct {
	workers []*Worker
	idx     uint32
}

// ---------- ERROR CLEANER ----------

func cleanErrorLine(line string) string {
	line = strings.TrimSpace(line)

	if line == "" {
		return ""
	}

	// убираем docker / shell шум
	if strings.Contains(line, "exit status") {
		return ""
	}

	if strings.Contains(line, "sh:") {
		return ""
	}

	// Go build noise
	if strings.Contains(line, "command-line-arguments") {
		return "compile error"
	}

	// убираем file:line:column префиксы
	if idx := strings.Index(line, ".go:"); idx != -1 {
		parts := strings.SplitN(line[idx:], " ", 2)
		if len(parts) == 2 {
			return parts[1]
		}
		return "syntax error"
	}

	// python / runtime fallback
	return line
}

// ---------- CONTAINER START ----------

func startContainer(name, image, langType string) error {
	cmd := exec.Command("docker", "inspect", name)
	if err := cmd.Run(); err == nil {
		startCmd := exec.Command("docker", "start", name)
		out, err := startCmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("failed to start container %s: %v, output: %s", name, err, string(out))
		}
		return nil
	}

	run := exec.Command(
		"docker", "run", "-d",
		"--name", name,
		"--network=appnet",

		"--memory=128m",
		"--memory-swap=128m",
		"--cpus=0.3",
		"--pids-limit=64",

		"--read-only",
		"--tmpfs", "/tmp:rw,size=64m",
		"--security-opt=no-new-privileges",
		"--cap-drop=ALL",

		"-e", fmt.Sprintf("LANG_TYPE=%s", langType),
		image,
	)

	out, err := run.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to run container %s: %v, output: %s", name, err, string(out))
	}

	time.Sleep(1 * time.Second)
	return nil
}

// ---------- POOL INIT ----------

func NewPool(numGo, numPy int) (*Pool, error) {
	const workerConcurrency = 4

	p := &Pool{}

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	for i := 0; i < numGo; i++ {
		name := fmt.Sprintf("gorunner-%d", i)

		if err := startContainer(name, "gorunner", "golang"); err != nil {
			return nil, err
		}

		p.workers = append(p.workers, &Worker{
			ContainerName: name,
			Lang:          "golang",
			APIEndpoint:   fmt.Sprintf("http://%s:8080/exec", name),
			sem:           make(chan struct{}, workerConcurrency),
			HttpClient:    client,
		})
	}

	for i := 0; i < numPy; i++ {
		name := fmt.Sprintf("pyrunner-%d", i)

		if err := startContainer(name, "pyrunner", "python"); err != nil {
			return nil, err
		}

		p.workers = append(p.workers, &Worker{
			ContainerName: name,
			Lang:          "python",
			APIEndpoint:   fmt.Sprintf("http://%s:8080/exec", name),
			sem:           make(chan struct{}, workerConcurrency),
			HttpClient:    client,
		})
	}

	return p, nil
}

// ---------- RUN ----------

func (p *Pool) RunCode(ctx context.Context, worker *Worker, code, lang, sessionID string) error {
	select {
	case worker.sem <- struct{}{}:
		defer func() { <-worker.sem }()
	case <-ctx.Done():
		return ctx.Err()
	}

	reqBody, err := json.Marshal(map[string]string{
		"code":       code,
		"session_id": sessionID,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", worker.APIEndpoint, bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := worker.HttpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("bad status: %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

// ---------- LOG STREAM ----------

func (p *Pool) RunCodeWithLogs(
	client *redis.Client,
	ctx context.Context,
	code string,
	sessionID string,
	lang string,
) (string, error) {

	worker := p.getWorker(lang)
	if worker == nil {
		return "error", fmt.Errorf("no worker available")
	}

	reqBody, _ := json.Marshal(map[string]string{
		"code": code,
	})

	req, err := http.NewRequestWithContext(ctx, "POST", worker.APIEndpoint, bytes.NewReader(reqBody))
	if err != nil {
		return "error", err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := worker.HttpClient.Do(req)
	if err != nil {
		return "error", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "error", fmt.Errorf("bad status: %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)

	hadOutput := false
	errorCount := 0

	for scanner.Scan() {
		line := scanner.Text()

		switch {
		case strings.HasPrefix(line, "OUT:"):
			msg := strings.TrimPrefix(line, "OUT:")
			msg = strings.TrimSpace(msg)
			if msg == "" {
				continue
			}

			hadOutput = true
			WritePubSub(client, msg, "task:"+sessionID+":out")

		case strings.HasPrefix(line, "ERR:"):
			msg := strings.TrimPrefix(line, "ERR:")
			msg = cleanErrorLine(msg)

			if msg == "" {
				continue
			}

			errorCount++
			WritePubSub(client, msg, "task:"+sessionID+":err")

		case line == "END":
			if hadOutput {
				return "done", nil
			}

			if errorCount > 0 {
				// если были ошибки — уже отправили их в ERR stream
				return "error", nil
			}

			return "error", fmt.Errorf("no output")
		}
	}

	if err := scanner.Err(); err != nil {
		return "error", err
	}

	return "error", fmt.Errorf("unexpected end")
}

// ---------- WORKER SELECTION ----------

func (p *Pool) getWorker(lang string) *Worker {
	var list []*Worker

	for _, w := range p.workers {
		if w.Lang == lang {
			list = append(list, w)
		}
	}

	if len(list) == 0 {
		return nil
	}

	idx := atomic.AddUint32(&p.idx, 1)
	return list[int(idx)%len(list)]
}

func (p *Pool) GetWorkerCount(lang string) int {
	count := 0
	for _, w := range p.workers {
		if w.Lang == lang {
			count++
		}
	}
	return count
}

func (p *Pool) GetWorkersStatus() map[string]interface{} {
	status := make(map[string]interface{})
	var workers []map[string]interface{}

	for _, w := range p.workers {
		workers = append(workers, map[string]interface{}{
			"name":     w.ContainerName,
			"lang":     w.Lang,
			"endpoint": w.APIEndpoint,
			"load":     len(w.sem),
			"capacity": cap(w.sem),
		})
	}

	status["workers"] = workers
	status["total"] = len(p.workers)

	return status
}
