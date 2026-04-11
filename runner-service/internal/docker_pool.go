package internal

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
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

// ---------- Docker setup ----------

func createNetwork() error {
	cmd := exec.Command("docker", "network", "inspect", "code-runner-net")
	if err := cmd.Run(); err == nil {
		return nil
	}

	cmd = exec.Command("docker", "network", "create", "code-runner-net")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to create network: %v, output: %s", err, string(out))
	}

	return nil
}

func startContainer(name, image, langType string) error {
	cmd := exec.Command("docker", "inspect", name)
	if err := cmd.Run(); err == nil {
		startCmd := exec.Command("docker", "start", name)
		if out, err := startCmd.CombinedOutput(); err != nil {
			return fmt.Errorf("failed to start container %s: %v, output: %s", name, err, string(out))
		}
		return nil
	}

	run := exec.Command(
		"docker", "run", "-d",
		"--name", name,
		"--network", "code-runner-net",

		// 🔒 ЖЁСТКИЕ ЛИМИТЫ
		"--memory=128m",
		"--memory-swap=128m",
		"--cpus=0.3",
		"--pids-limit=64",

		// 🔒 безопасность
		"--read-only",
		"--tmpfs", "/tmp:rw,size=64m",
		"--security-opt=no-new-privileges",
		"--cap-drop=ALL",

		"-e", fmt.Sprintf("LANG_TYPE=%s", langType),
		image,
	)

	out, err := run.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to start container %s: %v, output: %s", name, err, string(out))
	}

	time.Sleep(2 * time.Second)
	return nil
}

// ---------- Pool ----------

func NewPool(numGo, numPy int) (*Pool, error) {
	const workerConcurrency = 4
	p := &Pool{}

	if err := createNetwork(); err != nil {
		return nil, err
	}

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

// ---------- Run code ----------

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

// ---------- Run with logs ----------

func (p *Pool) RunCodeWithLogs(
	client *redis.Client,
	ctx context.Context,
	code string,
	sessionID string,
	lang string,
) (string, error) {

	worker := p.getWorker(lang)
	if worker == nil {
		return "error", fmt.Errorf("no worker")
	}

	reqBody, _ := json.Marshal(map[string]string{
		"code": code,
	})

	req, _ := http.NewRequestWithContext(ctx, "POST", worker.APIEndpoint, bytes.NewReader(reqBody))
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

	var hadOutput bool

	for scanner.Scan() {
		line := scanner.Text()

		log.Println("STREAM:", line)

		switch {
		case strings.HasPrefix(line, "OUT:"):
			msg := strings.TrimPrefix(line, "OUT:")
			hadOutput = true
			WritePubSub(client, msg, "task:"+sessionID+":out")

		case strings.HasPrefix(line, "ERR:"):
			msg := strings.TrimPrefix(line, "ERR:")
			WritePubSub(client, msg, "task:"+sessionID+":err")

		case line == "END":
			if hadOutput {
				return "done", nil
			}
			return "error", fmt.Errorf("no output")
		}
	}

	if err := scanner.Err(); err != nil {
		return "error", err
	}

	return "error", fmt.Errorf("unexpected end")
}

// ---------- utils ----------

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
