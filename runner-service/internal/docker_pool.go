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

func inspectField(name, format string) (string, error) {
	cmd := exec.Command("docker", "inspect", "-f", format, name)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("inspect %s failed: %v, output: %s", name, err, string(out))
	}

	return strings.TrimSpace(string(out)), nil
}

func waitRunnerReady(endpoint string, timeout time.Duration) error {
	httpClient := &http.Client{
		Timeout: 2 * time.Second,
	}

	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodGet, endpoint, nil)

		resp, err := httpClient.Do(req)
		if err == nil {
			resp.Body.Close()
			return nil
		}

		lastErr = err
		time.Sleep(250 * time.Millisecond)
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("timeout")
	}

	return fmt.Errorf("runner %s not ready: %w", endpoint, lastErr)
}

func removeContainer(name string) error {
	cmd := exec.Command("docker", "rm", "-f", name)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to remove container %s: %v, output: %s", name, err, string(out))
	}
	return nil
}

func runNewContainer(name, image, langType string) error {
	run := exec.Command(
		"docker", "run", "-d",
		"--name", name,
		"--network=appnet",
		"--memory=512m",
		"--memory-swap=512m",
		"--cpus=1.0",
		"--pids-limit=256",
		"--read-only",
		"--tmpfs", "/tmp:rw,exec,size=64m",
		"--security-opt=no-new-privileges",
		"--cap-drop=ALL",
		"-e", fmt.Sprintf("LANG_TYPE=%s", langType),
		"-e", "HOME=/tmp",
		"-e", "XDG_CACHE_HOME=/tmp/.cache",
		"-e", "GOCACHE=/tmp/.cache/go-build",
		"-e", "GOMODCACHE=/tmp/.cache/go-mod",
		image,
	)

	runOut, runErr := run.CombinedOutput()
	if runErr != nil {
		return fmt.Errorf("failed to run container %s: %v, output: %s", name, runErr, string(runOut))
	}

	return nil
}

func startExistingContainer(name string) error {
	startCmd := exec.Command("docker", "start", name)
	startOut, startErr := startCmd.CombinedOutput()
	if startErr != nil {
		return fmt.Errorf("failed to start container %s: %v, output: %s", name, startErr, string(startOut))
	}
	return nil
}

func restartExistingContainer(name string) error {
	restartCmd := exec.Command("docker", "restart", name)
	restartOut, restartErr := restartCmd.CombinedOutput()
	if restartErr != nil {
		return fmt.Errorf("failed to restart container %s: %v, output: %s", name, restartErr, string(restartOut))
	}
	return nil
}

func startContainer(name, image, langType string) error {
	endpoint := fmt.Sprintf("http://%s:8080/exec", name)

	currentImage, imageErr := inspectField(name, "{{.Config.Image}}")
	if imageErr != nil {
		if err := runNewContainer(name, image, langType); err != nil {
			return err
		}
		return waitRunnerReady(endpoint, 20*time.Second)
	}

	if currentImage != image {
		if err := removeContainer(name); err != nil {
			return err
		}
		if err := runNewContainer(name, image, langType); err != nil {
			return err
		}
		return waitRunnerReady(endpoint, 20*time.Second)
	}

	running, err := inspectField(name, "{{.State.Running}}")
	if err != nil {
		return err
	}

	if running == "true" {
		if err := waitRunnerReady(endpoint, 15*time.Second); err == nil {
			return nil
		}

		// Running container exists, but API is not healthy. Try soft restart once.
		if err := restartExistingContainer(name); err == nil {
			if err := waitRunnerReady(endpoint, 20*time.Second); err == nil {
				return nil
			}
		}

		// Fallback: recreate container.
		if err := removeContainer(name); err != nil {
			return err
		}
		if err := runNewContainer(name, image, langType); err != nil {
			return err
		}
		return waitRunnerReady(endpoint, 20*time.Second)
	}

	if err := startExistingContainer(name); err != nil {
		return err
	}

	if err := waitRunnerReady(endpoint, 20*time.Second); err != nil {
		if err := removeContainer(name); err != nil {
			return err
		}
		if err := runNewContainer(name, image, langType); err != nil {
			return err
		}
		return waitRunnerReady(endpoint, 20*time.Second)
	}

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

		if err := startContainer(name, "gigaproject-gorunner:local", "golang"); err != nil {
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

		if err := startContainer(name, "gigaproject-pyrunner:local", "python"); err != nil {
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

	select {
	case worker.sem <- struct{}{}:
		defer func() { <-worker.sem }()
	case <-ctx.Done():
		return "error", ctx.Err()
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
		if recoverErr := recoverWorker(worker); recoverErr == nil {
			reqRetry, reqErr := http.NewRequestWithContext(ctx, "POST", worker.APIEndpoint, bytes.NewReader(reqBody))
			if reqErr == nil {
				reqRetry.Header.Set("Content-Type", "application/json")
				resp, err = worker.HttpClient.Do(reqRetry)
			}
		}
	}
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
			if err := WritePubSub(client, msg, "task:"+sessionID+":out"); err != nil {
				return "error", err
			}

		case strings.HasPrefix(line, "ERR:"):
			msg := strings.TrimPrefix(line, "ERR:")
			msg = cleanErrorLine(msg)

			if msg == "" {
				continue
			}

			errorCount++
			if err := WritePubSub(client, msg, "task:"+sessionID+":err"); err != nil {
				return "error", err
			}

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

func recoverWorker(worker *Worker) error {
	image := "gigaproject-pyrunner:local"
	langType := "python"
	if worker.Lang == "golang" {
		image = "gigaproject-gorunner:local"
		langType = "golang"
	}

	return startContainer(worker.ContainerName, image, langType)
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
