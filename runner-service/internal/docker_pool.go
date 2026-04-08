// internal/pool.go
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
	"sync"
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

// createNetwork создает Docker сеть для контейнеров если её нет
func createNetwork() error {
	cmd := exec.Command("docker", "network", "inspect", "code-runner-net")
	if err := cmd.Run(); err == nil {
		// Сеть существует
		return nil
	}

	// Создаем сеть
	cmd = exec.Command("docker", "network", "create", "code-runner-net")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to create network: %v, output: %s", err, string(out))
	}

	return nil
}

// startContainer запускает контейнер с API сервером
func startContainer(name, image, langType string) error {
	// Проверяем существование контейнера
	cmd := exec.Command("docker", "inspect", name)
	if err := cmd.Run(); err == nil {
		// Контейнер существует, проверяем что он запущен
		startCmd := exec.Command("docker", "start", name)
		if out, err := startCmd.CombinedOutput(); err != nil {
			return fmt.Errorf("failed to start existing container %s: %v, output: %s", name, err, string(out))
		}
		return nil
	}

	// Запускаем новый контейнер в общей сети
	run := exec.Command(
		"docker", "run", "-d",
		"--name", name,
		"--network", "code-runner-net",
		"--tmpfs", "/tmp:rw,size=256m,exec",
		"--cap-add=ALL", // Даем все права (временно)
		"--security-opt=seccomp:unconfined",
		"--memory", "256m",
		"--cpus", "0.5",
		"--pids-limit", "128",
		"-e", fmt.Sprintf("LANG_TYPE=%s", langType),
		image,
	)

	out, err := run.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to start container %s: %v, output: %s", name, err, string(out))
	}

	// Ждем, пока API сервер внутри контейнера запустится
	time.Sleep(2 * time.Second)

	return nil
}

func NewPool(numGo, numPy int) (*Pool, error) {
	const workerConcurrency = 4
	p := &Pool{}

	// Создаем Docker сеть
	if err := createNetwork(); err != nil {
		return nil, fmt.Errorf("failed to create network: %v", err)
	}

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: 30 * time.Second,
	}

	// Запускаем Go воркеры
	for i := 0; i < numGo; i++ {
		name := fmt.Sprintf("gorunner-%d", i)

		if err := startContainer(name, "gorunner", "golang"); err != nil {
			return nil, fmt.Errorf("failed to start go worker %s: %v", name, err)
		}

		p.workers = append(p.workers, &Worker{
			ContainerName: name,
			Lang:          "golang",
			APIEndpoint:   fmt.Sprintf("http://%s:8080/exec", name),
			sem:           make(chan struct{}, workerConcurrency),
			HttpClient:    client,
		})
	}

	// Запускаем Python воркеры
	for i := 0; i < numPy; i++ {
		name := fmt.Sprintf("pyrunner-%d", i)

		if err := startContainer(name, "pyrunner", "python"); err != nil {
			return nil, fmt.Errorf("failed to start python worker %s: %v", name, err)
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

func (p *Pool) RunCode(ctx context.Context, code, lang, sessionID string) error {
	worker := p.getWorker(lang)
	if worker == nil {
		return fmt.Errorf("no worker for %s", lang)
	}

	// Захватываем семафор воркера
	select {
	case worker.sem <- struct{}{}:
		defer func() { <-worker.sem }()
	case <-ctx.Done():
		return ctx.Err()
	}

	// Формируем запрос
	reqBody, err := json.Marshal(map[string]string{
		"code":       code,
		"session_id": sessionID,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST",
		worker.APIEndpoint,
		bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Отправляем запрос
	resp, err := worker.HttpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	// Проверяем статус
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("bad status: %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

// RunCodeWithLogs полный цикл: отправка кода + чтение логов
func (p *Pool) RunCodeWithLogs(
	client *redis.Client,
	ctx context.Context,
	code string,
	sessionID string,
	lang string,
) (string, error) {

	// 1. Отправляем код через HTTP
	if err := p.RunCode(ctx, code, lang, sessionID); err != nil {
		return "error", err
	}

	// 2. Получаем воркер для чтения логов
	worker := p.getWorker(lang)
	if worker == nil {
		return "error", fmt.Errorf("no worker found")
	}

	// 3. Читаем логи из контейнера
	cmd := exec.CommandContext(ctx, "docker", "logs", "--follow", "--tail", "0", worker.ContainerName)

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return "error", fmt.Errorf("failed to get stdout pipe: %v", err)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return "error", fmt.Errorf("failed to get stderr pipe: %v", err)
	}

	if err := cmd.Start(); err != nil {
		return "error", fmt.Errorf("failed to start docker logs: %v", err)
	}
	defer cmd.Process.Kill()

	var wg sync.WaitGroup
	wg.Add(2)

	var hadOutput atomic.Bool
	var outputMu sync.Mutex
	var outputs []string

	readPipe := func(pipe io.Reader, streamType string) {
		defer wg.Done()
		scanner := bufio.NewScanner(pipe)
		scanner.Buffer(make([]byte, 0, 1024), 1024*1024)

		for scanner.Scan() {
			line := scanner.Text()
			hadOutput.Store(true)

			outputMu.Lock()
			outputs = append(outputs, line)
			outputMu.Unlock()

			// Отправляем в Redis PubSub
			log.Println("writing to pubsub:", sessionID)
			if err := WritePubSub(client, line, sessionID); err != nil {
				// Логируем ошибку но продолжаем
				fmt.Printf("Failed to write to Redis: %v\n", err)
			}
		}

		if err := scanner.Err(); err != nil {
			fmt.Printf("Error reading %s: %v\n", streamType, err)
		}
	}

	go readPipe(stdoutPipe, "stdout")
	go readPipe(stderrPipe, "stderr")

	// Ждем завершения чтения с таймаутом
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Ждем немного, чтобы собрать начальный вывод
	time.Sleep(100 * time.Millisecond)

	select {
	case <-done:
		if hadOutput.Load() {
			return "done", nil
		}
		return "error", fmt.Errorf("no output from container")
	case <-time.After(15 * time.Second):
		return "error", fmt.Errorf("timeout waiting for output")
	case <-ctx.Done():
		return "error", ctx.Err()
	}
}

func (p *Pool) getWorker(lang string) *Worker {
	// Собираем всех воркеров нужного языка
	var langWorkers []*Worker
	for _, w := range p.workers {
		if w.Lang == lang {
			langWorkers = append(langWorkers, w)
		}
	}

	if len(langWorkers) == 0 {
		return nil
	}

	// Round-robin выбор
	idx := atomic.AddUint32(&p.idx, 1)
	return langWorkers[int(idx)%len(langWorkers)]
}

// GetWorkerCount возвращает количество воркеров для языка
func (p *Pool) GetWorkerCount(lang string) int {
	count := 0
	for _, w := range p.workers {
		if w.Lang == lang {
			count++
		}
	}
	return count
}

// GetWorkersStatus возвращает статус всех воркеров
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
