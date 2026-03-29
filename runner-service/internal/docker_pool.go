package internal

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type Worker struct {
	ContainerName string
	Lang          string
	sem           chan struct{}
}

type Pool struct {
	workers []*Worker
	idx     uint32
}

func NewPool(numGo, numPy int) (*Pool, error) {

	const workerConcurrency = 4

	p := &Pool{}

	for i := 0; i < numGo; i++ {
		name := fmt.Sprintf("gorunner-%d", i)

		if err := startContainer(name, "gorunner"); err != nil {
			return nil, err
		}

		p.workers = append(p.workers, &Worker{
			ContainerName: name,
			Lang:          "golang",
			sem:           make(chan struct{}, workerConcurrency),
		})
	}

	for i := 0; i < numPy; i++ {
		name := fmt.Sprintf("pyrunner-%d", i)

		if err := startContainer(name, "pyrunner"); err != nil {
			return nil, err
		}

		p.workers = append(p.workers, &Worker{
			ContainerName: name,
			Lang:          "python",
			sem:           make(chan struct{}, workerConcurrency),
		})
	}

	return p, nil
}

func startContainer(name, image string) error {

	cmd := exec.Command("docker", "inspect", name)
	if err := cmd.Run(); err == nil {
		return nil
	}

	run := exec.Command(
		"docker", "run", "-d",
		"--name", name,

		"--tmpfs", "/tmp:rw,size=256m",

		"--network", "none",
		"--cap-drop=ALL",
		"--security-opt=no-new-privileges",

		"--memory", "256m",
		"--cpus", "0.5",
		"--pids-limit", "128",

		image,
		"sleep", "infinity",
	)

	out, err := run.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to start container %s: %v, output: %s", name, err, string(out))
	}

	return nil
}

func (p *Pool) nextWorker(lang string) *Worker {

	start := atomic.AddUint32(&p.idx, 1)

	for i := 0; i < len(p.workers); i++ {
		w := p.workers[(int(start)+i)%len(p.workers)]
		if w.Lang == lang {
			return w
		}
	}

	return nil
}

func (p *Pool) RunDocker(
	client *redis.Client,
	parentCtx context.Context,
	code string,
	id string,
	lang string,
) (string, string) {

	execCtx, cancel := context.WithTimeout(parentCtx, 15*time.Second)
	defer cancel()

	worker := p.nextWorker(lang)
	if worker == nil {
		return "error", "no available worker"
	}

	worker.sem <- struct{}{}
	defer func() { <-worker.sem }()

	var cmdArgs []string

	switch lang {

	case "golang":
		cmdArgs = []string{
			"sh", "-c",
			fmt.Sprintf(`
set -e

mkdir -p /workspace

cat > /workspace/main.go << 'EOF'
%s
EOF

cd /workspace

go build -o main_bin main.go

./main_bin
`, code),
		}

	case "python":
		cmdArgs = []string{
			"sh", "-c",
			fmt.Sprintf(`
set -e

mkdir -p /workspace

cat > /workspace/main.py << 'EOF'
%s
EOF

python3 /workspace/main.py
`, code),
		}

	default:
		return "error", "unknown language"
	}

	runCmd := exec.CommandContext(
		execCtx,
		"docker",
		append([]string{"exec", worker.ContainerName}, cmdArgs...)...,
	)

	stdoutPipe, err := runCmd.StdoutPipe()
	if err != nil {
		return "error", "stdout pipe error"
	}

	stderrPipe, err := runCmd.StderrPipe()
	if err != nil {
		return "error", "stderr pipe error"
	}

	if err := runCmd.Start(); err != nil {
		return "error", fmt.Sprintf("docker exec failed: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	var hadOutput atomic.Bool

	readPipe := func(pipe io.Reader) {
		defer wg.Done()

		scanner := bufio.NewScanner(pipe)
		scanner.Buffer(make([]byte, 0, 1024), 1024*1024)

		for scanner.Scan() {
			hadOutput.Store(true)
			_ = WritePubSub(client, scanner.Text(), id)
		}
	}

	go readPipe(stdoutPipe)
	go readPipe(stderrPipe)

	waitCh := make(chan error, 1)

	go func() {
		waitCh <- runCmd.Wait()
	}()

	select {

	case <-execCtx.Done():
		wg.Wait()

		if hadOutput.Load() {
			return "done", ""
		}
		return "error", "timeout"

	case err := <-waitCh:
		wg.Wait()

		if err != nil {
			return "error", err.Error()
		}
		return "done", ""
	}
}
