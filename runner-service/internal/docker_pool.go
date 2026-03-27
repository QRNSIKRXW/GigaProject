package internal

import (
	"bufio"
	"context"
	"encoding/base64"
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

		"--network", "none",
		"--read-only",
		"--tmpfs", "/sandbox:rw, size=128m",

		"--user", "1000:1000",

		"--cap-drop=ALL",
		"--security-opt=no-new-privileges",
		"--security-opt=seccomp=default",

		"--memory", "256m",
		"--cpus", "0.5",
		"--cpu-period", "100000",
		"--cpu-quota", "50000",
		"--pids-limit", "128",

		"--ulimit", "nofile=64:64",
		"--ulimit", "nproc=64:64",

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

func (p *Pool) RunDocker(client *redis.Client, parentCtx context.Context, code string, id string, lang string) (string, string) {

	execCtx, cancel := context.WithTimeout(parentCtx, 15*time.Second)
	defer cancel()

	worker := p.nextWorker(lang)

	if worker == nil {
		return "error", "no available worker"
	}

	worker.sem <- struct{}{}
	defer func() { <-worker.sem }()

	tmpDir := "/sandbox/tmp"

	codeB64 := base64.StdEncoding.EncodeToString([]byte(code))

	var cmdArgs []string

	if lang == "golang" {

		cmdArgs = []string{"sh", "-c",
			fmt.Sprintf(`
FILE=%s/%s.go
BIN=%s/%s_bin
echo %s | base64 -d > $FILE
mkdir -p %s/go-cache
GOTMPDIR=%s GOCACHE=%s/go-cache go build -o $BIN $FILE
$BIN
rm -f $FILE $BIN
`,
				tmpDir, id,
				tmpDir, id,
				codeB64,
				tmpDir,
				tmpDir,
				tmpDir)}

	} else if lang == "python" {

		cmdArgs = []string{"sh", "-c",
			fmt.Sprintf(`
FILE=%s/%s.py
echo %s | base64 -d > $FILE
python3 -I -B $FILE
rm -f $FILE
`,
				tmpDir, id,
				codeB64)}

	} else {
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

			line := scanner.Text()

			hadOutput.Store(true)

			_ = WritePubSub(client, line, id)
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
