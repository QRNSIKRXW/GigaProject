package internal

import (
	"context"
	"fmt"
	"os/exec"
)

func CreateRequest(ctx context.Context, dir string, filename string, lang string) (*exec.Cmd, error) {

	if lang == "golang" {

		args := []string{
			"run",
			"--rm",
			"--network", "none",
			"--memory", "128m",
			"--cpus", "0.5",
			"--pids-limit", "64",
			"-v", dir + ":/code",
			"gorunner",
			"/code/app", // запускаем бинарник
		}

		return exec.CommandContext(ctx, "docker", args...), nil
	}

	if lang == "python" {

		args := []string{
			"run",
			"--rm",
			"--network", "none",
			"--memory", "128m",
			"--cpus", "0.5",
			"--pids-limit", "64",
			"-v", dir + ":/code",
			"pyrunner",
			"python3", "/code/main.py",
		}

		return exec.CommandContext(ctx, "docker", args...), nil
	}

	return nil, fmt.Errorf("unknown lang")
}
