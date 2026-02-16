package internal

import (
	"context"
	"fmt"
	"os/exec"
)

func CreateRequest(ctx context.Context, dir string, filename string, lang string) (cmd *exec.Cmd, err error) {

	if lang == "golang" {

		args := []string{
			"run",
			"--rm",
			"--network", "none",
			"--memory", "128m",
			"--cpus", "0.5",
			"--pids-limit", "64",
			"--read-only",
			"--tmpfs", "/tmp:rw,size=16m",
			"-v", dir + ":/code",
			"gorunner",
			"go", "run", "/code/" + filename,
		}

		return exec.CommandContext(ctx, "docker", args...), nil
	} else if lang == "python" {

		args := []string{
			"run",
			"--rm",
			"--network", "none",
			"--memory", "128m",
			"--cpus", "0.5",
			"--pids-limit", "64",
			"--read-only",
			"--tmpfs", "/tmp:rw,size=16m",
			"-v", dir + ":/code",
			"pyrunner",
			"python", "/code/" + filename,
		}

		return exec.CommandContext(ctx, "docker", args...), nil

	} else {
		return nil, fmt.Errorf("unknown lang")
	}
}
