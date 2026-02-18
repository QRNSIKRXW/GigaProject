package internal

import (
	"fmt"
)

func CreateRequest(dir, filename, lang, id string) ([]string, string, error) {
	containerName := "task-" + id

	common := []string{
		"run",
		"-d",
		"--name", containerName,

		// сеть полностью отрублена
		"--network", "none",

		// ресурсы
		"--memory", "128m",
		"--cpus", "0.5",
		"--pids-limit", "64",

		// файловая система
		"--read-only",                            // rootfs только для чтения
		"--tmpfs", "/tmp:rw,noexec,nosuid,nodev", // отдельный tmpfs для /tmp

		// безопасность
		"--security-opt", "no-new-privileges",
		"--cap-drop", "ALL",

		// ulimit'ы
		"--ulimit", "nofile=64:64",
		"--ulimit", "nproc=64:64",

		// код монтируем как rw
		"-v", dir + ":/code:rw",
	}

	switch lang {
	case "golang":
		args := append(common, "gorunner")
		return args, containerName, nil

	case "python":
		args := append(common, "pyrunner")
		return args, containerName, nil

	default:
		return nil, "", fmt.Errorf("unknown lang")
	}
}
