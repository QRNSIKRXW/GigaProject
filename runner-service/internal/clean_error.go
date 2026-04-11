package internal

import (
	"strings"
)

func CleanError(out string) string {
	lines := strings.Split(out, "\n")

	var cleaned []string

	for _, l := range lines {

		// Go compiler noise
		if strings.Contains(l, "command-line-arguments") {
			continue
		}
		if strings.HasPrefix(l, "sh:") {
			continue
		}
		if strings.Contains(l, "exit status") {
			continue
		}
		if strings.Contains(l, "not found") {
			continue
		}
		if strings.TrimSpace(l) == "---" {
			continue
		}

		cleaned = append(cleaned, l)
	}

	return strings.TrimSpace(strings.Join(cleaned, "\n"))
}
