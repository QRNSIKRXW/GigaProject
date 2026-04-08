package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

var semaphore = make(chan struct{}, 4)

var req struct {
	Code      string `json:"code"`
	SessionID string `json:"session_id"`
}

func main() {
	lang := os.Getenv("LANG_TYPE")
	if lang == "" {
		lang = "golang"
	}

	fmt.Printf("Starting API server for %s on :8080\n", lang)

	http.HandleFunc("/exec", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		if req.Code == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "empty code"})
			return
		}

		select {
		case semaphore <- struct{}{}:
			go func() {
				defer func() { <-semaphore }()
				output, err := runCode(req.Code, lang)
				if err != nil {
					fmt.Printf("Execution error: %v\n", err)
				}
				if output != "" {
					fmt.Print(output)
				}
			}()
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"error": "server busy"})
			return
		}

		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
	})

	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Printf("Server error: %v\n", err)
	}
}

func runCode(code, lang string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Создаем уникальную временную директорию
	tmpDir := fmt.Sprintf("/tmp/run_%d", time.Now().UnixNano())

	var cmd *exec.Cmd

	switch lang {
	case "golang":

		cmd = exec.CommandContext(ctx, "sh", "-c", fmt.Sprintf(`
mkdir -p %s
cd %s
cat > main.go
go build -o main main.go
./main
rm -rf %s
`, tmpDir, tmpDir, tmpDir))

	case "python":
		cmd = exec.CommandContext(ctx, "sh", "-c", fmt.Sprintf(`
mkdir -p %s
cd %s
cat > main.py
python3 main.py
rm -rf %s
`, tmpDir, tmpDir, tmpDir))

	default:
		return "", fmt.Errorf("unknown language: %s", lang)
	}

	cmd.Stdin = strings.NewReader(code)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("execution error: %v, output: %s", err, string(output)) // Есть проблема с гонками вывода
	}

	return string(output), nil
}
