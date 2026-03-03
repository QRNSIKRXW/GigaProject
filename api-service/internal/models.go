package internal

type Task struct {
	Id      string
	Code    string
	OwnerId string
	Lang    string
}

type TaskStatus struct {
	Code   string `json:"code"`
	Status string `json:"status"`
	Result string `json:"result"`
	Error  string `json:"error"`
	Lang   string `json:"lang"`
}

var Wrong_res = TaskStatus{
	Code:   "",
	Status: "not_found",
	Result: "_",
	Error:  "_",
}

type RunResponse struct {
	Id string `json:"id"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

const MaxCodeSize = 64 * 1024 // 64 KB limit for code submission
