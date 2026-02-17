import { useState } from "react";
import { runCode } from "./api";
import Runner from "./Runner";

export default function App() {
  const [code, setCode] = useState(`package main

import "fmt"

func main() {
    fmt.Println("hello from container")
}`);
  const [taskId, setTaskId] = useState(null);

  async function handleRun() {
    const res = await runCode(code, "golang");
    setTaskId(res.id);
  }

  return (
    <div style={{ padding: 20, maxWidth: 800, margin: "0 auto" }}>
      <h1>Giga Runner</h1>

      <textarea
        value={code}
        onChange={(e) => setCode(e.target.value)}
        style={{
          width: "100%",
          height: 200,
          fontFamily: "monospace",
          fontSize: 14,
          padding: 10
        }}
      />

      <button
        onClick={handleRun}
        style={{
          marginTop: 10,
          padding: "10px 20px",
          fontSize: 16,
          cursor: "pointer"
        }}
      >
        Запустить
      </button>

      {taskId && <Runner taskId={taskId} />}
    </div>
  );
}
