import { useEffect, useRef, useState } from "react";

export default function Runner({ taskId }) {
  const wsRef = useRef(null);
  const [lines, setLines] = useState([]);

  useEffect(() => {
    if (!taskId) return;

    const ws = new WebSocket("ws://localhost:8000/ws");
    wsRef.current = ws;

    ws.onopen = () => {
      ws.send(JSON.stringify({ taskId }));
    };

    ws.onmessage = (event) => {
      setLines((prev) => [...prev, event.data]);
    };

    ws.onerror = (err) => {
      console.error("WS error:", err);
    };

    ws.onclose = () => {
      console.log("WS closed");
    };

    return () => {};
  }, [taskId]);

  return (
    <div style={{ marginTop: 20 }}>
      <h3>Вывод:</h3>
      <pre
        style={{
          background: "#111",
          color: "#0f0",
          padding: 20,
          borderRadius: 8,
          minHeight: 200,
          whiteSpace: "pre-wrap"
        }}
      >
        {lines.join("\n")}
      </pre>
    </div>
  );
}