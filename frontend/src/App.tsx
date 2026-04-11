import React, { useEffect, useMemo, useState } from "react"

type Lang = "golang" | "python"

interface HistoryItem {
  id: string
  lang: Lang
  code: string
  createdAt: number
}

const API_BASE = "/api"
const WS_URL = `ws://${location.host}/ws`

const defaultGo = `package main

import "fmt"

func main() {
    fmt.Println("HELLO")
}
`

const defaultPy = `print("HELLO")`

const App: React.FC = () => {
  const [lang, setLang] = useState<Lang>("golang")
  const [code, setCode] = useState<string>(defaultGo)
  const [output, setOutput] = useState<string[]>([])
  const [isRunning, setIsRunning] = useState(false)
  const [taskId, setTaskId] = useState<string>("")
  const [history, setHistory] = useState<HistoryItem[]>([])
  const [activeHistoryId, setActiveHistoryId] = useState<string | null>(null)

  useEffect(() => {
    const raw = localStorage.getItem("runner_history")
    if (!raw) return
    try {
      const parsed: HistoryItem[] = JSON.parse(raw)
      setHistory(parsed)
    } catch {}
  }, [])

  const saveHistory = (items: HistoryItem[]) => {
    setHistory(items)
    localStorage.setItem("runner_history", JSON.stringify(items))
  }

  const addToHistory = (item: HistoryItem) => {
    const updated = [item, ...history].slice(0, 50)
    saveHistory(updated)
  }

  const handleLangChange = (newLang: Lang) => {
    setLang(newLang)
    if (newLang === "golang" && code.trim() === defaultPy.trim()) {
      setCode(defaultGo)
    }
    if (newLang === "python" && code.trim() === defaultGo.trim()) {
      setCode(defaultPy)
    }
  }

  const prettyDate = (ts: number) =>
    new Date(ts).toLocaleString("ru-RU", {
      hour: "2-digit",
      minute: "2-digit",
      day: "2-digit",
      month: "2-digit"
    })

  // Поллинг статуса задачи
  const pollStatus = (id: string) => {
    const check = async () => {
      try {
        const resp = await fetch(`${API_BASE}/result/${id}`, {
          method: "GET",
          credentials: "include"
        })

        if (!resp.ok) {
          // 403, 500 и т.п.
          const text = await resp.text().catch(() => "")
          setOutput(prev => [
            ...prev,
            `[status error] ${resp.status} ${text || ""}`.trim()
          ])
          setIsRunning(false)
          return
        }

        const data = await resp.json()

        // ожидаем поля: status, result, error
        const status: string = data.status
        const result: string = data.result ?? ""
        const error: string = data.error ?? ""

        if (status === "done") {
          setIsRunning(false)
          return
        }


        if (status === "failed" || status === "error") {
          if (error) {
            setOutput(prev => [...prev, "---", `[error] ${error}`])
          }
          setIsRunning(false)
          return
        }

        // pending — повторяем через 500 мс
        setTimeout(check, 500)
      } catch (e) {
        setOutput(prev => [...prev, "[status request error]"])
        setIsRunning(false)
      }
    }

    check()
  }

  const runCode = async () => {
  if (!code.trim()) return

  setIsRunning(true)
  setOutput([])
  setActiveHistoryId(null)

  try {
    const runUrl =
      lang === "golang"
        ? `${API_BASE}/run/go`
        : `${API_BASE}/run/python`

    const resp = await fetch(runUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ code })
    })

    if (!resp.ok) {
      const text = await resp.text().catch(() => "")
      setOutput(prev => [
        ...prev,
        `[api error] ${resp.status} ${text || ""}`.trim()
      ])
      setIsRunning(false)
      return
    }

    const data = await resp.json()
    const newTaskId: string = data.id

    if (!newTaskId) {
      setOutput(prev => [...prev, "[api error] no taskId"])
      setIsRunning(false)
      return
    }

    setTaskId(newTaskId)

    // === WebSocket ===
    const ws = new WebSocket(WS_URL)

    ws.onopen = () => {
      ws.send(JSON.stringify({
        type: "subscribe",
        taskId: newTaskId
      }))
    }

    ws.onmessage = (event: MessageEvent) => {
  try {
    const msg = JSON.parse(event.data)

    if (typeof msg === "object" && msg.line !== undefined) {
      setOutput(prev => [...prev, msg.line])
    } else {
      setOutput(prev => [...prev, String(event.data)])
    }

  } catch {
    setOutput(prev => [...prev, String(event.data)])
  }
}

    ws.onerror = () => {
      setOutput(prev => [...prev, "[websocket error]"])
    }

    // === Статус ===
    pollStatus(newTaskId)

    addToHistory({
      id: newTaskId,
      lang,
      code,
      createdAt: Date.now()
    })
  } catch (err) {
    console.error(err)
    setOutput(prev => [...prev, "[request error]"])
    setIsRunning(false)
  }
}


  const loadFromHistory = (item: HistoryItem) => {
    setLang(item.lang)
    setCode(item.code)
    setActiveHistoryId(item.id)
    setOutput([])
    setTaskId(item.id)
    // при загрузке из истории можно при желании дернуть pollStatus(item.id),
    // но это уже опционально
  }

  const clearHistory = () => {
    saveHistory([])
    setActiveHistoryId(null)
  }

  const currentLangLabel = useMemo(
    () => (lang === "golang" ? "Go" : "Python"),
    [lang]
  )

  return (
    <div className="app-root">
      <header className="app-header">
        <div className="app-title">
          <span className="logo-dot" />
          <span>Code Runner</span>
        </div>
        <div className="app-header-right">
          <span className="badge">{currentLangLabel}</span>
        </div>
      </header>

      <div className="app-layout">
        <div className="panel panel-main">
          <div className="toolbar">
            <div className="toolbar-left">
              <button
                className={`pill ${lang === "golang" ? "pill-active" : ""}`}
                onClick={() => handleLangChange("golang")}
              >
                Go
              </button>
              <button
                className={`pill ${lang === "python" ? "pill-active" : ""}`}
                onClick={() => handleLangChange("python")}
              >
                Python
              </button>
            </div>
            <div className="toolbar-right">
              <button
                className={`btn-run ${isRunning ? "btn-run-disabled" : ""}`}
                onClick={runCode}
                disabled={isRunning}
              >
                {isRunning ? "Выполняется..." : "Запустить"}
              </button>
            </div>
          </div>

          <div className="editor-wrapper">
            <textarea
              className="editor"
              value={code}
              onChange={(e) => setCode(e.target.value)}
              spellCheck={false}
            />
          </div>

          <div className="output-wrapper">
            <div className="output-header">
              <span>Вывод</span>
              {isRunning && <span className="dot-live">● live</span>}
            </div>
            <div className="output-body">
              {output.length === 0 ? (
                <div className="output-placeholder">
                  Нажми «Запустить», чтобы увидеть вывод программы.
                </div>
              ) : (
                output.map((line, idx) => (
                  <div key={idx} className="output-line">
                    {line}
                  </div>
                ))
              )}
            </div>
          </div>
        </div>

        <div className="panel panel-side">
          <div className="history-header">
            <div>
              <div className="history-title">История запусков</div>
              <div className="history-subtitle">
                Локальная история в браузере
              </div>
            </div>
            <button
              className="btn-clear"
              onClick={clearHistory}
              disabled={history.length === 0}
            >
              Очистить
            </button>
          </div>

          <div className="history-list">
            {history.length === 0 ? (
              <div className="history-empty">
                Пока пусто. Запусти код — и он появится здесь.
              </div>
            ) : (
              history.map((item) => (
                <button
                  key={item.id}
                  className={
                    "history-item" +
                    (item.id === activeHistoryId ? " history-item-active" : "")
                  }
                  onClick={() => loadFromHistory(item)}
                >
                  <div className="history-item-top">
                    <span className="history-lang">
                      {item.lang === "golang" ? "Go" : "Python"}
                    </span>
                    <span className="history-date">
                      {prettyDate(item.createdAt)}
                    </span>
                  </div>
                  <div className="history-snippet">
                    {item.code.split("\n")[0].slice(0, 40) ||
                      "<пустая строка>"}
                  </div>
                </button>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

export default App
