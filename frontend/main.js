const runBtn = document.getElementById('runBtn');
const codeEl = document.getElementById('code');
const langEl = document.getElementById('lang');
const streamEl = document.getElementById('stream');
const resultEl = document.getElementById('result');
const historyBtn = document.getElementById('loadHistory');
const historyEl = document.getElementById('history');

let currentWs = null;

runBtn.onclick = async () => {
  streamEl.textContent = '';
  resultEl.textContent = '';

  const lang = langEl.value;
  const code = codeEl.value;

  const url = lang === 'go' ? '/api/run/go' : '/api/run/python';

  // === ВАЖНО: отправляем cookie ===
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    credentials: 'include',
    body: JSON.stringify({ code }),
  });

  if (!resp.ok) {
    const err = await resp.json().catch(() => ({}));
    resultEl.textContent = 'Ошибка запуска: ' + (err.error || resp.statusText);
    return;
  }

  const data = await resp.json();
  const taskId = data.id;

  // === WebSocket для стрима ===
  if (currentWs) {
    currentWs.close();
  }

  const wsProto = location.protocol === 'https:' ? 'wss' : 'ws';
  const ws = new WebSocket(`${wsProto}://${location.host}/ws`);
  currentWs = ws;

  ws.onopen = () => {
    ws.send(JSON.stringify({ taskId }));
  };

  ws.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      const line = msg.result || msg.line || event.data;
      streamEl.textContent += line + '\n';
    } catch {
      streamEl.textContent += event.data + '\n';
    }
  };

  ws.onclose = () => {
    console.log('WebSocket closed');
  };

  // === Опрашиваем финальный результат ===
  pollResult(taskId);
};

async function pollResult(taskId) {
  const interval = setInterval(async () => {
    const resp = await fetch(`/api/result/${taskId}`, {
      credentials: 'include',
    });

    if (!resp.ok) return;

    const data = await resp.json();

    if (data.status === 'done' || data.status === 'error' || data.status === 'failed') {
      clearInterval(interval);

      resultEl.textContent =
        'Статус: ' + data.status +
        '\n\nРезультат:\n' + (data.result || '') +
        '\n\nОшибка:\n' + (data.error || '');

      if (currentWs) currentWs.close();
    }
  }, 1000);
}

historyBtn.onclick = async () => {
  historyEl.innerHTML = '';

  const resp = await fetch('/api/history', {
    credentials: 'include',
  });

  if (!resp.ok) {
    historyEl.innerHTML = '<li>Ошибка загрузки истории</li>';
    return;
  }

  const data = await resp.json();
  const tasks = data.tasks || [];

  tasks.forEach((t) => {
    const li = document.createElement('li');
    li.textContent = `[${t.lang}] ${t.status} — ${t.id}`;
    historyEl.appendChild(li);
  });
};
