# Code Sandbox – Онлайн песочница для выполнения кода

**Code Sandbox** – это распределённая система для безопасного выполнения пользовательского кода (Go и Python) с web-интерфейсом и real-time логированием результатов через WebSocket.

## 📋 Содержание

- [Обзор](#обзор)
- [Архитектура](#архитектура)
- [Быстрый старт](#быстрый-старт)
- [Компоненты](#компоненты)
- [API](#api)
- [WebSocket](#websocket)
- [Развёртывание](#развёртывание)
- [Конфигурация](#конфигурация)
- [Безопасность](#безопасность)
- [Troubleshooting](#troubleshooting)
- [Дополнительная документация](#дополнительная-документация)

---

## 🎯 Обзор

### Что это?

Веб-приложение, которое позволяет пользователям:
- Написать код на **Go** или **Python** в браузере
- Нажать "Run"
- Получить результат выполнения в реальном времени (через WebSocket)
- Сохранить историю выполненных задач (локально в браузере)

### Как это работает?

1. Юзер отправляет код через REST API (`/api/run/go` или `/api/run/python`)
2. Код **помещается в очередь** Redis Streams (`tasks:go` или `tasks:python`)
3. **Worker процесс** (go-worker или py-worker) берёт задачу из очереди
4. Отправляет на **runner-service** (компиляция + запуск в Docker)
5. Результат **транслируется через Redis Pub/Sub**
6. Браузер получает результат через **WebSocket в реальном времени**
7. Сохраняется в Redis на 10 минут (или в dead-letter при ошибке)

### Ключевые особенности

✅ **Безопасность**: Docker sandbox с отключённой сетью, ограничением CPU/RAM, без capabilities  
✅ **Real-time**: WebSocket для потока вывода  
✅ **Масштабируемость**: Любое количество воркеров, Redis очереди  
✅ **Мониторинг**: Prometheus метрики, Grafana dashboards  
✅ **История**: Сохранение последних 50 задач на клиенте  

---

## 🏗️ Архитектура

```
┌─────────────────────────────────────────────────────────────┐
│                        FRONTEND (React)                      │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Browser → REST API (/api/run/*)                    │   │
│  │         ↓                                             │   │
│  │  WebSocket Subscribe (task:ID) ← Redis Pub/Sub       │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                          ↓
        ┌─────────────────────────────────────┐
        │         NGINX (ProxyPass)           │
        │  Фронт + API Proxy + WS Upgrade    │
        └─────────────────────────────────────┘
                          ↓
        ┌─────────────────────────────────────┐
        │   API-SERVICE (Go port 8080)        │
        │  ┌────────────────────────────────┐ │
        │  │ HTTP Handlers:                 │ │
        │  │  - /api/run/go (POST)          │ │
        │  │  - /api/run/python (POST)      │ │
        │  │  - /api/result/{id} (GET)      │ │
        │  │  - /api/history (GET)          │ │
        │  │  - /ws (WebSocket)             │ │
        │  │  - /metrics (Prometheus)       │ │
        │  └────────────────────────────────┘ │
        │              ↓                       │
        │   Pub/Sub Hub (broadcasting)        │
        └─────────────────────────────────────┘
                  ↓           ↓
        ┌─────────────────┐  ┌──────────────────────┐
        │     REDIS       │  │  API-WORKER (Go)     │
        │  ┌───────────┐  │  │  ┌────────────────┐  │
        │  │ Stream:   │  │  │  │ Monitors:      │  │
        │  │ tasks:go  │  │  │  │ - Worker stats │  │
        │  │ tasks:py  │  │  │  │ - Health       │  │
        │  └───────────┘  │  │  └────────────────┘  │
        │  ┌───────────┐  │  └──────────────────────┘
        │  │ Pub/Sub:  │  │
        │  │ task:ID   │  │
        │  └───────────┘  │
        │  ┌───────────┐  │
        │  │ Hash:     │  │  ┌──────────────────────┐
        │  │ task:ID   │  │  │ GO-WORKER (Go)       │
        │  │ user:ID   │  │  │  ┌────────────────┐  │
        │  └───────────┘  │  │  │ Streams:       │  │
        │                 │  │  │ - Consume      │  │
        │  ┌───────────┐  │  │  │ - Recover      │  │
        │  │ Hash:     │  │  │  │ - Execute      │  │
        │  │ worker:ID │  │  │  │ - Result       │  │
        │  └───────────┘  │  │  └────────────────┘  │
        └─────────────────┘  └──────────────────────┘
                                      ↓
                        ┌─────────────────────────┐
                        │   PY-WORKER (Go)        │
                        │  Same as GO-WORKER      │
                        │  but for Python tasks   │
                        └─────────────────────────┘
                                      ↓
        ┌─────────────────────────────────────────────┐
        │    RUNNER-SERVICE (Go port 9000)            │
        │  ┌───────────────────────────────────────┐  │
        │  │ Handles:                              │  │
        │  │  1. Write code to /tmp/run-*/main.go  │  │
        │  │  2. Compile (for Go)                  │  │
        │  │  3. docker run (gorunner/pyrunner)    │  │
        │  │  4. Stream output to Redis Pub/Sub    │  │
        │  │  5. Cleanup (kill container)          │  │
        │  └───────────────────────────────────────┘  │
        └─────────────────────────────────────────────┘
                        ↓
        ┌─────────────────────────────────────────────┐
        │         DOCKER CONTAINERS                   │
        │  ┌─────────────────────────────────────┐   │
        │  │  gorunner image                     │   │
        │  │  - Isolated (--network none)        │   │
        │  │  - No capabilities (--cap-drop ALL) │   │
        │  │  - Limited: 128MB RAM, 0.5 CPUs     │   │
        │  │  - Timeout: 3 seconds               │   │
        │  └─────────────────────────────────────┘   │
        │  ┌─────────────────────────────────────┐   │
        │  │  pyrunner image                     │   │
        │  │  (Same restrictions)                │   │
        │  └─────────────────────────────────────┘   │
        └─────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│             MONITORING (Optional)                            │
│  ┌───────────────┐        ┌──────────────────────────────┐  │
│  │  Prometheus   │────────│  Grafana (port 3001)         │  │
│  │  (port 9090)  │        │  - Worker dashboards         │  │
│  │               │        │  - Task metrics              │  │
│  │ Scrapes:      │        └──────────────────────────────┘  │
│  │ - :8080/met.  │
│  │ - :9101/met.  │
│  │ - :9102/met.  │
│  └───────────────┘
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Быстрый старт

### Требования

- Docker & Docker Compose
- Go 1.21+ (для локальной разработки)
- Node.js 18+ (для фронтенда, если менять)

### Запуск

```bash
# 1. Клонируйте репо
git clone <repo>
cd project

# 2. Выполните docker images для gorunner и pyrunner
# (они используются в runner-service, должны быть готовы)
docker build -t gorunner docker/go-runner/
docker build -t pyrunner docker/python-runner/

# 3. Поднимите весь стек
docker-compose up -d

# 4. Проверьте статус
docker-compose logs -f api-service
```

### Тестирование

```bash
# Откройте браузер
http://localhost

# Или через curl
curl -X POST http://localhost/api/run/go \
  -H "Content-Type: application/json" \
  -d '{"code":"package main\n\nimport \"fmt\"\n\nfunc main() {\n  fmt.Println(\"Hello\")\n}"}'
```

---

## 🔨 Компоненты

### 1. **api-service** (Go)

**Назначение**: HTTP API и WebSocket hub

**Порт**: 8080  
**Метрики**: `/metrics`

**Endpoints**:
- `POST /api/run/go` – запустить Go код
- `POST /api/run/python` – запустить Python код
- `GET /api/result/{id}` – получить результат задачи
- `GET /api/history` – история юзера (с пагинацией)
- `GET /ws` – WebSocket подписка на задачу
- `GET /internal/health` – проверка здоровья

**Функциональность**:
- Rate limiting по IP (20 реквестов/2 минуты)
- Управление cookies для идентификации юзера
- WebSocket hub для broadcasting результатов
- Push задачи в Redis Streams

### 2. **go-worker** & **py-worker** (Go)

**Назначение**: Обработка задач из очереди

**Порты**: 9102 (go-worker), 9101 (py-worker)  
**Метрики**: `/metrics`

**Функциональность**:
- Слушает Redis Stream (`tasks:go` или `tasks:python`)
- Берёт задачу из очереди (consumer group)
- Отправляет на runner-service
- Сохраняет результат в Redis
- Восстанавливает потерянные задачи (recovery loop)
- Отправляет результат в dead-letter при превышении повторов

**Параметры**:
- `pendingIdle`: 30 сек (как долго висит задача перед восстановлением)
- `maxRetries`: 5 (максимум повторов)
- `pendingBatch`: 10 (сколько задач восстанавливать за раз)

### 3. **api-worker** (Go)

**Назначение**: Мониторинг состояния воркеров

**Порт**: 8080 (но внутри docker-compose)  
**Метрики**: `/metrics`

**Endpoints**:
- `GET /internals/workers` – список всех воркеров
- `GET /internals/workers/{id}` – инфо о конкретном воркере
- `GET /internals/workers/status?status=online` – воркеры по статусу
- `GET /internals/workers/stats` – агрегированная статистика

### 4. **runner-service** (Go)

**Назначение**: Компиляция и запуск кода в Docker

**Порт**: 9000  
**Метрики**: `/metrics`

**Endpoints**:
- `POST /` – запустить код в контейнере

**Процесс выполнения**:
1. Получает `{code, lang, taskId}`
2. Создаёт временную папку `/tmp/run-*/`
3. Пишет код в `main.go` или `main.py`
4. **Для Go**: компилирует на хосте (`GOOS=linux GOARCH=amd64`)
5. Запускает `docker run -d --name task-{id} ...` с контейнером
6. Читает логи (`docker logs -f`) и шлёт в Redis Pub/Sub
7. Ждёт завершения контейнера (max 3 сек)
8. Убивает и удаляет контейнер
9. Возвращает статус

**Docker constraints**:
```
--network none              # Нет сети
--memory 128m               # Макс 128 MB RAM
--cpus 0.5                  # Max 50% CPU
--cap-drop ALL              # Нет Linux capabilities
--read-only                 # Файловая система read-only
--tmpfs /tmp:...            # Только /tmp rw (но no exec)
--ulimit nofile=64:64       # Max 64 файловых дескриптора
--ulimit nproc=64:64        # Max 64 процесса
```

### 5. **Frontend** (React/Vite)

**Назначение**: Web-интерфейс

**Технология**: React 18 + TypeScript + Vite

**Функциональность**:
- Редактор кода (простой textarea, можно заменить на Monaco/CodeMirror)
- Выбор языка (Go / Python)
- История последних 50 задач (localStorage)
- Real-time вывод (WebSocket)
- История по каждой задаче

### 6. **Redis** (Docker image)

**Версия**: 7

**Использование**:
- `streams (tasks:go, tasks:python)` – очереди задач
- `hashes (task:{id})` – состояние и результаты задач
- `hashes (worker:{id})` – информация о воркере
- `lists (user:{id}:tasks)` – история юзера
- `sets (workers:set)` – множество активных воркеров
- `hashes (dead:{id})` – dead-letter для неудачных задач
- `pub/sub (task:{id})` – трансляция вывода

### 7. **Nginx** (Docker)

**Назначение**: Reverse Proxy + Static Files Serving

**Функциональность**:
- Проксирует `/api/*` на api-service
- Проксирует `/ws` на api-service (с WebSocket upgrade)
- Ограничивает `/internal/*` по IP (только 127.0.0.1)
- Отдаёт статику (фронтенд html/js/css)

---

## 📡 API

### POST `/api/run/go`

**Описание**: Запустить Go код

**Request**:
```json
{
  "code": "package main\nimport \"fmt\"\nfunc main() {\n  fmt.Println(\"Hello\")\n}"
}
```

**Response** (200):
```json
{
  "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

**Response** (413):
```json
{
  "error": "code too large"
}
```

**Response** (429 Too Many Requests):
```json
{
  "error": "rate limit exceeded"
}
```

**Cookie**: Автоматически устанавливается `owner` на 12 дней

---

### POST `/api/run/python`

Аналогично `/api/run/go`, но для Python кода.

---

### GET `/api/result/{id}`

**Описание**: Получить результат задачи

**Response** (200 – готова):
```json
{
  "status": "done",
  "result": "Hello\n",
  "error": "",
  "code": "...",
  "lang": "golang"
}
```

**Response** (202 – в очереди):
```json
{
  "status": "pending"
}
```

**Response** (404 – ошибка выполнения):
```json
{
  "status": "failed",
  "result": "...",
  "error": "panic: ...",
  "code": "..."
}
```

**Response** (404 – потеряна/expired):
```json
{
  "status": "not_found",
  "result": "_",
  "error": "_"
}
```

**Статусы**:
- `pending` – задача в очереди или обрабатывается
- `done` – успешно выполнена
- `failed` – ошибка (паника, traceback и т.п.)
- `not_found` – задача не найдена или истекла (TTL 10 мин)

---

### GET `/api/history`

**Описание**: История задач юзера (с пагинацией)

**Query params**:
- `cursor` (опционально) – ID задачи, начиная с которой читать дальше

**Response**:
```json
{
  "tasks": [
    {
      "id": "a1b2c3d4-...",
      "code": "...",
      "status": "done",
      "result": "Hello\n",
      "error": "",
      "lang": "golang"
    }
  ],
  "cursor": "next_task_id"  // null если больше нет
}
```

**Лимит**: 20 задач на странице  
**Хранение**: Последние 160 задач на юзера  
**Сортировка**: Новые сверху (LIFO)

---

### GET `/internal/health`

Возвращает `200 OK` с телом `"OK"`.

---

## 🔌 WebSocket

### Подписка на задачу

```javascript
const ws = new WebSocket('ws://localhost/ws');

ws.onopen = () => {
  ws.send(JSON.stringify({
    type: 'subscribe',
    taskId: 'a1b2c3d4-...'
  }));
};

ws.onmessage = (event) => {
  const msg = JSON.parse(event.data);
  // { taskId: "...", line: "output line" }
  console.log(msg.line);
};
```

### Сообщение формат

**От сервера** (per line of output):
```json
{
  "taskId": "a1b2c3d4-...",
  "line": "Hello from stdout"
}
```

### Жизненный цикл

1. Клиент открывает WebSocket
2. Отправляет `{"type":"subscribe", "taskId":"..."}`
3. Сервер регистрирует клиента в hub
4. Когда runner-service пишет в Redis Pub/Sub, hub брутит всем подписанным клиентам
5. При закрытии браузера или переходе – WebSocket закрывается, клиент удаляется из hub

### Таймауты

- **3 секунды**: максимальное время выполнения кода в контейнере
- Если код ещё выполняется après 3 сек – контейнер убивается, возвращается `"error": "timeout"`

---

## 🐳 Развёртывание

### Docker Compose (для разработки)

```bash
docker-compose up -d
docker-compose logs -f
docker-compose ps
```

**Порты**:
- `80` – nginx (frontend + API)
- `8000` – api-service напрямую (для отладки)
- `9090` – prometheus
- `3001` – grafana (user: admin, pass: admin)

### Kubernetes (для production-like среды)

Требуется создать:
1. Deployments для каждого сервиса
2. Services (ClusterIP для внутренних, LoadBalancer для nginx)
3. ConfigMaps для конфигурации
4. Secrets для чувствительных данных
5. PersistentVolumes для Redis/Grafana data

Пример chart можно начать с `helm create code-sandbox`.

---

## ⚙️ Конфигурация

### Переменные окружения

| Переменная | Сервис | Default | Описание |
|----------|---------|---------|---------|
| `REDIS_ADDR` | go-worker, py-worker, api-service, runner-service | `redis:6379` | Redis адрес |
| `RUNNER_URL` | go-worker, py-worker | `http://host.docker.internal:9000` | URL runner-service |

### Hardcoded параметры (нужна конфигурация)

| Параметр | Файл | Значение | Что менять? |
|----------|------|---------|-----------|
| Таймаут выполнения | `runner-service/runner.go:18` | 3 сек | Может быть меньше/больше |
| Rate limit | `api-service/handlers.go` | 20 рекв/2 мин | По запросу |
| Лимит памяти | `runner-service/args.go` | 128m | Для GoLang зависит от кода |
| Максимум CPU | `runner-service/args.go` | 0.5 | Может быть 1.0+ |
| TTL результата | `api-service/redis.go:49` | 10 мин | Redis Expire |
| Максимум задач в истории | `api-service/redis.go:53` | 160 | Для одного юзера |
| MaxCodeSize | `api-service/models.go` | 64 KB | Лимит кода |

---

## 🔒 Безопасность

### Песочница

✅ **Включено**:
- Docker isolation (--network none)
- Капабилитис дропнуты (--cap-drop ALL)
- Таймаут 3 сек (жёсткий лимит time)
- Ограничение памяти (128 MB)
- Ограничение CPU (0.5)
- Ограничение процессов (64)
- Read-only FS (кроме /tmp)
- No setuid, no new privileges

⚠️ **Расширение песочницы**:
- Скомпилировать Go код в **контейнере** (selinux/apparmor профайл)
- Использовать **seccomp** для доп. syscall filtering
- Запускать с unprivileged user

### API Security

⚠️ **Отсутствует**:
- ❌ HTTPS/TLS
- ❌ Аутентификация (только UUID cookie)
- ❌ CORS проверка (CheckOrigin: true)

✅ **Добавлено**:
- Rate limiting по IP
- Проверка размера кода (64 KB)
- Content-Type validation
- `/internal/*` ограничена в nginx по IP

### JWT / OAuth / Auth

Для production:
```go
// Пример middleware
func AuthMiddleware(next http.Handler) http.Handler {
  return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    token := r.Header.Get("Authorization")
    if token == "" {
      http.Error(w, "missing token", 401)
      return
    }
    // Validate JWT
    claims, err := validateJWT(token)
    if err != nil {
      http.Error(w, "invalid token", 401)
      return
    }
    // Pass user to next handler
    ctx := context.WithValue(r.Context(), "user", claims)
    next.ServeHTTP(w, r.WithContext(ctx))
  })
}
```

---

## 🐛 Troubleshooting

### Проблема: "Failed to execute task"

**Причины**:
1. Runner-service не доступен (проверьте `RUNNER_URL`)
2. Docker daemon не запущен
3. Image `gorunner`/`pyrunner` не существует

**Решение**:
```bash
docker ps | grep runner
docker images | grep gorunner
docker logs runner-service
```

---

### Проблема: "Code too large"

**Причина**: Код > 64 KB

**Решение**: Увеличить лимит в `api-service/models.go`:
```go
const MaxCodeSize = 256 * 1024 // 256 KB
```

---

### Проблема: WebSocket не получает вывод

**Причины**:
1. Контейнер выполнился раньше, чем клиент подписался
2. Redis Pub/Sub не работает
3. Network разрыв

**Решение**:
```bash
# Проверьте Redis
redis-cli PING
redis-cli PUBSUB CHANNELS

# Проверьте логи api-service
docker logs api-service | grep "PUBSUB"
```

---

### Проблема: Rate limit слишком строгий

**Решение**: Измените в `api-service/handlers.go`:
```go
if err := RateLimit(client, r.Context(), ip, 100); err != nil {  // было 20
```

---

### Проблема: Контейнеры не убираются

**Причина**: Runner-service упал или зависнул

**Решение**:
```bash
docker ps -a | grep "task-"
docker rm -f task-*
```

---

### Проблема: Redis переполнена

**Причина**: Слишком много старых задач, TTL не работает

**Решение**:
```bash
redis-cli
> FLUSHDB  # Опасно! Удалит всё
> CLIENT LIST  # Проверьте подключения
```

---

## 📈 Мониторинг

### Prometheus

Доступен на `http://localhost:9090`

**Метрики** собираются со всех сервисов на `/metrics`:
- go_goroutines
- go_memstats_heap_alloc_bytes
- process_resident_memory_bytes
- http_requests_total
- redis_commands_issued_total (если добавить)

### Grafana

Доступна на `http://localhost:3001` (admin:admin)

**Создайте dashboard with queries**:
```promql
# Активные горутины
go_goroutines{job="api-service"}

# Память (MB)
process_resident_memory_bytes / 1024 / 1024

# HTTP запросы
rate(http_requests_total[5m])
```

---

## 📚 Дополнительно

### Регулярные выражения для поиска ошибок

Файлы `containsUserError()` и `extractErrorLine()` определены в:
- `go-worker/internal/executor.go`
- `py-worker/internal/executor.go`

Ищут: `"panic"`, `"Traceback"`, `"error:"`

### Worker recovery

При падении worker'а (неудачный результат или timeout):
1. Задача остаётся в `pending` группе Stream'а
2. Recovery loop каждые 3 секунды проверяет зависшие задачи
3. Если задача зависла > 30 сек, её перехватывают другие воркеры
4. Если повторов > 5 – отправляется в dead-letter (`dead:{taskId}`)

### Dead-letter обработка

Если задача не выполнена после 5 попыток:
```go
client.HSet(ctx, "dead:"+task.Id, map[string]interface{}{
  "error": "too many retries",
  "code":  task.Code,
})
```

Результат виден в `/api/result/{id}` с `status: "failed"`.

---

## 🎓 Учебные материалы

### Как расширить?

1. **Добавить язык (Java, Rust, etc.)**:
   - Создать `java-runner` image
   - Добавить новый worker (java-worker)
   - Добавить endpoint `/api/run/java`

2. **Добавить сохранение результатов**:
   - Заменить Redis на PostgreSQL
   - Хранить user + task_id + code + result + timestamp
   - Добавить `/api/results` с фильтром по дате

3. **Добавить Auth**:
   - Подключить JWT library (https://github.com/golang-jwt/jwt)
   - Middleware для проверки токена
   - Frontend: хранить токен в localStorage

4. **Добавить HTTPS**:
   - Caddy вместо Nginx (автоматический Let's Encrypt)
   - Или рукавно: certbot + nginx cert renewal

---

## 👥 Контакты

Вопросы? Обращайтесь к разработчику материала.

---

**Последнее обновление**: 12 апреля 2026  
**Версия проекта**: 1.0.0
