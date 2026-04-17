# fixes.md

Дата: 2026-04-17

Ниже перечислены все изменения, внесенные в рамках текущего цикла фиксов.

## 1) API / Backend

### `api-service/internal/handlers.go`
- Добавлен `clientIPFromRequest(...)` для корректного извлечения IP:
  - приоритет: `X-Forwarded-For` -> `X-Real-IP` -> `RemoteAddr` без порта.
- `GoRunHandler` и `PyRunHandler` переведены на новый разбор IP (устранен обход rate limit через смену ephemeral порта).
- Исправлена пагинация history:
  - `stop := start + limit - 1`.
- Исправлен ключ чтения задач в history:
  - было `HGetAll(ctx, val)`;
  - стало `HGetAll(ctx, "task:"+val)`.

### `api-service/main.go`
- Исправлен graceful shutdown HTTP-сервера:
  - убран сценарий с уже отмененным контекстом;
  - добавлен `context.WithTimeout(..., 5s)`;
  - добавлен лог ошибок `srv.Shutdown(...)`.

### `api-service/internal/ws/hub.go`
- Усилен WebSocket origin-check:
  - вместо `CheckOrigin: true` теперь разрешается only same-host origin.
- Добавлена функция `isAllowedOrigin(...)`.
- Улучшена очистка подписок при backpressure/drop клиента:
  - если клиент удален и подписчиков не осталось, подписка `task:*` корректно останавливается.

### `api-service/internal/ws/client.go`
- Убрано двойное закрытие `Send`-канала в `writePump` (исключен риск panic `close of closed channel`).

## 2) Worker / PubSub flow

### `go-worker/internal/executor.go`
- Исправлена подписка на реальные каналы логов:
  - было: `task:<id>`;
  - стало: `task:<id>:out` и `task:<id>:err`.
- Удален невалидный фильтр по `line.TaskId` (в payload его нет).
- Добавлена проверка decode ответа runner-service (`invalid runner response`).
- Нормализован разбор ошибки при `runRes.Status == "error"`:
  - если есть output -> `failed` + извлеченная строка ошибки;
  - если output нет -> `error` с fallback сообщением.
- Нормализован output (`TrimRight` по `\n`).

## 3) Runner Service

### `runner-service/main.go`
- Добавлено чтение количества раннеров из env:
  - `GO_RUNNER_COUNT` (default `3`),
  - `PY_RUNNER_COUNT` (default `2`).
- Добавлена валидация env-значений (`>=0`, integer).
- Добавлена защита от конфигурации `0`/`0` (фейл fast).

### `runner-service/internal/redis.go`
- Исправлен лог publish-канала (корректный формат имени канала).

### `runner-service/internal/docker_pool.go`
- Добавлен контроль конкурентного доступа (`worker.sem`) в `RunCodeWithLogs`.
- Ошибки `WritePubSub(...)` больше не игнорируются.
- Критически обновлена логика `startContainer(...)`:
  - stale контейнеры удаляются и пересоздаются,
  - неиспользуемые/старые экземпляры не переиспользуются silently.
- Изменены image tags раннеров:
  - `gigaproject-gorunner:local`,
  - `gigaproject-pyrunner:local`.
- Усилены runtime-параметры sandbox контейнеров:
  - `--cpus=1.0` (было `0.3`),
  - `--memory=512m` / `--memory-swap=512m` (было `128m`),
  - `--pids-limit=256` (было `64`),
  - `--tmpfs /tmp:rw,exec,size=64m` (добавлен `exec`),
  - env для кэша и HOME:
    - `HOME=/tmp`,
    - `XDG_CACHE_HOME=/tmp/.cache`,
    - `GOCACHE=/tmp/.cache/go-build`,
    - `GOMODCACHE=/tmp/.cache/go-mod`.
- Эти изменения устраняют ошибки:
  - `read-only file system` для Go cache,
  - `resource temporarily unavailable` (fork/exec),
  - `Permission denied` при запуске `./main` из `/tmp`.

## 4) Docker Compose / Infrastructure

### `Docker-compose.yaml`
- Удален устаревший `version`.
- Для `runner-service` добавлены env:
  - `GO_RUNNER_COUNT=${GO_RUNNER_COUNT:-3}`,
  - `PY_RUNNER_COUNT=${PY_RUNNER_COUNT:-2}`.
- Добавлены build-only сервисы для гарантированной сборки нужных runner image:
  - `gorunner-image` (`gigaproject-gorunner:local`),
  - `pyrunner-image` (`gigaproject-pyrunner:local`).
- Обновлен `depends_on` у `runner-service`:
  - ожидание успешного завершения build-only сервисов,
  - зависимость от `redis`.
- Зафиксировано имя сети `appnet` (`name: appnet`) для согласованности с Docker CLI в runner-service.
- Удален `internal: true` у основной сети `appnet`, чтобы восстановить публикацию портов на localhost (`80`, `8000`, `9000`).

## 5) Frontend

### `frontend/src/App.tsx`
- `ws://` сделан протокол-зависимым:
  - `wss://` при `https:`, иначе `ws://`.
- Добавлены `useRef` для:
  - активного WebSocket,
  - таймера poll.
- Добавлен cleanup при unmount и перед новым запуском:
  - закрытие старого WebSocket,
  - очистка таймеров polling.
- После terminal-статусов (`done`/`failed`/`error`) WebSocket закрывается корректно.

## 6) Python Worker module identity

### `py-worker/go.mod`
- Исправлен module path:
  - было `.../go-worker`;
  - стало `.../py-worker`.

### `py-worker/main.go`
- Исправлен импорт `internal` на py-worker путь.
- Исправлен лог запуска метрик (`py-worker metrics ...`).

## 7) README / Документация

### `README.md`
- Обновлена секция переменных окружения:
  - добавлены `GO_RUNNER_COUNT`, `PY_RUNNER_COUNT`.
- Добавлен пример масштабирования runner-пула без ручного редактирования compose.
- Синхронизированы формулировки по docker network/sandbox, чтобы соответствовали фактической конфигурации.
- Обновлены замечания по WebSocket security (same-origin вместо прежнего `CheckOrigin: true`).

## 8) Фактические результаты после фиксов

- Восстановлена доступность:
  - `http://localhost/` (nginx),
  - `http://localhost:8000` (api-service),
  - `http://localhost:9000` (runner-service).
- Восстановлено авто-создание пула runner-контейнеров по env.
- Исполнение Python задач стабильно (`status=done`).
- Исполнение Go задач стабилизировано (устранены ошибки cache/read-only, pids, noexec, memory/cpu bottlenecks).

## 9) Дополнительные релизные фиксы (последний цикл)

### `runner-service/internal/docker_pool.go`
- Убрана схема с безусловным `docker rm -f` при старте контейнеров.
- Добавлен безопасный lifecycle контейнеров:
  - inspect -> reuse/start/restart,
  - recreate только при несовместимом image или неустранимой нездоровости.
- Добавлен readiness-check `waitRunnerReady(...)` перед допуском контейнера в пул.
- Добавлены вспомогательные функции:
  - `inspectField(...)`,
  - `removeContainer(...)`,
  - `runNewContainer(...)`,
  - `startExistingContainer(...)`,
  - `restartExistingContainer(...)`.
- Результат: устранены race-сценарии, когда задача отправлялась в ещё не готовый раннер (`connection refused` / `EOF`).

### `api-service/internal/handlers.go`
- Усилена защита от spoofing IP в rate-limit:
  - forwarding-заголовки (`X-Real-IP`, `X-Forwarded-For`) учитываются только от trusted-proxy источников.
- Добавлены функции:
  - `isTrustedProxyIP(...)`,
  - `firstForwardedIP(...)`.
- Для прямых клиентских подключений источник IP — `RemoteAddr` (без доверия к заголовкам клиента).

### `frontend/src/App.tsx`
- Добавлен fallback вывода при `status=done`:
  - если WebSocket-сообщения не пришли/потерялись, UI подставляет `result` из polling-ответа `/api/result/{id}`.
- Результат: устранен сценарий «успех без видимого вывода».

### `go-worker/internal/executor.go`
- Удалена эвристика, ошибочно помечавшая успешные задачи как `failed` только из-за текста `error:` в stdout.
- Результат: снижены ложные срабатывания на валидный пользовательский вывод.

### Новые тесты

#### `api-service/internal/handlers_test.go`
- Добавлены unit-тесты для anti-spoof логики IP:
  - direct-client игнорирует spoofed заголовки,
  - trusted-proxy использует `X-Real-IP`,
  - fallback на первый валидный `X-Forwarded-For`.

#### `runner-service/internal/docker_pool_test.go`
- Добавлены unit-тесты readiness-проверки:
  - успешный ответ endpoint считается готовностью,
  - недоступный endpoint корректно таймаутится с ошибкой.

### Проверка
- Прогнаны `go test ./...`:
  - `api-service` (включая новые тесты) — OK,
  - `runner-service` (включая новые тесты) — OK,
  - `go-worker` — OK.
