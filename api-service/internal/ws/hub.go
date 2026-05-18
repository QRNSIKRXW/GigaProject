package ws

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

type BroadcastMessage struct {
	Type string `json:"type"` // stdout | stderr
	Line string `json:"line"`
}

type subscription struct {
	cancel context.CancelFunc
	count  int
}

type Hub struct {
	Clients    map[string]map[*Client]bool
	register   chan *Client
	unregister chan *Client
	broadcast  chan struct {
		taskId string
		data   []byte
	}
	done chan bool
	wg   sync.WaitGroup

	Rdb *redis.Client

	subs map[string]*subscription
	mu   sync.Mutex
}

var Upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return isAllowedOrigin(r)
	},
}

func isAllowedOrigin(r *http.Request) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return true
	}

	originURL, err := url.Parse(origin)
	if err != nil {
		return false
	}

	return strings.EqualFold(originURL.Host, r.Host)
}

func CreateHub(rdb *redis.Client) *Hub {
	return &Hub{
		Clients:    make(map[string]map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast: make(chan struct {
			taskId string
			data   []byte
		}, 256),
		done: make(chan bool),
		Rdb:  rdb,
		subs: make(map[string]*subscription),
	}
}

func (h *Hub) StartHub() {
	h.wg.Add(1)
	defer h.wg.Done()

	for {
		select {

		case client := <-h.register:

			if client.TaskId == "" {
				continue
			}

			if _, ok := h.Clients[client.TaskId]; !ok {
				h.Clients[client.TaskId] = make(map[*Client]bool)
			}

			h.Clients[client.TaskId][client] = true
			h.startSubscription(client.TaskId)

		case client := <-h.unregister:

			if clients, ok := h.Clients[client.TaskId]; ok {

				delete(clients, client)
				client.closeSend()

				if len(clients) == 0 {
					delete(h.Clients, client.TaskId)
					h.stopSubscription(client.TaskId)
				}
			}

		case msg := <-h.broadcast:

			if clients, ok := h.Clients[msg.taskId]; ok {

				for client := range clients {

					select {
					case client.Send <- msg.data:
					default:
						client.closeSend()
						delete(clients, client)
						if len(clients) == 0 {
							delete(h.Clients, msg.taskId)
							h.stopSubscription(msg.taskId)
						}
					}
				}
			}

		case <-h.done:
			return
		}
	}
}

// ---------- SUBSCRIPTIONS ----------

func (h *Hub) startSubscription(taskId string) {

	h.mu.Lock()
	defer h.mu.Unlock()

	sub, exists := h.subs[taskId]
	if exists {
		sub.count++
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	h.subs[taskId] = &subscription{
		cancel: cancel,
		count:  1,
	}

	go h.runPubSub(ctx, taskId)
}

func (h *Hub) stopSubscription(taskId string) {

	h.mu.Lock()
	defer h.mu.Unlock()

	sub, exists := h.subs[taskId]
	if !exists {
		return
	}

	sub.count--

	if sub.count <= 0 {
		sub.cancel()
		delete(h.subs, taskId)
	}
}

// ---------- 🔥 ГЛАВНОЕ ИСПРАВЛЕНИЕ ----------

func (h *Hub) runPubSub(ctx context.Context, taskId string) {

	outCh := "task:" + taskId + ":out"
	errCh := "task:" + taskId + ":err"

	sub := h.Rdb.Subscribe(ctx, outCh, errCh)
	defer sub.Close()

	ch := sub.Channel()

	for {
		select {

		case msg, ok := <-ch:
			if !ok {
				return
			}

			var payload struct {
				Line string `json:"line"`
			}

			if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
				continue
			}

			var msgType string

			if msg.Channel == outCh {
				msgType = "stdout"
			} else {
				msgType = "stderr"
			}

			log.Printf("Received message on channel %s: %s", msg.Channel, payload.Line)
			log.Printf("Broadcasting message of type %s for task %s", msgType, taskId)

			finalMsg, _ := json.Marshal(BroadcastMessage{
				Type: msgType,
				Line: payload.Line,
			})

			select {
			case h.broadcast <- struct {
				taskId string
				data   []byte
			}{taskId: taskId, data: finalMsg}:

			default:
				// backpressure drop
			}

		case <-ctx.Done():
			return
		}
	}
}

// ---------- SHUTDOWN ----------

func (h *Hub) Shutdown() {

	close(h.done)

	h.mu.Lock()
	for _, sub := range h.subs {
		sub.cancel()
	}
	h.mu.Unlock()

	time.Sleep(100 * time.Millisecond)
	h.wg.Wait()
}
