package ws

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

type BroadcastMessage struct {
	TaskId string `json:"taskId"`
	Line   string `json:"line"`
}

type subscription struct {
	cancel context.CancelFunc
	count  int
}

type Hub struct {
	Clients    map[string]map[*Client]bool
	register   chan *Client
	unregister chan *Client
	broadcast  chan BroadcastMessage
	done       chan bool
	wg         sync.WaitGroup

	Rdb *redis.Client

	subs map[string]*subscription
	mu   sync.Mutex
}

// 🔥 лимиты
const maxClientsPerTask = 100

// 🔥 метрики
var (
	wsConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ws_active_connections",
		Help: "Active websocket connections",
	})

	wsDroppedMessages = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ws_dropped_messages_total",
		Help: "Dropped WS messages due to backpressure",
	})

	wsBroadcastTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ws_broadcast_total",
		Help: "Total broadcast messages",
	})
)

func init() {
	prometheus.MustRegister(wsConnections, wsDroppedMessages, wsBroadcastTotal)
}

var Upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func CreateHub(rdb *redis.Client) *Hub {
	return &Hub{
		Clients:    make(map[string]map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan BroadcastMessage, 256),
		done:       make(chan bool),
		Rdb:        rdb,
		subs:       make(map[string]*subscription),
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

			// 🔥 лимит
			if len(h.Clients[client.TaskId]) >= maxClientsPerTask {
				close(client.Send)
				continue
			}

			h.Clients[client.TaskId][client] = true
			wsConnections.Inc()

			h.startSubscription(client.TaskId)

		case client := <-h.unregister:

			if clients, ok := h.Clients[client.TaskId]; ok {

				if _, exists := clients[client]; exists {
					delete(clients, client)
					close(client.Send)
					wsConnections.Dec()
				}

				if len(clients) == 0 {
					delete(h.Clients, client.TaskId)
					h.stopSubscription(client.TaskId)
				}
			}

		case msg := <-h.broadcast:

			wsBroadcastTotal.Inc()

			payload, _ := json.Marshal(msg)

			if clients, ok := h.Clients[msg.TaskId]; ok {

				for client := range clients {

					select {
					case client.Send <- payload:
					default:
						wsDroppedMessages.Inc()
						close(client.Send)
						delete(clients, client)
						wsConnections.Dec()
					}
				}
			}

		case <-h.done:
			return
		}
	}
}

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

func (h *Hub) runPubSub(ctx context.Context, taskId string) {

	channel := "task:" + taskId

	sub := h.Rdb.Subscribe(ctx, channel)

	_, err := sub.Receive(ctx)
	if err != nil {
		return
	}

	ch := sub.Channel()

	defer sub.Close()

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return
			}

			var data BroadcastMessage
			if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
				continue
			}

			select {
			case h.broadcast <- data:
			default:
				wsDroppedMessages.Inc()
			}

		case <-ctx.Done():
			return
		}
	}
}

// 🔥 graceful shutdown
func (h *Hub) Shutdown() {

	close(h.done)

	h.mu.Lock()
	for _, sub := range h.subs {
		sub.cancel()
	}
	h.mu.Unlock()

	// даём pubsub время завершиться
	time.Sleep(100 * time.Millisecond)

	h.wg.Wait()
}
