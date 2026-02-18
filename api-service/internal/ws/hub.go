package ws

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

type BroadcastMessage struct {
	TaskId string `json:"taskId"`
	Line   string `json:"line"`
}

type Hub struct {
	Clients    map[string]map[*Client]bool
	register   chan *Client
	unregister chan *Client
	broadcast  chan BroadcastMessage
	done       chan bool
	wg         sync.WaitGroup
	Rdb        *redis.Client
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
		broadcast:  make(chan BroadcastMessage),
		done:       make(chan bool),
		Rdb:        rdb,
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

		case client := <-h.unregister:
			if clients, ok := h.Clients[client.TaskId]; ok {
				delete(clients, client)
				if len(clients) == 0 {
					delete(h.Clients, client.TaskId)
				}
			}

		case msg := <-h.broadcast:
			log.Println("HUB BROADCAST:", msg.TaskId, msg.Line)

			payload, _ := json.Marshal(msg)
			if clients, ok := h.Clients[msg.TaskId]; ok {
				for client := range clients {
					select {
					case client.Send <- payload:
					default:
					}
				}
			}

		case <-h.done:
			return
		}
	}
}

func (h *Hub) Shutdown() {
	close(h.done)
	h.wg.Wait()
}
