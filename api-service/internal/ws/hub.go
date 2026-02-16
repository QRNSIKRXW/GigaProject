package ws

import (
	"sync"

	"github.com/gorilla/websocket"
)

type BroadcastMessage struct {
	TaskId string
	Line   string
}

type Hub struct {
	Clients    map[string]map[*Client]bool
	register   chan *Client
	unregister chan *Client
	broadcast  chan BroadcastMessage
	done       chan bool
	wg         sync.WaitGroup
}

var Upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func (h *Hub) StartHub() {
	h.wg.Add(1)
	defer h.wg.Done()

	for {
		select {

		case client := <-h.register:
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
			if clients, ok := h.Clients[msg.TaskId]; ok {
				for client := range clients {
					client.Send <- []byte(msg.Line)
				}
			}

		case <-h.done:
			return
		}
	}
}

func CreateHub() *Hub {
	return &Hub{
		Clients:    make(map[string]map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan BroadcastMessage),
		done:       make(chan bool),
		wg:         sync.WaitGroup{},
	}
}

func (h *Hub) Shutdown() {

	close(h.done)

	h.wg.Wait()

	for _, clientMap := range h.Clients {
		for client := range clientMap {
			client.WebSocket.Close()
			close(client.Send)
		}
	}

	h.Clients = map[string]map[*Client]bool{}

}
