package ws

import (
	"log"
	"net/http"
)

func ConnectionHandler(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		ws, err := Upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("WS upgrade error:", err)
			return
		}

		client := &Client{
			Hub:       hub,
			WebSocket: ws,
			Send:      make(chan []byte, 32),
		}

		go client.writePump()
		go client.readPump()
	}
}
