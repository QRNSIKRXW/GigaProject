package ws

import (
	"log"
	"net/http"
)

func ConnectionHandler(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		wsConn, err := Upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("WS upgrade error:", err)
			return
		}

		client := &Client{
			Hub:       hub,
			WebSocket: wsConn,
			Send:      make(chan []byte, 32),
		}

		// 🔥 важно: НЕ регистрируем тут
		// регистрация произойдет после получения taskId в readPump

		go client.writePump()
		go client.readPump()
	}
}
