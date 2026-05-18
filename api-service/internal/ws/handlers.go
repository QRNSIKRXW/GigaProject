package ws

import (
	"log"
	"net/http"
)

func ConnectionHandler(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ownerCookie, err := r.Cookie("owner")
		if err != nil || ownerCookie.Value == "" {
			http.Error(w, "missing owner cookie", http.StatusUnauthorized)
			return
		}

		wsConn, err := Upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("WS upgrade error:", err)
			return
		}

		client := &Client{
			Hub:       hub,
			WebSocket: wsConn,
			Send:      make(chan []byte, 32),
			OwnerID:   ownerCookie.Value,
		}

		// 🔥 важно: НЕ регистрируем тут
		// регистрация произойдет после получения taskId в readPump

		go client.writePump()
		go client.readPump()
	}
}
