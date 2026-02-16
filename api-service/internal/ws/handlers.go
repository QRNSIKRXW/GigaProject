package ws

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

type data struct {
	TaskId string `json:"taskId"`
}

func ConnectionHandler(hub *Hub, client *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		log.Println("WS CONNECTED")

		ws, err := Upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("WS CLOSE REASON: upgrade error:", err)
			http.Error(w, "upgrade failed", http.StatusBadGateway)
			return
		}

		log.Println("WS UPGRADED OK")

		_, msg, err := ws.ReadMessage()
		if err != nil {
			log.Println("WS CLOSE REASON: cannot read first message:", err)
			ws.Close()
			return
		}

		log.Println("WS FIRST MESSAGE:", string(msg))

		var msgData data
		if err := json.Unmarshal(msg, &msgData); err != nil {
			log.Println("WS CLOSE REASON: bad payload:", err)
			ws.WriteMessage(websocket.CloseMessage, []byte("bad payload"))
			ws.Close()
			return
		}

		log.Println("WS TASK ID:", msgData.TaskId)

		cookie, err := r.Cookie("owner")
		if err != nil {
			log.Println("WS CLOSE REASON: missing owner cookie")
			ws.WriteMessage(websocket.CloseMessage, []byte("missing owner cookie"))
			ws.Close()
			return
		}

		log.Println("WS OWNER COOKIE:", cookie.Value)

		savedOwner, _ := client.HGet(r.Context(), "task:"+msgData.TaskId, "owner").Result()
		if savedOwner != cookie.Value {
			log.Println("WS CLOSE REASON: unauthorized")
			ws.WriteMessage(websocket.CloseMessage, []byte("unauthorized"))
			ws.Close()
			return
		}

		log.Println("WS AUTH OK")

		hubClient := &Client{
			WebSocket: ws,
			Send:      make(chan []byte, 16),
			TaskId:    msgData.TaskId,
		}

		log.Println("WS REGISTER CLIENT")

		hub.register <- hubClient

		go hubClient.writePump(hub)
		go hubClient.readPump(hub)

		log.Println("WS CLIENT STARTED")
	}
}
