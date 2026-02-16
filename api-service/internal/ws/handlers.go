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

		ws, err := Upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("upgrade error", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(w).Encode(map[string]string{"Error": "connection error"})
			return
		}

		_, msg, err := ws.ReadMessage()
		if err != nil {
			ws.Close()
			return
		}

		var msgData data
		if err := json.Unmarshal(msg, &msgData); err != nil {
			ws.WriteMessage(websocket.CloseMessage, []byte("bad payload"))
			ws.Close()
			return
		}

		cookie, err := r.Cookie("owner")
		if err != nil {
			ws.WriteMessage(websocket.CloseMessage, []byte("missing owner cookie"))
			ws.Close()
			return
		}

		owner := cookie.Value

		savedOwner, _ := client.HGet(r.Context(), "task:"+msgData.TaskId, "owner").Result()
		if savedOwner != owner {
			ws.WriteMessage(websocket.CloseMessage, []byte("unauthorized"))
			ws.Close()
			return
		}

		hubClient := &Client{
			WebSocket: ws,
			Send:      make(chan []byte, 16), // буфер, чтобы не блокироваться
			TaskId:    msgData.TaskId,
		}

		// ВАЖНО: регистрируем клиента в хабе
		hub.register <- hubClient

		go hubClient.writePump(hub)
		go hubClient.readPump(hub)
	}
}
