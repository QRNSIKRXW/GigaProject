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

	resultFunc := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		ws, err := Upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("upgrade error", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			json.NewEncoder(w).Encode(map[string]string{"Error": "connection error"})
		}

		_, msg, err := ws.ReadMessage()
		if err != nil {
			ws.Close()
			return
		}

		var msgData data
		json.Unmarshal(msg, &msgData)

		hubClient := Client{
			WebSocket: ws,
			Send:      make(chan []byte),
			TaskId:    msgData.TaskId,
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

		go hubClient.writePump(hub)
		go hubClient.readPump(hub)

	})

	return resultFunc

}
