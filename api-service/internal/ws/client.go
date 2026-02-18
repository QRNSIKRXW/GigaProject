package ws

import (
	"context"
	"encoding/json"
	"log"

	"github.com/gorilla/websocket"
)

type Client struct {
	Hub       *Hub
	WebSocket *websocket.Conn
	Send      chan []byte
	TaskId    string
}

func (c *Client) readPump() {
	defer func() {
		c.Hub.unregister <- c
		c.WebSocket.Close()
	}()

	for {
		_, msg, err := c.WebSocket.ReadMessage()
		if err != nil {
			return
		}

		var req struct {
			Type   string `json:"type"`
			TaskId string `json:"taskId"`
		}

		if err := json.Unmarshal(msg, &req); err != nil {
			continue
		}

		if req.Type == "subscribe" {
			c.TaskId = req.TaskId
			c.Hub.register <- c

			go StartPubSubListener(
				context.Background(),
				c.Hub.Rdb,
				c.Hub,
				"task:"+req.TaskId,
			)
		}
	}
}

func (c *Client) writePump() {
	for msg := range c.Send {
		log.Println("WRITE TO CLIENT:", string(msg))
		err := c.WebSocket.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			return
		}
	}
}
