package ws

import (
	"encoding/json"

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

			// переподписка
			if c.TaskId != "" {
				c.Hub.unregister <- c
			}

			c.TaskId = req.TaskId
			c.Hub.register <- c
		}
	}
}

func (c *Client) writePump() {
	defer func() {
		// 🔥 важно: корректное закрытие канала
		close(c.Send)
		c.WebSocket.Close()
	}()

	for msg := range c.Send {
		err := c.WebSocket.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			return
		}
	}
}
