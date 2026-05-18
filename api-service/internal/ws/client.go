package ws

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

type Client struct {
	Hub       *Hub
	WebSocket *websocket.Conn
	Send      chan []byte
	TaskId    string
	OwnerID   string
	closeOnce sync.Once
}

func (c *Client) closeSend() {
	c.closeOnce.Do(func() {
		close(c.Send)
	})
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
			if !c.canSubscribe(req.TaskId) {
				_ = c.WebSocket.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","line":"access denied"}`))
				continue
			}

			// переподписка
			if c.TaskId != "" {
				c.Hub.unregister <- c
			}

			c.TaskId = req.TaskId
			c.Hub.register <- c
		}
	}
}

func (c *Client) canSubscribe(taskID string) bool {
	if taskID == "" || c.OwnerID == "" {
		return false
	}

	owner, err := c.Hub.Rdb.HGet(context.Background(), "task:"+taskID, "owner").Result()
	if err == redis.Nil {
		return false
	}
	if err != nil {
		log.Printf("ws owner check failed: %v", err)
		return false
	}

	return owner == c.OwnerID
}

func (c *Client) writePump() {
	defer func() {
		c.WebSocket.Close()
	}()

	for msg := range c.Send {
		err := c.WebSocket.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			return
		}
	}
}
