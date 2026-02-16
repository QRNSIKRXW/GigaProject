package ws

import (
	"log"
	"time"

	"github.com/gorilla/websocket"
)

type Client struct {
	WebSocket *websocket.Conn
	Send      chan []byte
	TaskId    string
}

func (c *Client) writePump(h *Hub) {
	ticker := time.NewTicker(10 * time.Second)
	defer func() {
		ticker.Stop()
		h.unregister <- c
		c.WebSocket.Close()
	}()

	c.WebSocket.SetCloseHandler(func(code int, text string) error {
		log.Println("websocket: client disconnected")
		return nil
	})

	for {
		select {

		case msg, ok := <-c.Send:
			if !ok {
				c.WebSocket.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.WebSocket.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}

		case <-ticker.C:
			if err := c.WebSocket.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
				return
			}
		}
	}
}

func (c *Client) readPump(h *Hub) {
	defer func() {
		h.unregister <- c
		c.WebSocket.Close()
	}()

	c.WebSocket.SetReadLimit(1024)
	c.WebSocket.SetReadDeadline(time.Now().Add(30 * time.Second))

	c.WebSocket.SetPongHandler(func(string) error {
		// клиент ответил pong → продлеваем дедлайн
		c.WebSocket.SetReadDeadline(time.Now().Add(30 * time.Second))
		return nil
	})

	for {
		_, _, err := c.WebSocket.ReadMessage()
		if err != nil {
			break
		}
	}
}
