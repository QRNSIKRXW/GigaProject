package ws

import (
	"context"
	"encoding/json"
	"log"

	"github.com/redis/go-redis/v9"
)

func StartPubSubListener(ctx context.Context, rdb *redis.Client, hub *Hub) {
	sub := rdb.Subscribe(ctx, "channel:broadcast")
	ch := sub.Channel()

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				log.Println("PUBSUB: channel closed")
				return
			}

			log.Println("PUBSUB: got message:", msg.Payload)

			var data BroadcastMessage
			if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
				log.Println("PUBSUB: unmarshal error:", err)
				continue
			}

			log.Println("PUBSUB: parsed taskId:", data.TaskId, "line:", data.Line)

			hub.broadcast <- data

		case <-ctx.Done():
			log.Println("PUBSUB: ctx done, closing")
			sub.Close()
			return
		}
	}
}
