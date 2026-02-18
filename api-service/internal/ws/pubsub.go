package ws

import (
	"context"
	"encoding/json"
	"log"

	"github.com/redis/go-redis/v9"
)

func StartPubSubListener(ctx context.Context, rdb *redis.Client, hub *Hub, channel string) {
	sub := rdb.Subscribe(ctx, channel)
	ch := sub.Channel()

	for {
		select {
		case msg := <-ch:
			if msg == nil {
				return
			}

			var data BroadcastMessage
			if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
				continue
			}

			// ВРЕМЕННО:
			log.Println("PUBSUB MSG:", channel, "=>", data.TaskId, data.Line)

			hub.broadcast <- data

		case <-ctx.Done():
			sub.Close()
			return
		}
	}
}
