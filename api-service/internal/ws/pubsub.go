package ws

import (
	"context"
	"encoding/json"

	"github.com/redis/go-redis/v9"
)

func StartPubSubListener(ctx context.Context, rdb *redis.Client, hub *Hub) {
	sub := rdb.Subscribe(ctx, "channel:broadcast")
	ch := sub.Channel()

	for {

		select {
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var data BroadcastMessage
			json.Unmarshal([]byte(msg.Payload), &data)
			hub.broadcast <- data

		case <-ctx.Done():
			sub.Close()
			return

		}
	}
}
