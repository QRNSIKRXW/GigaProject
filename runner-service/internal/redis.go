package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"log"

	"github.com/redis/go-redis/v9"
)

func StartRedis() *redis.Client {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	return rdb
}

// Пишем в PubSub всегда с context.Background(), чтобы не зависеть от HTTP-контекста.
func WritePubSub(client *redis.Client, middleResult string, id string) error {
	resStruct := RedisLine{
		Line: middleResult,
	}

	payload, err := json.Marshal(resStruct)
	if err != nil {
		return err
	}

	ctx := context.Background()

	var lastErr error
	for i := 0; i < 4; i++ {
		log.Printf("Attempting to publish to channel %s: %s", id, payload)
		err = client.Publish(ctx, id, payload).Err()
		if err == nil {
			return nil
		}
		lastErr = err
		fmt.Printf("PUBLISH ERROR: %v\n", err)
	}

	return lastErr
}
