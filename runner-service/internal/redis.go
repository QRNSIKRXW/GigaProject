package internal

import (
	"context"
	"encoding/json"
	"os"

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

func WritePubSub(client *redis.Client, ctx context.Context, middleResult string, id string) error {

	resStruct := RedisLine{
		TaskId: id,
		Line:   middleResult,
	}

	payload, err := json.Marshal(resStruct)
	if err != nil {
		return err
	}

	err = client.Publish(ctx, "channel:broadcast", payload).Err()
	if err != nil {
		return err
	}

	return nil

}
