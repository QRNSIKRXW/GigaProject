package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func RateLimit(client *redis.Client, ctx context.Context, ip string, limit int) error {

	count, err := client.Incr(ctx, "rate:"+ip).Result()
	if err != nil {
		return err
	}

	if count == 1 {
		client.Expire(ctx, "rate:"+ip, 2*time.Minute)
	}

	if count > int64(limit) {
		return fmt.Errorf("rate limit exceeded")
	}

	return nil

}
