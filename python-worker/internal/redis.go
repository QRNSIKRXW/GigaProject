package internal

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/redis/go-redis/v9"
)

func StartRedis() *redis.Client {

	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})

	return rdb
}

func GetTask(client *redis.Client, ctx context.Context, consumerID string, group string, stream string) (Task, string, error) {

	res, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumerID,
		Streams:  []string{stream, ">"},
		Count:    1,
		Block:    0, // ждать бесконечно
	}).Result()

	if err != nil {
		return WrongTask, "_", err
	}

	var task Task

	if len(res) == 0 || len(res[0].Messages) == 0 {
		return WrongTask, "_", fmt.Errorf("empty query")
	}

	resStream := res[0]
	msg := resStream.Messages[0]

	id := msg.ID

	task, err = ParseTask(msg)
	if err != nil {
		return WrongTask, "_", err
	}

	return task, id, nil

}

func AckTask(client *redis.Client, ctx context.Context, stream string, group string, id string) error {

	return client.XAck(ctx, stream, group, id).Err()

}

func CreateGroup(client *redis.Client, ctx context.Context, stream string, group string) error {

	err := client.XGroupCreateMkStream(
		ctx,
		stream,
		group,
		"$",
	).Err()

	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}

	return nil

}

func WriteResult(client *redis.Client, ctx context.Context, result TaskStatus, id string) error {

	err := client.HSet(ctx, "task:"+id, map[string]interface{}{
		"status": result.Status,
		"result": result.Result,
		"error":  result.Error,
	}).Err()
	if err != nil {
		return err
	}

	return nil

}

func WritePubSub(client *redis.Client, ctx context.Context, middleResult string, id string) error {

	payload := fmt.Sprintf(`{"taskId":"%s","line":"%s"}`, id, middleResult)
	err := client.Publish(ctx, "channel:broadcast", payload).Err()
	if err != nil {
		return err
	}

	return nil

}
