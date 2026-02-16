package internal

import (
	"context"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

func StartRedis() *redis.Client {

	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})

	return rdb
}

func PushTask(task Task, client *redis.Client, ctx context.Context, stream string) error {

	_, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		MaxLen: 10000,
		Values: map[string]interface{}{
			"id":   task.Id,
			"code": task.Code,
		},
	}).Result()

	if err != nil {
		return err
	}

	key := "task:" + task.Id

	err = client.HSet(ctx, key, map[string]interface{}{
		"status": "queued",
		"code":   task.Code,
		"result": "_",
		"error":  "_",
		"owner":  task.OwnerId,
		"lang":   task.Lang,
	}).Err()

	if err != nil {
		return err
	}

	client.Expire(ctx, key, time.Minute*10)

	client.LPush(ctx, "user:"+task.OwnerId+":tasks", task.Id)
	client.LTrim(ctx, "user:"+task.OwnerId+":tasks", 0, 159)

	return nil

}

func GetStatus(ctx context.Context, client *redis.Client, id string) (TaskStatus, error) {

	key := "task:" + id
	var result TaskStatus

	values, err := client.HGetAll(ctx, key).Result()
	if err != nil {
		return Wrong_res, err
	}

	if len(values) > 0 {

		stat := values["status"]
		result.Status = stat

		out := values["result"]
		result.Result = out

		misstake := values["error"]
		result.Error = misstake

		code := values["code"]
		result.Code = code

		lang := values["lang"]
		result.Lang = lang

		return result, nil

	}

	deadErr, err := FindInDeadLetter(ctx, client, id)
	if err == nil && deadErr != "" {
		return TaskStatus{
			Code:   "",
			Status: "failed",
			Result: "",
			Error:  deadErr,
		}, nil
	}

	return TaskStatus{
		Code:   "",
		Status: "pending",
		Result: "",
		Error:  "",
	}, nil
}

func FindInDeadLetter(ctx context.Context, client *redis.Client, id string) (codeError string, err error) {

	key := "dead:" + id

	values, err := client.HGetAll(ctx, key).Result()
	if err != nil {
		return "", err
	}

	if len(values) == 0 {
		return "", nil
	}

	return values["error"], nil

}
