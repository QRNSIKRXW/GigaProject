package internal

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

func StartRedis() *redis.Client {

	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})

	return rdb
}

func GetWorkersArr(client *redis.Client, ctx context.Context) ([]WorkerInfo, error) {

	result := make([]WorkerInfo, 0, 15)

	var cursor uint64

	for {

		keys, NewCursor, err := client.SScan(ctx, "workers:set", cursor, "*", 10).Result()
		if err != nil {
			return []WorkerInfo{}, err
		}

		for _, workerId := range keys {

			data, err := client.HGetAll(ctx, "worker:"+workerId).Result()
			if err != nil {
				continue
			}

			if len(data) == 0 {
				continue
			}

			lastHeartbeat, err := strconv.ParseInt(data["last_heartbeat"], 10, 64)
			if err != nil {
				return []WorkerInfo{}, err
			}

			status := data["status"]
			if (time.Now().Unix() - lastHeartbeat) > 30 {
				status = "offline"
			}

			processed, err := strconv.ParseInt(data["processed"], 10, 64)
			if err != nil {
				return []WorkerInfo{}, err
			}

			failed, err := strconv.ParseInt(data["failed"], 10, 64)
			if err != nil {
				return []WorkerInfo{}, err
			}

			dead, err := strconv.ParseInt(data["dead"], 10, 64)
			if err != nil {
				return []WorkerInfo{}, err
			}

			worker := WorkerInfo{
				Id:            workerId,
				Status:        status,
				LastHeartbeat: lastHeartbeat,
				Processed:     processed,
				Failed:        failed,
				Dead:          dead,
			}

			result = append(result, worker)

		}

		cursor = NewCursor
		if cursor == 0 {
			break
		}

	}

	return result, nil

}

func GetWorkerById(client *redis.Client, ctx context.Context, id string) (WorkerInfo, error) {

	data, err := client.HGetAll(ctx, "worker:"+id).Result()
	if err != nil {
		return WorkerInfo{}, err
	}

	if len(data) == 0 {
		return WorkerInfo{}, nil
	}

	lastHeartbeat, err := strconv.ParseInt(data["last_heartbeat"], 10, 64)
	if err != nil {
		return WorkerInfo{}, err
	}

	processed, err := strconv.ParseInt(data["processed"], 10, 64)
	if err != nil {
		return WorkerInfo{}, err
	}

	failed, err := strconv.ParseInt(data["failed"], 10, 64)
	if err != nil {
		return WorkerInfo{}, err
	}

	dead, err := strconv.ParseInt(data["dead"], 10, 64)
	if err != nil {
		return WorkerInfo{}, err
	}

	result := WorkerInfo{
		Id:            id,
		Status:        data["status"],
		LastHeartbeat: lastHeartbeat,
		Processed:     processed,
		Failed:        failed,
		Dead:          dead,
	}

	return result, nil

}
