package internal

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func (worker *Worker) mainLoop(ctx context.Context) {

	for {

		if err := ctx.Err(); err != nil {
			return
		}

		task, id, err := GetTask(worker.client, ctx, worker.consumerID, worker.group, worker.stream)
		if err != nil {
			log.Printf("[worker %s] error: %v", worker.consumerID, err)
			time.Sleep(250 * time.Millisecond)
			continue
		}

		if task.Id == "" {
			continue
		}

		log.Printf("[worker %s] task: %v", worker.consumerID, task)

		result := ExecuteTask(worker.client, task)
		if result.Status == "done" {
			worker.processed++
		} else {
			worker.failed++
		}

		err = WriteResult(worker.client, ctx, result, task.Id)
		if err != nil {
			log.Printf("[worker %s] error: %v", worker.consumerID, err)
			time.Sleep(250 * time.Millisecond)
			continue
		}

		err = AckTask(worker.client, ctx, worker.stream, worker.group, id)
		if err != nil {
			log.Printf("[worker %s] error: %v", worker.consumerID, err)
			time.Sleep(250 * time.Millisecond)
			continue
		}

	}

}

func (worker *Worker) recoveryLoop(ctx context.Context) {

	for {

		if ctx.Err() != nil {
			return
		}

		recoverArr, err := recoverPending(worker.client, ctx, worker.stream,
			worker.group, worker.pendingIdle, worker.maxRetries)
		if err != nil || len(recoverArr) == 0 {
			log.Printf("[worker %s] recovery error: %v", worker.consumerID, err)
			time.Sleep(3 * time.Second)
			continue
		}

		if len(recoverArr) == 0 {
			time.Sleep(3 * time.Second)
			continue
		}

		idToClaim := make([]string, 0, len(recoverArr))

		for _, val := range recoverArr {

			if val.RetryCount > worker.maxRetries {

				msgs, _ := worker.client.XRange(ctx, worker.stream, val.MsgId, val.MsgId).Result()
				if len(msgs) > 0 {
					task, _ := ParseTask(msgs[0])

					worker.client.HSet(ctx, "dead:"+task.Id, map[string]interface{}{
						"error": "too many retries",
						"code":  task.Code,
					})
					worker.client.Expire(ctx, "dead:"+task.Id, time.Hour)

				}

				worker.dead++

				worker.client.XAck(ctx, worker.stream, worker.group, val.MsgId)
				continue

			}

			idToClaim = append(idToClaim, val.MsgId)

		}

		if len(idToClaim) > 0 {

			err = claimAndExecute(worker, ctx, idToClaim)
			if err != nil {
				log.Printf("[worker %s] recovery error: %v", worker.consumerID, err)
				time.Sleep(3 * time.Second)
				continue
			}
		}

		time.Sleep(3 * time.Second)

	}

}

func (worker *Worker) StartWorker(ctx context.Context, wg *sync.WaitGroup) error {

	err := CreateGroup(worker.client, ctx, worker.stream, worker.group)
	if err != nil {
		return err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		worker.recoveryLoop(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		worker.heartBeatLoop(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		worker.mainLoop(ctx)
	}()

	return nil

}

func NewWorker(client *redis.Client, stream, group string) *Worker {
	return &Worker{
		client:       client,
		stream:       stream,
		group:        group,
		consumerID:   "worker-" + uuid.New().String(),
		pendingIdle:  30 * time.Second,
		pendingBatch: 10,
		maxRetries:   5,
	}
}

func (worker *Worker) heartBeatLoop(ctx context.Context) {

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {

		select {

		case <-ctx.Done():
			return

		case <-ticker.C:
			worker.client.HSet(ctx, "worker:"+worker.consumerID, map[string]interface{}{
				"status":         "online",
				"last_heartbeat": time.Now().Unix(),
				"processed":      worker.processed,
				"failed":         worker.failed,
				"dead":           worker.dead,
			})

			worker.client.Expire(ctx, "worker:"+worker.consumerID, 30*time.Second)

			worker.client.SAdd(ctx, "workers:set", worker.consumerID)

		}

	}

}
