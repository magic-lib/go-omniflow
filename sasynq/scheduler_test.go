package sasynq

import (
	"context"
	"log"
	"testing"
	"time"
)

const TypeScheduledGet = "scheduled:get"

type ScheduledGetPayload struct {
	URL string `json:"url"`
}

func handleScheduledGetTask(ctx context.Context, p *ScheduledGetPayload) error {
	log.Printf("[ScheduledGet] Task for URL %s completed successfully", p.URL)
	return nil
}

func registerSchedulerTasks(scheduler *Scheduler) error {
	payload1 := &ScheduledGetPayload{URL: "https://google.com"}
	entryID1, err := scheduler.RegisterTask("@every 2s", TypeScheduledGet, payload1)
	if err != nil {
		return err
	}
	log.Printf("Registered periodic task with entry ID: %s", entryID1)

	payload2 := &ScheduledGetPayload{URL: "https://bing.com"}
	entryID2, err := scheduler.RegisterTask("@every 3s", TypeScheduledGet, payload2)
	if err != nil {
		return err
	}
	log.Printf("Registered periodic task with entry ID: %s", entryID2)

	return nil
}

func runServer(redisCfg RedisConfig) (*Server, error) {
	serverCfg := DefaultServerConfig() // Uses critical, default, low queues
	srv := NewServer(redisCfg, serverCfg)
	srv.Use(LoggingMiddleware())

	// register task handle function, there are three registration methods available
	RegisterTaskHandler(srv.Mux(), TypeScheduledGet, HandleFunc(handleScheduledGetTask))

	srv.Run()

	return srv, nil
}

func TestNewScheduler(t *testing.T) {
	scheduler := NewScheduler(getRedisConfig())
	err := registerSchedulerTasks(scheduler)
	if err != nil {
		t.Log("register scheduler tasks failed", err)
	} else {
		scheduler.Run()
	}

	srv, err := runServer(getRedisConfig())
	if err != nil {
		t.Log("run server failed", err)
		return
	}
	time.Sleep(15 * time.Second)
	srv.Shutdown()
}
