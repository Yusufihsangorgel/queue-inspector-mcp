// Seeds a Redis database with real Asynq tasks in a known mix of states so the
// adapter and tests can be checked against genuine library output rather than a
// guess at the key layout and message encoding. It also prints the state that
// Asynq's own Inspector reports, which serves as the ground-truth oracle the
// TypeScript adapter is compared against.
//
// Env:
//   REDIS_ADDR default 127.0.0.1:6379
//   REDIS_DB   default 15  (experiment DB, kept away from real data)
//
// Safe to run repeatedly: it clears only the configured queues on the target DB.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/hibiken/asynq"
)

const (
	typeWelcome   = "email:welcome"
	typeArchive   = "email:archive"
	typeRetry     = "email:retry"
	typeScheduled = "email:scheduled"
	typePending   = "email:pending"
	typeBlob      = "blob:binary"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func handle(_ context.Context, t *asynq.Task) error {
	switch t.Type() {
	case typeArchive, typeRetry:
		return fmt.Errorf("simulated permanent failure for %s", t.Type())
	default:
		return nil
	}
}

func main() {
	addr := env("REDIS_ADDR", "127.0.0.1:6379")
	db, _ := strconv.Atoi(env("REDIS_DB", "15"))
	opt := asynq.RedisClientOpt{Addr: addr, DB: db}

	inspector := asynq.NewInspector(opt)
	// Clear only the queues this producer owns; never the whole DB.
	for _, q := range []string{"default", "low"} {
		inspector.DeleteAllPendingTasks(q)
		inspector.DeleteAllScheduledTasks(q)
		inspector.DeleteAllRetryTasks(q)
		inspector.DeleteAllArchivedTasks(q)
		inspector.DeleteAllCompletedTasks(q)
	}

	client := asynq.NewClient(opt)
	payload := func(v map[string]any) []byte {
		b, _ := json.Marshal(v)
		return b
	}

	// default queue: exercised by the worker.
	must(client.Enqueue(asynq.NewTask(typeWelcome, payload(map[string]any{"to": "user@example.test"})),
		asynq.Queue("default"), asynq.Retention(time.Hour)))
	must(client.Enqueue(asynq.NewTask(typeArchive, payload(map[string]any{"to": "dead@example.test"})),
		asynq.Queue("default"), asynq.MaxRetry(0)))
	must(client.Enqueue(asynq.NewTask(typeRetry, payload(map[string]any{"to": "retry@example.test"})),
		asynq.Queue("default"), asynq.MaxRetry(5)))
	must(client.Enqueue(asynq.NewTask(typeScheduled, payload(map[string]any{"to": "later@example.test"})),
		asynq.Queue("default"), asynq.ProcessIn(time.Hour)))

	// low queue: never processed, so tasks stay pending. One carries a payload
	// with raw non-UTF8 bytes to exercise binary handling downstream.
	must(client.Enqueue(asynq.NewTask(typePending, payload(map[string]any{"to": "queued@example.test"})),
		asynq.Queue("low")))
	must(client.Enqueue(asynq.NewTask(typeBlob, []byte{0x00, 0x01, 0x02, 0xff, 0xfe, 'h', 'i'}),
		asynq.Queue("low")))
	_ = client.Close()

	// Run the worker briefly against "default" only.
	srv := asynq.NewServer(opt, asynq.Config{
		Concurrency: 5,
		Queues:      map[string]int{"default": 1},
		// Keep retry backoff short so a retried task lands in the retry state
		// quickly and deterministically within the run window.
		RetryDelayFunc: func(n int, _ error, _ *asynq.Task) time.Duration {
			return 30 * time.Second
		},
		LogLevel: asynq.FatalLevel,
	})
	mux := asynq.NewServeMux()
	mux.HandleFunc(typeWelcome, handle)
	mux.HandleFunc(typeArchive, handle)
	mux.HandleFunc(typeRetry, handle)

	go func() {
		if err := srv.Run(mux); err != nil {
			panic(err)
		}
	}()
	time.Sleep(3 * time.Second)
	srv.Shutdown()

	// Ground-truth snapshot from Asynq's own Inspector.
	out := map[string]any{"db": db}
	queues := map[string]any{}
	for _, q := range []string{"default", "low"} {
		info, err := inspector.GetQueueInfo(q)
		if err != nil {
			queues[q] = map[string]any{"error": err.Error()}
			continue
		}
		queues[q] = map[string]any{
			"pending":   info.Pending,
			"active":    info.Active,
			"scheduled": info.Scheduled,
			"retry":     info.Retry,
			"archived":  info.Archived,
			"completed": info.Completed,
		}
	}
	out["queues"] = queues
	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
	_ = inspector.Close()
}

func must(_ *asynq.TaskInfo, err error) {
	if err != nil {
		panic(err)
	}
}
