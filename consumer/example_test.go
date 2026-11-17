package consumer_test

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yakser/asynqpg/adapter/pgxadapter"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/consumer"
)

func ExampleNew() {
	db, err := sqlx.Connect("postgres", "postgres://postgres:password@localhost:5432/asynqpg?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	c, err := consumer.New(consumer.Config{
		Pool:            db,
		FetchInterval:   100 * time.Millisecond,
		ShutdownTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	_ = c
}

func ExampleConsumer_RegisterTaskHandler() {
	db, err := sqlx.Connect("postgres", "postgres://postgres:password@localhost:5432/asynqpg?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	c, err := consumer.New(consumer.Config{Pool: db})
	if err != nil {
		log.Fatal(err)
	}

	if err := c.RegisterTaskHandler("email:send",
		consumer.TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
			fmt.Printf("Processing task %d: %s\n", task.ID, task.Type)
			return nil
		}),
		consumer.WithWorkersCount(5),
		consumer.WithTimeout(30*time.Second),
	); err != nil {
		log.Fatal(err)
	}
}

func ExampleConsumer_Use() {
	db, err := sqlx.Connect("postgres", "postgres://postgres:password@localhost:5432/asynqpg?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	c, err := consumer.New(consumer.Config{Pool: db})
	if err != nil {
		log.Fatal(err)
	}

	_ = c.Use(func(next consumer.TaskHandler) consumer.TaskHandler {
		return consumer.TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
			slog.Info("processing task", "type", task.Type, "id", task.ID)
			err := next.Handle(ctx, task)
			slog.Info("task done", "type", task.Type, "id", task.ID, "error", err)
			return err
		})
	})
}

// ExampleNew_notifications configures a direct listener and a one-second safety poll.
// No optional migration is required; library mutations publish atomically.
func ExampleNew_notifications() {
	native, err := pgxpool.New(context.Background(), "postgres://postgres:password@localhost:5432/asynqpg?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer native.Close()
	db, err := pgxadapter.New(native)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	c, err := consumer.New(consumer.Config{
		Pool:          db,
		FetchInterval: time.Second,
		FetchCooldown: 10 * time.Millisecond,
		Notifications: &consumer.NotificationConfig{},
	})
	if err != nil {
		log.Fatal(err)
	}
	_ = c // Register handlers, Start, and Stop using the normal application lifecycle.
}
