package producer_test

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/producer"
)

func ExampleNew() {
	db, err := sqlx.Connect("postgres", "postgres://postgres:password@localhost:5432/asynqpg?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	p, err := producer.New(producer.Config{
		Pool:            db,
		DefaultMaxRetry: 3,
	})
	if err != nil {
		log.Fatal(err)
	}

	_ = p
}

func ExampleProducer_Enqueue() {
	db, err := sqlx.Connect("postgres", "postgres://postgres:password@localhost:5432/asynqpg?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	p, err := producer.New(producer.Config{Pool: db})
	if err != nil {
		log.Fatal(err)
	}

	payload, _ := json.Marshal(map[string]string{"to": "user@example.com"})

	_, err = p.Enqueue(context.Background(), asynqpg.NewTask("email:send", payload,
		asynqpg.WithMaxRetry(5),
		asynqpg.WithDelay(10*time.Second),
	))
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleProducer_EnqueueMany() {
	db, err := sqlx.Connect("postgres", "postgres://postgres:password@localhost:5432/asynqpg?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	p, err := producer.New(producer.Config{Pool: db})
	if err != nil {
		log.Fatal(err)
	}

	tasks := []*asynqpg.Task{
		asynqpg.NewTask("email:send", []byte(`{"to":"a@example.com"}`)),
		asynqpg.NewTask("email:send", []byte(`{"to":"b@example.com"}`)),
	}

	ids, err := p.EnqueueMany(context.Background(), tasks)
	if err != nil {
		log.Fatal(err)
	}

	_ = ids
}
