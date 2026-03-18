package client_test

import (
	"context"
	"log"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/client"
)

func ExampleNew() {
	db, err := sqlx.Connect("postgres", "postgres://postgres:password@localhost:5432/asynqpg?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	cl, err := client.New(client.Config{Pool: db})
	if err != nil {
		log.Fatal(err)
	}

	_ = cl
}

func ExampleClient_ListTasks() {
	db, err := sqlx.Connect("postgres", "postgres://postgres:password@localhost:5432/asynqpg?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	cl, err := client.New(client.Config{Pool: db})
	if err != nil {
		log.Fatal(err)
	}

	result, err := cl.ListTasks(context.Background(), client.NewListParams().
		States(asynqpg.TaskStatusFailed, asynqpg.TaskStatusPending).
		Types("email:send").
		Limit(50).
		OrderBy(client.OrderByCreatedAt, client.SortDesc),
	)
	if err != nil {
		log.Fatal(err)
	}

	_ = result
}
