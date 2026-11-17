package ui_test

import (
	"log"
	"net/http"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/consumer"
	"github.com/yakser/asynqpg/ui"
)

func ExampleNewHandler() {
	db, err := sqlx.Connect("postgres", "postgres://postgres:password@localhost:5432/asynqpg?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	handler, err := ui.NewHandler(ui.HandlerOpts{
		Pool:   db,
		Prefix: "/asynqpg",
	})
	if err != nil {
		log.Fatal(err)
	}

	http.Handle("/asynqpg/", handler)
}

func ExampleNewHandler_basicAuth() {
	db, err := sqlx.Connect("postgres", "postgres://postgres:password@localhost:5432/asynqpg?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	handler, err := ui.NewHandler(ui.HandlerOpts{
		Pool:      db,
		Prefix:    "/asynqpg",
		BasicAuth: &ui.BasicAuth{Username: "admin", Password: "secret"},
	})
	if err != nil {
		log.Fatal(err)
	}

	http.Handle("/asynqpg/", handler)
}

// ExampleNewHandler_consumerInfo shares preview settings with the consumer.
func ExampleNewHandler_consumerInfo() {
	db, err := sqlx.Connect("postgres", "postgres://postgres:password@localhost:5432/asynqpg?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	settings := ui.MaintenanceConfig{StuckThreshold: 2 * time.Minute, CompletedRetention: time.Hour, FailedRetention: 2 * time.Hour, CancelledRetention: time.Hour}
	c, err := consumer.New(consumer.Config{Pool: db, StuckThreshold: settings.StuckThreshold, CompletedRetention: settings.CompletedRetention, FailedRetention: settings.FailedRetention, CancelledRetention: settings.CancelledRetention})
	if err != nil {
		log.Fatal(err)
	}
	// Register handlers and start c as part of the application's lifecycle.
	handler, err := ui.NewHandler(ui.HandlerOpts{Pool: db, ConsumerInfoFunc: func() []asynqpg.ConsumerInfo { return []asynqpg.ConsumerInfo{c.Info()} }, Maintenance: &settings})
	if err != nil {
		log.Fatal(err)
	}
	http.Handle("/", handler)
}
