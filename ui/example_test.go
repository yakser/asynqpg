package ui_test

import (
	"log"
	"net/http"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

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
