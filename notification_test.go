package asynqpg_test

import (
	"github.com/jmoiron/sqlx"

	"github.com/yakser/asynqpg"
)

var (
	_ asynqpg.Pool = (*sqlx.DB)(nil)
	_ asynqpg.Tx   = (*sqlx.Tx)(nil)
)
