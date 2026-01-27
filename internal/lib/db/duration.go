package db

import (
	"database/sql/driver"
	"time"
)

type Duration struct {
	duration time.Duration
}

func NewDuration(duration time.Duration) Duration {
	return Duration{
		duration: duration,
	}
}

func (d Duration) IsZero() bool {
	return d.duration == 0
}

func (d Duration) Value() (driver.Value, error) {
	return d.duration.Round(time.Millisecond).String(), nil
}

// Duration returns the underlying time.Duration.
func (d Duration) Duration() time.Duration {
	return d.duration
}

// String returns the PostgreSQL interval string representation.
func (d Duration) String() string {
	return d.duration.Round(time.Millisecond).String()
}
