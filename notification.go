package asynqpg

import (
	"context"
	"errors"
)

var (
	// ErrNotificationsUnsupported means a query-only pool was used with notifications enabled.
	ErrNotificationsUnsupported = errors.New("pool does not support notifications")
	// ErrNotificationRouting means the query pool could not reach the listener through NOTIFY.
	ErrNotificationRouting = errors.New("notification routing verification failed")
)

// Notification is a PostgreSQL notification without driver-specific types.
type Notification struct{ Channel, Payload string }

// NotificationConnection is a dedicated session owned by its recipient.
// Operations are serialized by the caller. Close must be idempotent.
// Listen and Unlisten must complete their commands before returning.
//
//go:generate go tool mockery --case underscore --with-expecter --name NotificationConnection
type NotificationConnection interface {
	Listen(context.Context, string) error
	Unlisten(context.Context, string) error
	WaitForNotification(context.Context) (Notification, error)
	Ping(context.Context) error
	Close(context.Context) error
}

// NotificationPool supplies queries and dedicated listeners for the same database.
// A successful open transfers a non-nil connection to the caller. Closing that
// connection must not close the pool. The application owns the pool's lifetime.
//
//go:generate go tool mockery --case underscore --with-expecter --name NotificationPool
type NotificationPool interface {
	Pool
	OpenNotificationConnection(context.Context) (NotificationConnection, error)
}
