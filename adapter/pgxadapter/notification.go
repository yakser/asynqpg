package pgxadapter

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"

	"github.com/yakser/asynqpg"
)

type notificationConnection struct {
	conn        *pgx.Conn
	beforeClose func(*pgx.Conn)
	once        sync.Once
	closeErr    error
}

func (c *notificationConnection) Listen(ctx context.Context, channel string) error {
	_, err := c.conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize())
	return operationError(ctx, "subscribe", err)
}
func (c *notificationConnection) Unlisten(ctx context.Context, channel string) error {
	_, err := c.conn.Exec(ctx, "UNLISTEN "+pgx.Identifier{channel}.Sanitize())
	return operationError(ctx, "unsubscribe", err)
}
func (c *notificationConnection) WaitForNotification(ctx context.Context) (asynqpg.Notification, error) {
	n, err := c.conn.WaitForNotification(ctx)
	if err != nil {
		return asynqpg.Notification{}, operationError(ctx, "receive", err)
	}
	if n == nil {
		return asynqpg.Notification{}, fmt.Errorf("receive: %w", ErrNotificationHandlerUnsupported)
	}
	return asynqpg.Notification{Channel: n.Channel, Payload: n.Payload}, nil
}
func (c *notificationConnection) Ping(ctx context.Context) error {
	return operationError(ctx, "ping", c.conn.Ping(ctx))
}
func (c *notificationConnection) Close(ctx context.Context) error {
	c.once.Do(func() {
		if c.beforeClose != nil {
			c.beforeClose(c.conn)
		}
		c.closeErr = safeError(ctx, c.conn.Close(ctx))
	})
	return c.closeErr
}

func operationError(ctx context.Context, stage string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", stage, safeError(ctx, err))
}
