package notify

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/yakser/asynqpg"
)

// verifyRoute establishes actual delivery; matching database metadata is insufficient.
// The caller closes the connection on failure, which also removes any probe subscription.
func verifyRoute(ctx context.Context, pool asynqpg.Pool, conn asynqpg.NotificationConnection) error {
	channelBytes := make([]byte, 16)
	payloadBytes := make([]byte, 16)
	if _, err := rand.Read(channelBytes); err != nil {
		return fmt.Errorf("probe entropy: %w", err)
	}
	if _, err := rand.Read(payloadBytes); err != nil {
		return fmt.Errorf("probe entropy: %w", err)
	}
	channel := "asynqpg_probe_" + hex.EncodeToString(channelBytes)
	payload := hex.EncodeToString(payloadBytes)
	if err := conn.Listen(ctx, channel); err != nil {
		return operationError("probe subscribe", err, asynqpg.ErrNotificationRouting)
	}
	if _, err := pool.ExecContext(ctx, "SELECT pg_catalog.pg_notify($1, $2)", channel, payload); err != nil {
		return operationError("probe publish", err, asynqpg.ErrNotificationRouting)
	}
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return operationError("probe receive", err, asynqpg.ErrNotificationRouting)
		}
		if n.Channel == channel && n.Payload == payload {
			break
		}
	}
	if err := conn.Unlisten(ctx, channel); err != nil {
		return operationError("probe cleanup", err, asynqpg.ErrNotificationRouting)
	}
	return nil
}
