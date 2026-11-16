package notify

import (
	"context"
	"errors"
	"fmt"

	"github.com/yakser/asynqpg"
)

// operationError labels provider failures without formatting untrusted driver errors.
func operationError(stage string, err, sentinel error) error {
	if errors.Is(err, asynqpg.ErrNotificationsUnsupported) {
		return fmt.Errorf("%s (unsupported provider): %w", stage, asynqpg.ErrNotificationsUnsupported)
	}
	category := "unavailable"
	if errors.Is(err, context.DeadlineExceeded) {
		category = "timeout"
	}
	if errors.Is(err, context.Canceled) {
		category = "canceled"
	}
	return fmt.Errorf("%s (%s): %w", stage, category, sentinel)
}
