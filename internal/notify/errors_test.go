package notify

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
)

func TestOperationErrorsRedactProviderDetails(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"acquire", "subscribe", "probe publish", "ping", "receive"} {
		err := operationError(stage, errors.New("postgres://user:secret@host/db"), ErrUnavailable)
		require.ErrorIs(t, err, ErrUnavailable)
		require.ErrorContains(t, err, stage)
		require.NotContains(t, err.Error(), "secret")
		require.ErrorContains(t, operationError(stage, context.DeadlineExceeded, asynqpg.ErrNotificationRouting), "timeout")
	}
}
