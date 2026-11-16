package notify

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/mocks"
)

func TestVerifyRoute(t *testing.T) {
	t.Parallel()
	const publishFailure = "publish"
	const receiveFailure = "receive"
	for _, mode := range []string{"success", publishFailure, receiveFailure, "cleanup"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			pool := mocks.NewPool(t)
			conn := mocks.NewNotificationConnection(t)
			var channel, payload string
			conn.EXPECT().Listen(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, ch string) error { channel = ch; return nil }).Once()
			pool.EXPECT().ExecContext(mock.Anything, "SELECT pg_catalog.pg_notify($1, $2)", mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, _ string, args ...any) (sql.Result, error) {
				require.Equal(t, channel, args[0])
				payload = args[1].(string)
				if mode == publishFailure {
					return nil, errors.New("secret")
				}
				return driver.RowsAffected(1), nil
			}).Once()
			if mode != publishFailure {
				conn.EXPECT().WaitForNotification(mock.Anything).Return(asynqpg.Notification{Channel: "unrelated"}, nil).Once()
				conn.EXPECT().WaitForNotification(mock.Anything).RunAndReturn(func(context.Context) (asynqpg.Notification, error) {
					if mode == receiveFailure {
						return asynqpg.Notification{}, context.DeadlineExceeded
					}
					return asynqpg.Notification{Channel: channel, Payload: payload}, nil
				}).Once()
				if mode != receiveFailure {
					var err error
					if mode == "cleanup" {
						err = errors.New("secret")
					}
					conn.EXPECT().Unlisten(mock.Anything, mock.Anything).Return(err).Once()
				}
			}
			err := verifyRoute(t.Context(), pool, conn)
			if mode == "success" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, asynqpg.ErrNotificationRouting)
				require.NotContains(t, err.Error(), "secret")
				require.ErrorContains(t, err, "probe "+mode)
			}
		})
	}
}
