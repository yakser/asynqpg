package repository

import (
	"strings"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg/mocks"
)

func TestSuppressedBatchPublicationUsesPlainInsert(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		const taskType = "fast"
		q := mocks.NewQuerier(t)
		repo := NewRepository(q)
		require.Equal(t, []string{taskType}, repo.publisher.Reserve([]string{taskType}))
		q.EXPECT().SelectContext(mock.Anything, mock.Anything, mock.MatchedBy(func(query string) bool {
			return !strings.Contains(query, "readiness_hints") && strings.Contains(query, "INSERT INTO")
		}), mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		_, err := repo.PushTasks(t.Context(), PushTasksParams{Tasks: []PushTaskParams{{Type: taskType, Payload: []byte(`{}`), AttemptsLeft: 1}}})
		require.NoError(t, err)
	})
}
