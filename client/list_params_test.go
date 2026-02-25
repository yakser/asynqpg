package client

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/yakser/asynqpg"
)

func TestUnitListParams_Defaults(t *testing.T) {
	t.Parallel()

	p := NewListParams()

	assert.Equal(t, defaultLimit, p.limit)
	assert.Equal(t, OrderByID, p.orderBy)
	assert.Equal(t, SortAsc, p.order)
	assert.Equal(t, 0, p.offset)
	assert.Nil(t, p.statuses)
	assert.Nil(t, p.types)
	assert.Nil(t, p.ids)
}

func TestUnitListParams_LimitClamping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    int
		expected int
	}{
		{"below minimum", 0, 1},
		{"negative", -10, 1},
		{"minimum boundary", 1, 1},
		{"normal value", 50, 50},
		{"maximum boundary", maxLimit, maxLimit},
		{"above maximum", maxLimit + 1, maxLimit},
		{"way above maximum", 999999, maxLimit},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := NewListParams().Limit(tt.input)
			assert.Equal(t, tt.expected, p.limit)
		})
	}
}

func TestUnitListParams_NegativeOffset(t *testing.T) {
	t.Parallel()

	p := NewListParams().Offset(-5)
	assert.Equal(t, 0, p.offset)

	p = NewListParams().Offset(10)
	assert.Equal(t, 10, p.offset)
}

func TestUnitListParams_BuilderChaining(t *testing.T) {
	t.Parallel()

	p := NewListParams().
		States(asynqpg.TaskStatusFailed, asynqpg.TaskStatusCancelled).
		Types("email", "sms").
		IDs(1, 2, 3).
		Limit(50).
		Offset(10).
		OrderBy(OrderByCreatedAt, SortDesc)

	assert.Equal(t, []asynqpg.TaskStatus{asynqpg.TaskStatusFailed, asynqpg.TaskStatusCancelled}, p.statuses)
	assert.Equal(t, []string{"email", "sms"}, p.types)
	assert.Equal(t, []int64{1, 2, 3}, p.ids)
	assert.Equal(t, 50, p.limit)
	assert.Equal(t, 10, p.offset)
	assert.Equal(t, OrderByCreatedAt, p.orderBy)
	assert.Equal(t, SortDesc, p.order)
}

func TestUnitListParams_ToRepoParams(t *testing.T) {
	t.Parallel()

	p := NewListParams().
		States(asynqpg.TaskStatusPending, asynqpg.TaskStatusFailed).
		Types("email").
		IDs(10, 20).
		Limit(25).
		Offset(5).
		OrderBy(OrderByUpdatedAt, SortDesc)

	rp := p.toRepoParams()

	assert.Equal(t, []string{"pending", "failed"}, rp.Statuses)
	assert.Equal(t, []string{"email"}, rp.Types)
	assert.Equal(t, []int64{10, 20}, rp.IDs)
	assert.Equal(t, 25, rp.Limit)
	assert.Equal(t, 5, rp.Offset)
	assert.Equal(t, "updated_at", rp.OrderBy)
	assert.Equal(t, "DESC", rp.OrderDir)
}

func TestUnitListParams_ToRepoParams_Defaults(t *testing.T) {
	t.Parallel()

	p := NewListParams()
	rp := p.toRepoParams()

	assert.Empty(t, rp.Statuses)
	assert.Nil(t, rp.Types)
	assert.Nil(t, rp.IDs)
	assert.Equal(t, defaultLimit, rp.Limit)
	assert.Equal(t, 0, rp.Offset)
	assert.Equal(t, "id", rp.OrderBy)
	assert.Equal(t, "ASC", rp.OrderDir)
}
