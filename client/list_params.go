package client

import (
	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/internal/repository"
)

// ListOrderField represents a column to sort results by.
type ListOrderField string

const (
	OrderByID          ListOrderField = "id"
	OrderByCreatedAt   ListOrderField = "created_at"
	OrderByUpdatedAt   ListOrderField = "updated_at"
	OrderByBlockedTill ListOrderField = "blocked_till"
)

// SortOrder represents the direction of sorting.
type SortOrder string

const (
	SortAsc  SortOrder = "ASC"
	SortDesc SortOrder = "DESC"
)

const (
	defaultLimit = 100
	maxLimit     = 10000
)

// ListParams configures how tasks are listed and filtered.
// Use NewListParams() to create with defaults, then chain builder methods.
type ListParams struct {
	statuses []asynqpg.TaskStatus
	types    []string
	ids      []int64
	limit    int
	offset   int
	orderBy  ListOrderField
	order    SortOrder
}

// NewListParams creates a new ListParams with default values.
func NewListParams() *ListParams {
	return &ListParams{
		limit:   defaultLimit,
		orderBy: OrderByID,
		order:   SortAsc,
	}
}

// States filters tasks by status.
func (p *ListParams) States(states ...asynqpg.TaskStatus) *ListParams {
	p.statuses = states
	return p
}

// Types filters tasks by task type.
func (p *ListParams) Types(types ...string) *ListParams {
	p.types = types
	return p
}

// IDs filters tasks by specific IDs.
func (p *ListParams) IDs(ids ...int64) *ListParams {
	p.ids = ids
	return p
}

// Limit sets the maximum number of tasks to return.
// Must be between 1 and 10000. Values outside this range are clamped.
func (p *ListParams) Limit(n int) *ListParams {
	if n < 1 {
		n = 1
	}
	if n > maxLimit {
		n = maxLimit
	}
	p.limit = n
	return p
}

// Offset sets the number of tasks to skip (for pagination).
func (p *ListParams) Offset(n int) *ListParams {
	if n < 0 {
		n = 0
	}
	p.offset = n
	return p
}

// OrderBy sets the sort field and direction.
func (p *ListParams) OrderBy(field ListOrderField, order SortOrder) *ListParams {
	p.orderBy = field
	p.order = order
	return p
}

func (p *ListParams) toRepoParams() repository.ListTasksParams {
	statuses := make([]string, len(p.statuses))
	for i, s := range p.statuses {
		statuses[i] = string(s)
	}

	return repository.ListTasksParams{
		Statuses: statuses,
		Types:    p.types,
		IDs:      p.ids,
		Limit:    p.limit,
		Offset:   p.offset,
		OrderBy:  string(p.orderBy),
		OrderDir: string(p.order),
	}
}

// ListResult contains the list of tasks and the total count matching the filters.
type ListResult struct {
	Tasks []*TaskInfo
	Total int
}
