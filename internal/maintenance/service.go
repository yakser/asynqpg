package maintenance

import "context"

//go:generate go tool mockery --case underscore --with-expecter --name Service

// Service represents a background maintenance service.
type Service interface {
	// Start starts the service. It should return immediately after starting
	// background goroutines. The service should stop when ctx is cancelled.
	Start(ctx context.Context) error

	// Stop stops the service gracefully.
	Stop()

	// Name returns the service name for logging.
	Name() string
}
