package maintenance

import (
	"context"
	"log/slog"
	"sync"
)

// Maintainer manages multiple maintenance services.
// It starts/stops all services together and is typically controlled by
// the leadership elector - only the leader runs maintenance services.
type Maintainer struct {
	services []Service
	logger   *slog.Logger

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewMaintainer creates a new Maintainer with the given services.
func NewMaintainer(logger *slog.Logger, services ...Service) *Maintainer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Maintainer{
		services: services,
		logger:   logger,
	}
}

// Start starts all maintenance services.
// Services run until Stop is called or the provided context is cancelled.
func (m *Maintainer) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.started {
		return nil
	}

	ctx, m.cancel = context.WithCancel(ctx)
	m.started = true

	for _, svc := range m.services {
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			if err := svc.Start(ctx); err != nil {
				m.logger.Error("maintenance service failed to start",
					"service", svc.Name(),
					"error", err,
				)
			}
		}()
	}

	m.logger.Info("maintenance services started", "count", len(m.services))
	return nil
}

// Stop stops all maintenance services gracefully.
func (m *Maintainer) Stop() {
	m.mu.Lock()
	if !m.started {
		m.mu.Unlock()
		return
	}
	m.cancel()
	m.started = false
	m.mu.Unlock()

	// Stop each service
	for _, svc := range m.services {
		svc.Stop()
	}

	// Wait for all service goroutines to finish
	m.wg.Wait()
	m.logger.Info("maintenance services stopped")
}

// IsStarted returns whether the maintainer is currently running.
func (m *Maintainer) IsStarted() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.started
}
