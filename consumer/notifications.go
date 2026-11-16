package consumer

import (
	"fmt"
	"time"
)

// NotificationConfig enables readiness wakeups in addition to fallback polling.
// No optional migration is required.
type NotificationConfig struct{}

func validateFetchCooldown(cooldown, poll time.Duration) (time.Duration, error) {
	if cooldown == 0 {
		cooldown = min(10*time.Millisecond, poll)
	}
	if cooldown < time.Millisecond || cooldown > poll {
		return 0, fmt.Errorf("fetch cooldown must be at least 1ms and no greater than FetchInterval")
	}
	return cooldown, nil
}
