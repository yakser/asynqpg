package publisher

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLimiterDedupeExpiryAndBound(t *testing.T) {
	t.Parallel()
	const overflowType = "overflow"
	var l Limiter
	now := time.Now()
	require.Equal(t, []string{"a", "b"}, l.reserve([]string{"a", "a", "b"}, now))
	require.Empty(t, l.reserve([]string{"a"}, now.Add(Interval-time.Nanosecond)))
	require.Equal(t, []string{"a"}, l.reserve([]string{"a"}, now.Add(Interval)))
	types := make([]string, MaxTypes*2)
	for i := range types {
		types[i] = fmt.Sprint(i)
	}
	l.reserve(types, now.Add(Interval))
	require.Len(t, l.next, MaxTypes)
	require.Empty(t, l.reserve([]string{overflowType}, now.Add(Interval)))
	require.Equal(t, []string{overflowType}, l.reserve([]string{overflowType}, now.Add(2*Interval)))
	require.Len(t, l.next, 1)
}
func TestLimiterConcurrentReservation(t *testing.T) {
	t.Parallel()
	var l Limiter
	now := time.Now()
	var wg sync.WaitGroup
	results := make(chan []string, 100)
	for range 100 {
		wg.Go(func() { results <- l.reserve([]string{"same"}, now) })
	}
	wg.Wait()
	close(results)
	count := 0
	for r := range results {
		count += len(r)
	}
	require.Equal(t, 1, count)
}
