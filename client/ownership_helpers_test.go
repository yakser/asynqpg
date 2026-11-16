//go:build integration

package client_test

// firstGenerations is for fixtures claimed exactly once.
func firstGenerations(ids []int64) []int64 {
	g := make([]int64, len(ids))
	for i := range g {
		g[i] = 1
	}
	return g
}
