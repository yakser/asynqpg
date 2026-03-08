//go:build dev

package ui

import (
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// frontendHandler returns a handler that serves frontend files from disk in dev mode.
// This allows using Vite's dev server with HMR while still testing the Go API.
func frontendHandler() http.Handler {
	_, filename, _, _ := runtime.Caller(0)
	distDir := filepath.Join(filepath.Dir(filename), "frontend", "dist")

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")

		// Try file on disk.
		if path != "" {
			fullPath := filepath.Join(distDir, filepath.Clean(path))
			if _, err := os.Stat(fullPath); err == nil {
				http.ServeFile(w, r, fullPath)
				return
			}
		}

		// SPA fallback.
		http.ServeFile(w, r, filepath.Join(distDir, "index.html"))
	})
}
