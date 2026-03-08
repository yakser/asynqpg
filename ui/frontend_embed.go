//go:build !dev

package ui

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed frontend/dist/*
var frontendFS embed.FS

func frontendHandler() http.Handler {
	distFS, err := fs.Sub(frontendFS, "frontend/dist")
	if err != nil {
		panic("asynqpg/ui: create sub filesystem: " + err.Error())
	}
	fileServer := http.FileServer(http.FS(distFS))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")

		// Try to serve the file directly.
		if path != "" {
			if f, err := distFS.Open(path); err == nil {
				f.Close()
				// Hashed assets (JS/CSS) are content-addressed – cache indefinitely.
				// Everything else (including index.html) must revalidate on every load.
				if strings.HasPrefix(path, "assets/") {
					w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
				} else {
					w.Header().Set("Cache-Control", "no-cache")
				}
				fileServer.ServeHTTP(w, r)
				return
			}
		}

		// SPA fallback: serve index.html for any route not matching a static file.
		// Must not be cached so the browser always fetches the latest entry point.
		w.Header().Set("Cache-Control", "no-cache")
		r.URL.Path = "/"
		fileServer.ServeHTTP(w, r)
	})
}
