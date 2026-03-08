package ui

import (
	"crypto/subtle"
	"net/http"
	"strings"
	"time"
)

func basicAuthMiddleware(username, password string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			u, p, ok := r.BasicAuth()
			if !ok {
				w.Header().Set("WWW-Authenticate", `Basic realm="asynqpg"`)
				writeError(w, http.StatusUnauthorized, "unauthorized", "authentication required")
				return
			}

			usernameMatch := subtle.ConstantTimeCompare([]byte(u), []byte(username)) == 1
			passwordMatch := subtle.ConstantTimeCompare([]byte(p), []byte(password)) == 1

			if !usernameMatch || !passwordMatch {
				w.Header().Set("WWW-Authenticate", `Basic realm="asynqpg"`)
				writeError(w, http.StatusUnauthorized, "unauthorized", "invalid credentials")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

const sessionCookieName = "asynqpg_session"

func sessionAuthMiddleware(store SessionStore) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cookie, err := r.Cookie(sessionCookieName)
			if err != nil || cookie.Value == "" {
				writeError(w, http.StatusUnauthorized, "unauthorized", "authentication required")
				return
			}

			sess, err := store.Get(r.Context(), cookie.Value)
			if err != nil {
				writeError(w, http.StatusUnauthorized, "unauthorized", "invalid or expired session")
				return
			}

			if time.Now().After(sess.ExpiresAt) {
				writeError(w, http.StatusUnauthorized, "unauthorized", "session expired")
				return
			}

			ctx := withUser(r.Context(), &sess.User)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func corsMiddleware(allowedOrigins []string) func(http.Handler) http.Handler {
	originSet := make(map[string]struct{}, len(allowedOrigins))
	for _, o := range allowedOrigins {
		originSet[strings.TrimRight(o, "/")] = struct{}{}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin != "" {
				if _, ok := originSet[origin]; ok {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
					w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
					w.Header().Set("Access-Control-Max-Age", "86400")
				}
			}

			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
