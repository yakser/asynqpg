package uiauth

import (
	"errors"
	"time"
)

// ErrSessionNotFound is returned when a session token is not found in the store.
var ErrSessionNotFound = errors.New("session not found")

// User represents an authenticated user from any OAuth/SSO provider.
type User struct {
	ID        string `json:"id"`
	Provider  string `json:"provider"`
	Name      string `json:"name"`
	AvatarURL string `json:"avatar_url"`
	Email     string `json:"email"`
}

// Session represents an authenticated user session.
type Session struct {
	Token     string
	User      User
	CreatedAt time.Time
	ExpiresAt time.Time
}
