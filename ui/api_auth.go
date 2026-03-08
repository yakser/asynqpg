package ui

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	oauthStateCookieName = "asynqpg_oauth_state"
	oauthStateMaxAge     = 600 // 10 minutes
	oauthStateBytes      = 32
)

type authProviderResponse struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	IconURL  string `json:"icon_url"`
	LoginURL string `json:"login_url"`
}

// handleAuthProviders returns the list of configured OAuth providers.
func (h *handler) handleAuthProviders(w http.ResponseWriter, _ *http.Request) {
	providers := make([]authProviderResponse, 0, len(h.opts.AuthProviders))
	for _, p := range h.opts.AuthProviders {
		providers = append(providers, authProviderResponse{
			ID:       p.ID(),
			Name:     p.DisplayName(),
			IconURL:  p.IconURL(),
			LoginURL: fmt.Sprintf("/api/auth/login/%s", p.ID()),
		})
	}
	writeJSON(w, http.StatusOK, providers)
}

// handleAuthMe returns the currently authenticated user.
func (h *handler) handleAuthMe(w http.ResponseWriter, r *http.Request) {
	user := UserFromContext(r.Context())
	if user == nil {
		writeError(w, http.StatusUnauthorized, "unauthorized", "not authenticated")
		return
	}
	writeJSON(w, http.StatusOK, user)
}

// handleAuthLogin initiates the OAuth flow for the specified provider.
func (h *handler) handleAuthLogin(w http.ResponseWriter, r *http.Request) {
	providerID := r.PathValue("provider")
	provider := h.findProvider(providerID)
	if provider == nil {
		writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("unknown auth provider: %s", providerID))
		return
	}

	state, err := generateOAuthState()
	if err != nil {
		h.logger.Error("generate OAuth state", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to initiate login")
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     oauthStateCookieName,
		Value:    state,
		Path:     "/api/auth/",
		MaxAge:   oauthStateMaxAge,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   h.opts.SecureCookies,
	})

	callbackURL := h.buildCallbackURL(r, providerID)
	provider.BeginAuth(w, r, callbackURL, state)
}

// handleAuthCallback handles the OAuth callback from the provider.
func (h *handler) handleAuthCallback(w http.ResponseWriter, r *http.Request) {
	providerID := r.PathValue("provider")
	provider := h.findProvider(providerID)
	if provider == nil {
		writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("unknown auth provider: %s", providerID))
		return
	}

	// Validate CSRF state.
	stateCookie, err := r.Cookie(oauthStateCookieName)
	if err != nil || stateCookie.Value == "" {
		h.redirectToLogin(w, r, "invalid_state", "")
		return
	}

	urlState := r.URL.Query().Get("state")
	if subtle.ConstantTimeCompare([]byte(stateCookie.Value), []byte(urlState)) != 1 {
		h.redirectToLogin(w, r, "invalid_state", "")
		return
	}

	// Clear state cookie.
	http.SetCookie(w, &http.Cookie{
		Name:     oauthStateCookieName,
		Value:    "",
		Path:     "/api/auth/",
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   h.opts.SecureCookies,
	})

	user, err := provider.CompleteAuth(w, r)
	if err != nil {
		h.logger.Error("OAuth provider error", "provider", providerID, "error", err)
		h.redirectToLogin(w, r, "provider_error", err.Error())
		return
	}

	// Create session.
	token, err := generateSessionToken()
	if err != nil {
		h.logger.Error("failed to generate session token", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to create session")
		return
	}

	now := time.Now()
	session := &Session{
		Token:     token,
		User:      *user,
		CreatedAt: now,
		ExpiresAt: now.Add(h.opts.SessionMaxAge),
	}

	if err := h.sessionStore.Save(r.Context(), session); err != nil {
		h.logger.Error("failed to save session", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to create session")
		return
	}

	cookiePath := "/"
	if h.opts.Prefix != "/" {
		cookiePath = h.opts.Prefix
	}

	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     cookiePath,
		MaxAge:   int(h.opts.SessionMaxAge.Seconds()),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   h.opts.SecureCookies,
	})

	http.Redirect(w, r, h.prefixPath("/dashboard"), http.StatusFound)
}

// handleAuthLogout deletes the current session and clears the cookie.
func (h *handler) handleAuthLogout(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie(sessionCookieName)
	if err == nil && cookie.Value != "" {
		if delErr := h.sessionStore.Delete(r.Context(), cookie.Value); delErr != nil {
			h.logger.Error("failed to delete session", "error", delErr)
		}
	}

	cookiePath := "/"
	if h.opts.Prefix != "/" {
		cookiePath = h.opts.Prefix
	}

	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     cookiePath,
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   h.opts.SecureCookies,
	})

	writeJSON(w, http.StatusOK, map[string]string{"status": "logged_out"})
}

func (h *handler) findProvider(id string) AuthProvider {
	for _, p := range h.opts.AuthProviders {
		if p.ID() == id {
			return p
		}
	}
	return nil
}

func (h *handler) buildCallbackURL(r *http.Request, providerID string) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if fwdProto := r.Header.Get("X-Forwarded-Proto"); fwdProto != "" {
		scheme = fwdProto
	}

	host := r.Host
	prefix := strings.TrimRight(h.opts.Prefix, "/")
	return fmt.Sprintf("%s://%s%s/api/auth/callback/%s", scheme, host, prefix, providerID)
}

func (h *handler) redirectToLogin(w http.ResponseWriter, r *http.Request, errCode, errMsg string) {
	loginPath := h.prefixPath("/login")
	params := url.Values{}
	params.Set("error", errCode)
	if errMsg != "" {
		params.Set("message", errMsg)
	}
	http.Redirect(w, r, loginPath+"?"+params.Encode(), http.StatusFound)
}

func (h *handler) prefixPath(path string) string {
	prefix := strings.TrimRight(h.opts.Prefix, "/")
	if prefix == "" {
		return path
	}
	return prefix + path
}

func generateOAuthState() (string, error) {
	b := make([]byte, oauthStateBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate OAuth state: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
