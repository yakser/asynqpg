package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"golang.org/x/oauth2"
	oauthgithub "golang.org/x/oauth2/github"

	"github.com/yakser/asynqpg/ui"
)

// GitHubAuthProvider implements ui.AuthProvider for GitHub OAuth.
type GitHubAuthProvider struct {
	config oauth2.Config
}

// NewGitHubAuthProvider creates a new GitHub OAuth provider.
// Scopes: read:user (profile info), user:email (email address).
func NewGitHubAuthProvider(clientID, clientSecret string) *GitHubAuthProvider {
	return &GitHubAuthProvider{
		config: oauth2.Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			Scopes:       []string{"read:user", "user:email"},
			Endpoint:     oauthgithub.Endpoint,
		},
	}
}

func (g *GitHubAuthProvider) ID() string          { return "github" }
func (g *GitHubAuthProvider) DisplayName() string { return "GitHub" }
func (g *GitHubAuthProvider) IconURL() string     { return "" }

// BeginAuth redirects to GitHub's authorization page.
func (g *GitHubAuthProvider) BeginAuth(w http.ResponseWriter, r *http.Request, callbackURL string, state string) {
	cfg := g.config
	cfg.RedirectURL = callbackURL

	url := cfg.AuthCodeURL(state, oauth2.AccessTypeOnline)
	http.Redirect(w, r, url, http.StatusFound)
}

// CompleteAuth exchanges the authorization code for a token and fetches user info.
func (g *GitHubAuthProvider) CompleteAuth(_ http.ResponseWriter, r *http.Request) (*ui.User, error) {
	code := r.URL.Query().Get("code")
	if code == "" {
		return nil, fmt.Errorf("missing authorization code")
	}

	ctx := r.Context()
	token, err := g.config.Exchange(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("exchange code: %w", err)
	}

	ghUser, err := fetchGitHubUser(ctx, g.config.Client(ctx, token))
	if err != nil {
		return nil, fmt.Errorf("fetch GitHub user: %w", err)
	}

	return &ui.User{
		ID:        fmt.Sprintf("%d", ghUser.ID),
		Provider:  "github",
		Name:      ghUser.Name,
		AvatarURL: ghUser.AvatarURL,
		Email:     ghUser.Email,
	}, nil
}

type githubUser struct {
	ID        int    `json:"id"`
	Login     string `json:"login"`
	Name      string `json:"name"`
	Email     string `json:"email"`
	AvatarURL string `json:"avatar_url"`
}

func fetchGitHubUser(ctx context.Context, client *http.Client) (*githubUser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com/user", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GitHub API returned status %d", resp.StatusCode)
	}

	var u githubUser
	if err := json.NewDecoder(resp.Body).Decode(&u); err != nil {
		return nil, err
	}

	// Use login as name fallback.
	if u.Name == "" {
		u.Name = u.Login
	}

	return &u, nil
}
