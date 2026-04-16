package main

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

var (
	debugLevelStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	infoLevelStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("12"))
	warnLevelStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("11"))
	errorLevelStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
	timeStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	attrStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
)

// ChannelHandler is a slog.Handler that renders log records with colored levels
// and sends them to a buffered channel for TUI consumption.
type ChannelHandler struct {
	ch    chan<- string
	level slog.Level
	attrs []slog.Attr
	group string
}

func NewChannelHandler(ch chan<- string, level slog.Level) *ChannelHandler {
	return &ChannelHandler{ch: ch, level: level}
}

func (h *ChannelHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *ChannelHandler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder

	b.WriteString(timeStyle.Render(r.Time.Format("15:04:05")))
	b.WriteString(" ")

	levelStr, style := formatLevel(r.Level)
	b.WriteString(style.Render(levelStr))
	b.WriteString(" ")
	b.WriteString(r.Message)

	for _, a := range h.attrs {
		b.WriteString("  ")
		b.WriteString(attrStyle.Render(formatAttr(h.group, a)))
	}

	r.Attrs(func(a slog.Attr) bool {
		b.WriteString("  ")
		b.WriteString(attrStyle.Render(formatAttr(h.group, a)))
		return true
	})

	select {
	case h.ch <- b.String():
	default:
	}

	return nil
}

func (h *ChannelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ChannelHandler{
		ch:    h.ch,
		level: h.level,
		attrs: append(slices.Clone(h.attrs), attrs...),
		group: h.group,
	}
}

func (h *ChannelHandler) WithGroup(name string) slog.Handler {
	g := name
	if h.group != "" {
		g = h.group + "." + name
	}
	return &ChannelHandler{
		ch:    h.ch,
		level: h.level,
		attrs: slices.Clone(h.attrs),
		group: g,
	}
}

func formatLevel(level slog.Level) (string, lipgloss.Style) {
	switch {
	case level < slog.LevelInfo:
		return "DEBUG", debugLevelStyle
	case level < slog.LevelWarn:
		return "INFO ", infoLevelStyle
	case level < slog.LevelError:
		return "WARN ", warnLevelStyle
	default:
		return "ERROR", errorLevelStyle
	}
}

func formatAttr(group string, a slog.Attr) string {
	key := a.Key
	if group != "" {
		key = group + "." + key
	}
	return fmt.Sprintf("%s=%s", key, a.Value.String())
}
