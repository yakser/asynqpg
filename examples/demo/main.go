package main

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "demo",
		Short: "asynqpg full-stack demo with interactive TUI",
		RunE:  runDemo,
	}

	rootCmd.Flags().StringP("log-level", "l", "info", "Log level: debug, info, warn, error")
	rootCmd.Flags().IntP("tasks", "n", 100, "Number of initial tasks to seed")
	rootCmd.Flags().Bool("no-auto", false, "Disable automatic task generation")
	rootCmd.Flags().Bool("no-logs", false, "Hide log viewport")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func runDemo(cmd *cobra.Command, _ []string) error {
	loadEnvFile(".env")

	levelStr, _ := cmd.Flags().GetString("log-level")
	logLevel, err := parseLogLevel(levelStr)
	if err != nil {
		return err
	}

	initialTasks, _ := cmd.Flags().GetInt("tasks")
	noAuto, _ := cmd.Flags().GetBool("no-auto")
	noLogs, _ := cmd.Flags().GetBool("no-logs")

	logCh := make(chan string, 1000)

	app, err := NewApp(AppConfig{
		LogLevel:     logLevel,
		InitialTasks: initialTasks,
		AutoGenerate: !noAuto,
		LogCh:        logCh,
	})
	if err != nil {
		return fmt.Errorf("failed to start demo: %w", err)
	}

	m := newTUI(app, logCh, !noLogs)
	p := tea.NewProgram(m, tea.WithAltScreen(), tea.WithMouseCellMotion())

	if _, err := p.Run(); err != nil {
		app.Shutdown()
		return fmt.Errorf("TUI error: %w", err)
	}

	fmt.Println("Shutting down, waiting for services to stop...")
	app.Shutdown()

	stats := app.GetStats()
	fmt.Println()
	fmt.Println("Demo finished. Final stats:")
	fmt.Printf("  email processed:        %d\n", stats.EmailProcessed)
	fmt.Printf("  notification processed: %d\n", stats.NotifProcessed)
	fmt.Printf("  report processed:       %d\n", stats.ReportProcessed)
	fmt.Println()

	return nil
}

func parseLogLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid log level %q: use debug, info, warn, or error", s)
	}
}

func loadEnvFile(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		if os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
}
