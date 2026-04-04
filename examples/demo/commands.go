package main

import (
	"fmt"
	"strconv"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

type statusMsg string

type cmdResult struct {
	status string
	cmd    tea.Cmd
	clear  bool
}

func runCommand(app *App, input string) cmdResult {
	parts := strings.Fields(strings.TrimSpace(input))
	if len(parts) == 0 {
		return cmdResult{}
	}

	switch parts[0] {
	case "enqueue", "e":
		return runEnqueue(app, parts[1:])

	case "auto":
		return runAuto(app, parts[1:])

	case "stats":
		s := app.GetStats()
		return cmdResult{
			status: fmt.Sprintf("Processed -- email: %d, notification: %d, report: %d",
				s.EmailProcessed, s.NotifProcessed, s.ReportProcessed),
		}

	case "clear":
		return cmdResult{clear: true}

	case "help", "h", "?":
		return cmdResult{
			status: "Commands: enqueue <type> [N], auto on|off, stats, clear, help, quit",
		}

	case "quit", "q", "exit":
		return cmdResult{cmd: tea.Quit}

	default:
		return cmdResult{
			status: fmt.Sprintf("Unknown command: %s. Type 'help' for usage.", parts[0]),
		}
	}
}

func runEnqueue(app *App, args []string) cmdResult {
	if len(args) == 0 {
		return cmdResult{status: "Usage: enqueue <email|notification|report> [count]"}
	}

	taskType := args[0]
	count := 1

	if len(args) >= 2 {
		n, err := strconv.Atoi(args[1])
		if err != nil || n <= 0 {
			return cmdResult{status: "Invalid count: must be a positive integer"}
		}
		count = n
	}

	return cmdResult{
		cmd: func() tea.Msg {
			if err := app.EnqueueTasks(taskType, count); err != nil {
				return statusMsg(fmt.Sprintf("Error: %v", err))
			}
			return statusMsg(fmt.Sprintf("Enqueued %d %s task(s)", count, taskType))
		},
	}
}

func runAuto(app *App, args []string) cmdResult {
	if len(args) == 0 {
		return cmdResult{status: "Usage: auto on|off"}
	}

	switch args[0] {
	case "on":
		app.StartAutoGenerate()
		return cmdResult{status: "Auto-generation enabled"}
	case "off":
		app.StopAutoGenerate()
		return cmdResult{status: "Auto-generation disabled"}
	default:
		return cmdResult{status: "Usage: auto on|off"}
	}
}
