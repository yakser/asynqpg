package main

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("12"))

	headerStyle = lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("63")).
			Padding(0, 1)

	urlLabelStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("7"))

	urlValueStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("14")).
			Underline(true)

	separatorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("8"))

	statusBarStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("8"))

	promptStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("63")).
			Bold(true)

	statusMsgStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("10"))

	focusIndicator = lipgloss.NewStyle().
			Foreground(lipgloss.Color("63")).
			Bold(true)
)

type logBatchMsg []string

type tuiModel struct {
	app       *App
	viewport  viewport.Model
	textInput textinput.Model

	logs  []string
	logCh <-chan string

	ready        bool
	width        int
	height       int
	inputFocused bool
	showLogs     bool
	status       string
}

func newTUI(app *App, logCh <-chan string, showLogs bool) tuiModel {
	ti := textinput.New()
	ti.Placeholder = "Type a command (help for usage)"
	ti.Focus()
	ti.CharLimit = 256

	return tuiModel{
		app:          app,
		textInput:    ti,
		logCh:        logCh,
		inputFocused: true,
		showLogs:     showLogs,
	}
}

func (m tuiModel) Init() tea.Cmd {
	return waitForLogs(m.logCh)
}

func (m tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m.handleResize(), nil

	case logBatchMsg:
		return m.handleLogs(msg)

	case statusMsg:
		m.status = string(msg)
		return m, nil

	case tea.MouseMsg:
		if m.showLogs && m.ready {
			var cmd tea.Cmd
			m.viewport, cmd = m.viewport.Update(msg)
			return m, cmd
		}
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(msg)
	}

	return m, nil
}

func (m tuiModel) View() string {
	if !m.ready {
		return "  Starting demo..."
	}

	var b strings.Builder

	b.WriteString(m.headerView())
	b.WriteString("\n")

	if m.showLogs {
		b.WriteString(m.viewport.View())
		b.WriteString("\n")
	}

	b.WriteString(m.footerView())

	return b.String()
}

func (m tuiModel) handleResize() tuiModel {
	headerH := lipgloss.Height(m.headerView())
	footerH := lipgloss.Height(m.footerView())
	vpHeight := m.height - headerH - footerH - 1

	if vpHeight < 1 {
		vpHeight = 1
	}

	if !m.ready {
		m.viewport = viewport.New(m.width, vpHeight)
		m.viewport.SetContent(strings.Join(m.logs, "\n"))
		m.ready = true
	} else {
		m.viewport.Width = m.width
		m.viewport.Height = vpHeight
	}

	m.textInput.Width = m.width - 6

	return m
}

func (m tuiModel) handleLogs(entries logBatchMsg) (tuiModel, tea.Cmd) {
	m.logs = append(m.logs, entries...)

	const maxLines = 10000
	if len(m.logs) > maxLines {
		m.logs = m.logs[len(m.logs)-maxLines:]
	}

	if m.showLogs && m.ready {
		atBottom := m.viewport.AtBottom()
		m.viewport.SetContent(strings.Join(m.logs, "\n"))
		if atBottom {
			m.viewport.GotoBottom()
		}
	}

	return m, waitForLogs(m.logCh)
}

func (m tuiModel) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyCtrlC:
		return m, tea.Quit

	case tea.KeyCtrlL:
		m.logs = nil
		if m.ready {
			m.viewport.SetContent("")
		}
		return m, nil

	case tea.KeyTab:
		if m.showLogs {
			m.inputFocused = !m.inputFocused
			if m.inputFocused {
				m.textInput.Focus()
			} else {
				m.textInput.Blur()
			}
		}
		return m, nil
	}

	if m.inputFocused {
		return m.handleInputKey(msg)
	}

	return m.handleViewportKey(msg)
}

func (m tuiModel) handleInputKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEnter:
		input := m.textInput.Value()
		m.textInput.SetValue("")
		m.status = ""

		result := runCommand(m.app, input)
		if result.clear {
			m.logs = nil
			if m.ready {
				m.viewport.SetContent("")
			}
			m.status = "Logs cleared"
			return m, nil
		}
		if result.status != "" {
			m.status = result.status
		}
		return m, result.cmd

	case tea.KeyPgUp, tea.KeyPgDown, tea.KeyUp, tea.KeyDown:
		if m.showLogs {
			var cmd tea.Cmd
			m.viewport, cmd = m.viewport.Update(msg)
			return m, cmd
		}
		return m, nil
	}

	var cmd tea.Cmd
	m.textInput, cmd = m.textInput.Update(msg)
	return m, cmd
}

func (m tuiModel) handleViewportKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	m.viewport, cmd = m.viewport.Update(msg)
	return m, cmd
}

func (m tuiModel) headerView() string {
	title := titleStyle.Render("asynqpg Demo")

	urls := fmt.Sprintf(
		"%s %s\n%s %s\n%s %s\n%s %s",
		urlLabelStyle.Render("  Web UI:     "), urlValueStyle.Render(fmt.Sprintf("http://localhost%s", m.app.Addr)),
		urlLabelStyle.Render("  Jaeger:     "), urlValueStyle.Render("http://localhost:16686"),
		urlLabelStyle.Render("  Grafana:    "), urlValueStyle.Render("http://localhost:3000"),
		urlLabelStyle.Render("  Prometheus: "), urlValueStyle.Render("http://localhost:9090"),
	)

	autoStatus := statusMsgStyle.Render("ON")
	if !m.app.AutoRunning() {
		autoStatus = lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Render("OFF")
	}

	statusLine := fmt.Sprintf("  Consumers: 2  |  Auth: %s  |  Auto: %s  |  Seeded: %d",
		m.app.AuthMode, autoStatus, m.app.Seeded)

	content := fmt.Sprintf("%s\n\n%s\n\n%s", title, urls, statusLine)

	w := m.width - 2
	if w < 20 {
		w = 20
	}

	return headerStyle.Width(w).Render(content)
}

func (m tuiModel) footerView() string {
	separator := separatorStyle.Render(strings.Repeat("─", m.width))

	var focusHint string
	if m.showLogs {
		if m.inputFocused {
			focusHint = " [Tab: scroll logs]"
		} else {
			focusHint = focusIndicator.Render(" [Tab: input]")
		}
	}

	input := fmt.Sprintf(" %s %s", promptStyle.Render(">"), m.textInput.View())

	helpText := "  enqueue <type> [N] | auto on/off | stats | clear | help | Ctrl+C to quit"
	if m.status != "" {
		helpText = "  " + statusMsgStyle.Render(m.status)
	}

	return fmt.Sprintf("%s\n%s%s\n%s",
		separator,
		input, statusBarStyle.Render(focusHint),
		statusBarStyle.Render(helpText),
	)
}

func waitForLogs(ch <-chan string) tea.Cmd {
	return func() tea.Msg {
		entry, ok := <-ch
		if !ok {
			return nil
		}
		entries := []string{entry}
		for {
			select {
			case e, ok := <-ch:
				if !ok {
					return logBatchMsg(entries)
				}
				entries = append(entries, e)
				if len(entries) >= 100 {
					return logBatchMsg(entries)
				}
			default:
				return logBatchMsg(entries)
			}
		}
	}
}
