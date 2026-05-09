package ui

// Shared test fixtures used across multiple *_test.go files in package ui.
// These exist to satisfy goconst, which detects repeated string literals
// across all files in a package.
const (
	testUserName          = "Alice"
	testBasicAuthUser     = "admin"
	testBasicAuthPass     = "pass"
	testProviderGithub    = "github"
	testTaskTypeEmailSend = "email.send"
	testTaskTypeReportGen = "report.gen"
	testTaskTypeListPage  = "list-page"
	testExpiredToken      = "expired-token"
)
