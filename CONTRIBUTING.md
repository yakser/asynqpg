# Contributing to asynqpg

Thank you for your interest in contributing to **asynqpg**. Contributions of all
kinds are welcome, including bug reports, feature ideas, documentation
improvements, tests, and production code.

## Table of Contents

1. [Code of Conduct](#code-of-conduct)
2. [Getting Started](#getting-started)
3. [How to Contribute](#how-to-contribute)
4. [Code Conventions](#code-conventions)
5. [Testing Conventions](#testing-conventions)
6. [Pull Request Guidelines](#pull-request-guidelines)
7. [Getting Help](#getting-help)

## Code of Conduct

By participating in this project, you agree to follow the rules in
[`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md).

## Getting Started

### Prerequisites

- Go 1.25+
- Docker
- PostgreSQL 14+ if you want to run the library against a local database

Integration tests use `testcontainers-go`, so Docker is required even if you do
not run PostgreSQL manually.

### Fork and clone

1. Fork the repository on GitHub.
2. Clone your fork locally.
3. Add the upstream remote if you plan to keep your fork in sync.

```bash
git clone https://github.com/<your-user>/asynqpg.git
cd asynqpg
git remote add upstream https://github.com/yakser/asynqpg.git
```

### Development setup

```bash
make up
make migrate
```

### Run checks locally

```bash
make test
make test-integration
make test-all
make lint
```

## How to Contribute

### Reporting Bugs

Before opening a new issue, please check whether the problem has already been
reported.

When filing a bug report, include:

- Go version
- PostgreSQL version
- Operating system
- Clear reproduction steps
- Expected behavior
- Actual behavior
- Relevant logs, stack traces, or screenshots

Use the bug report template when possible:
<https://github.com/yakser/asynqpg/issues/new/choose>

### Suggesting Features

Open an issue describing:

- The problem you want to solve
- Your proposed solution
- Alternative approaches you considered
- Trade-offs, compatibility concerns, or migration impact

Feature requests are more useful when they explain the user problem first and
the API shape second.

### Submitting Code

1. Fork the repository and create a focused branch from `master`.
2. Make your changes in small, reviewable commits.
3. Add or update tests for the behavior you change.
4. Run `make test` and `make lint` before pushing.
5. Open a pull request against `master`.

## Code Conventions

- Follow the
  [Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md).
- Use English for code, comments, logs, and documentation.
- Keep SQL queries inline inside repository methods.
- Wrap errors with `fmt.Errorf("context: %w", err)`.
- Accept interfaces and return structs.
- Do not use named return values.
- Do not use panics in library code.

## Testing Conventions

- Call `t.Parallel()` at the top of every test.
- Structure tests using Arrange / Act / Assert.
- Use `got` and `want` variable names.
- Use `testify/assert` or `require`.
- Unit tests should mock or stub all external dependencies.
- Integration tests must use the `//go:build integration` build tag and
  `testcontainers-go`.

## Pull Request Guidelines

- Keep pull requests small and focused.
- Reference the related issue when applicable.
- Ensure CI passes before requesting review.
- Add tests for new behavior and bug fixes.
- Update documentation when the public API or workflow changes.

## Release Process

Releases are managed via `scripts/release.py` and automated through GitHub
Actions. Only maintainers create releases.

### Versioning

The repository uses [Semantic Versioning](https://semver.org/). Core and UI
modules are versioned independently with prefixed git tags:

- Core: `v0.5.0` -- `github.com/yakser/asynqpg@v0.5.0`
- UI: `ui/v0.1.0` -- `github.com/yakser/asynqpg/ui@v0.1.0`

### Creating a release

```bash
# Core module
make release-core V=v0.5.0
git push origin master --tags

# UI module (uses latest core tag by default)
make release-ui V=v0.1.0
git push origin master --tags

# UI module pinned to a specific core version
python3 scripts/release.py ui v0.1.0 --core-version v0.4.0
git push origin master --tags
```

When releasing both modules, always release core first and push before releasing
UI, so the Go module proxy can index the new core version.

The release workflow (`.github/workflows/release.yml`) runs on tag push and
automatically:

1. Runs the full test suite as a final gate
2. Generates a changelog from Conventional Commits using git-cliff
3. Creates a GitHub Release
4. Warms the Go module proxy cache

## Getting Help

If you have a question, open an issue and use the `question` label, or start a
discussion in the repository if discussions are enabled.
