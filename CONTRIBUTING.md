# Contributing to (S)AGE

Thanks for your interest in contributing to (S)AGE. This document covers the process for contributing to the project.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/sage.git`
3. Create a branch: `git checkout -b your-feature`
4. Make your changes
5. Run tests before submitting

## Development Setup

```bash
# Start the network
make init
make up

# Run all tests
make test          # 48 Go unit tests
make integration   # 13 integration tests (needs running network)
make sdk-test      # 21 Python SDK tests
make lint          # golangci-lint

# Python SDK development
cd sdk/python
pip install -e ".[dev]"
pytest -v
```

## Pull Requests

1. **One PR per feature/fix** -- keep changes focused
2. **Write tests** -- new functionality needs test coverage
3. **Run the full test suite** before submitting (`make test && make lint && make sdk-test`)
4. **Follow existing code style** -- Go: gofmt + golangci-lint, Python: existing conventions
5. **Update documentation** if your change affects the public API
6. **Describe what and why** in the PR description, not just what files changed

### PR Title Format

Use a clear, descriptive title:
- `Add vector similarity threshold to query endpoint`
- `Fix epoch score calculation for cold-start agents`
- `Update Python SDK to support domain hierarchies`

### What Makes a Good PR

- Passes all CI checks (lint, test, build, docker, sdk-test)
- Includes tests for new functionality
- Doesn't break existing tests
- Keeps commits atomic and well-described
- Updates relevant documentation

## Code Style

### Go
- Follow standard Go conventions (`gofmt`, `go vet`)
- golangci-lint must pass (config in `.golangci.yml`)
- Use `testify` for test assertions
- Use build tags for integration tests (`//go:build integration`)

### Python
- Python 3.10+ required
- Use type hints throughout
- Pydantic v2 for data models
- httpx for HTTP (sync + async)
- pytest + respx for testing

## Architecture Notes

Before making significant changes, please understand:

- **Determinism is absolute** in the ABCI state machine. No `time.Now()`, no random map iteration, no `fmt.Sprintf` on floats
- **PostgreSQL writes only in Commit** -- never in FinalizeBlock
- **On-chain = hashes only** (BadgerDB), off-chain = full content (PostgreSQL)
- **SDK URL paths** use `/v1/memory/` (singular) -- keep in sync with server routes

See `CLAUDE.md` for the full list of critical implementation rules.

## Reporting Issues

Open an issue on GitHub with:
- What you expected to happen
- What actually happened
- Steps to reproduce
- Relevant logs (`make logs-abci`)
- Your environment (OS, Docker version, Go version)

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
