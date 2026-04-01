# Contributing

Thanks for your interest in contributing to Perspecta. Bug reports, documentation
improvements, and focused code changes are all welcome.
For security vulnerabilities, do not open a public issue; follow
[SECURITY.md](SECURITY.md) instead.

## Before You Start

- Keep changes small and focused when possible.
- If you are planning a larger change or are unsure about the direction, open an
  issue first before investing heavily in implementation.

## Development Checks

Before opening a pull request, run:

```bash
make fmt-check
make clippy
make test
```

For local setup, run commands, and general development notes, see
[README.md](README.md).

## Pull Requests

- Use a short, descriptive branch name and pull request title.
- Explain what changed and how you tested it.
- Add or update tests when behavior changes.
- Include screenshots when a pull request changes visible UI behavior.

## Community

Please follow the project [Code of Conduct](CODE_OF_CONDUCT.md).
