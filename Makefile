SHELL := /bin/bash

.DEFAULT_GOAL := help

.PHONY: help build build-release run run-release check test fmt fmt-check clippy clean \
	install-watch watch-check watch-test watch-run watch-all dev install-protocol-linux

help:
	@echo "Perspecta Viewer - Make targets"
	@echo ""
	@echo "  make run            Run app (debug)"
	@echo "  make run-release    Run app (release)"
	@echo "  make build          Build binary (debug)"
	@echo "  make build-release  Build binary (release)"
	@echo "  make check          cargo check"
	@echo "  make test           cargo test"
	@echo "  make fmt            cargo fmt"
	@echo "  make fmt-check      cargo fmt --check"
	@echo "  make clippy         cargo clippy -- -D warnings"
	@echo "  make clean          cargo clean"
	@echo ""
	@echo "  make install-watch  Install cargo-watch"
	@echo "  make watch-check    Re-run cargo check on file changes"
	@echo "  make watch-test     Re-run cargo test on file changes"
	@echo "  make watch-run      Re-run cargo run on file changes"
	@echo "  make watch-all      Re-run check + test on file changes"
	@echo "  make dev            Start watch-run, or show install hint if missing"
	@echo "  make install-protocol-linux  Register perspecta:// URL handler (Linux)"

build:
	cargo build

build-release:
	cargo build --release

run:
	cargo run

run-release:
	cargo run --release

check:
	cargo check

test:
	cargo test

fmt:
	cargo fmt

fmt-check:
	cargo fmt -- --check

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

clean:
	cargo clean

install-watch:
	cargo install cargo-watch

watch-check:
	cargo watch -x check

watch-test:
	cargo watch -x test

watch-run:
	cargo watch -x run

watch-all:
	cargo watch -x check -x test

dev:
	@if cargo watch --version >/dev/null 2>&1; then \
		cargo watch -x run; \
	else \
		echo "cargo-watch is not installed."; \
		echo "Run: make install-watch"; \
		exit 1; \
	fi

install-protocol-linux:
	bash scripts/register-protocol-linux.sh
