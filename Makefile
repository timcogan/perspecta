SHELL := /bin/bash

.DEFAULT_GOAL := help

.PHONY: help build build-local build-release build-release-local run run-local run-release \
	check test fmt fmt-check clippy clean install-watch watch-check watch-test watch-run \
	watch-all dev site install-protocol-linux benchmark local-build-env

BENCH_RUNS ?= 5
BENCH_WARMUP ?= 1
BENCH_ROWS ?= 1024
BENCH_COLS ?= 1024
BENCH_TIMEOUT_SECS ?= 15
BENCH_IMAGES ?= 8
PACKAGE_VERSION := $(shell awk '/^\[package\]$$/ { in_package = 1; next } /^\[/ { in_package = 0 } in_package && /^version = / { gsub(/"/, "", $$3); print $$3; exit }' Cargo.toml)
LOCAL_BUILD_TIMESTAMP ?= $(shell date -u +%Y%m%d%H%M%S)
LOCAL_VERSION_SUFFIX := -$(LOCAL_BUILD_TIMESTAMP)
LOCAL_VERSION := $(PACKAGE_VERSION)$(LOCAL_VERSION_SUFFIX)

help:
	@echo "Perspecta DICOM Viewer - Make targets"
	@echo ""
	@echo "  make run            Run app (debug)"
	@echo "  make run-local      Run app with local prerelease timestamp"
	@echo "  make run-release    Run app (release)"
	@echo "  make build          Build binary (debug)"
	@echo "  make build-local    Build debug binary with local prerelease timestamp"
	@echo "  make build-release  Build binary (release)"
	@echo "  make build-release-local  Build release binary with local prerelease timestamp"
	@echo "  make check          cargo check"
	@echo "  make test           cargo test"
	@echo "  make fmt            cargo fmt"
	@echo "  make fmt-check      cargo fmt --check"
	@echo "  make clippy         cargo clippy -- -D warnings"
	@echo "  make clean          cargo clean"
	@echo "  make benchmark      Run full open benchmark (release); set BENCH_IMAGES=1 or 8"
	@echo ""
	@echo "  make install-watch  Install cargo-watch"
	@echo "  make watch-check    Re-run cargo check on file changes"
	@echo "  make watch-test     Re-run cargo test on file changes"
	@echo "  make watch-run      Re-run cargo run on file changes"
	@echo "  make watch-all      Re-run check + test on file changes"
	@echo "  make dev            Start watch-run, or show install hint if missing"
	@echo "  make site           Run website locally with Hugo"
	@echo "  make install-protocol-linux  Register perspecta:// URL handler (Linux)"

build:
	cargo build

build-local: local-build-env
	PERSPECTA_VERSION_SUFFIX="$(LOCAL_VERSION_SUFFIX)" cargo build

build-release:
	cargo build --release

build-release-local: local-build-env
	PERSPECTA_VERSION_SUFFIX="$(LOCAL_VERSION_SUFFIX)" cargo build --release

run:
	cargo run

run-local: local-build-env
	PERSPECTA_VERSION_SUFFIX="$(LOCAL_VERSION_SUFFIX)" cargo run

run-release:
	cargo run --release

check:
	cargo check --workspace --all-targets --all-features

test:
	cargo test --workspace --all-targets --all-features

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

clean:
	cargo clean

install-watch:
	cargo install cargo-watch

watch-check:
	cargo watch -x 'check --workspace --all-targets --all-features'

watch-test:
	cargo watch -x 'test --workspace --all-targets --all-features'

watch-run:
	cargo watch -x run

watch-all:
	cargo watch -x 'check --workspace --all-targets --all-features' -x 'test --workspace --all-targets --all-features'

dev:
	@if cargo watch --version >/dev/null 2>&1; then \
		cargo watch -x run; \
	else \
		echo "cargo-watch is not installed."; \
		echo "Run: make install-watch"; \
		exit 1; \
	fi

site:
	@command -v hugo >/dev/null 2>&1 || { echo "hugo is not installed."; echo "Install Hugo and retry."; exit 1; }
	hugo server --source website

install-protocol-linux:
	bash scripts/register-protocol-linux.sh

benchmark:
	@cargo build --quiet --release -p perspecta --bin perspecta
	@cargo build --quiet --release -p benchmark-tools --bin benchmark_open
	@./target/release/benchmark_open --app ./target/release/perspecta --runs $(BENCH_RUNS) --warmup $(BENCH_WARMUP) --rows $(BENCH_ROWS) --cols $(BENCH_COLS) --timeout-secs $(BENCH_TIMEOUT_SECS) --images $(BENCH_IMAGES)

local-build-env:
	@if [[ -z "$(PACKAGE_VERSION)" ]]; then \
		echo "Could not determine package version from Cargo.toml" >&2; \
		exit 1; \
	fi
	@if [[ ! "$(LOCAL_BUILD_TIMESTAMP)" =~ ^[0-9]{14}$$ ]]; then \
		echo "LOCAL_BUILD_TIMESTAMP must match YYYYMMDDHHMMSS, got: $(LOCAL_BUILD_TIMESTAMP)" >&2; \
		exit 1; \
	fi
