# Perspecta Design

This document is intentionally short.
Its primary purpose is consistency during development, not full architecture coverage.

## Design Intent

- Keep behavior predictable across local load, DICOMweb load, GSPS overlays, and history.
- Keep module responsibilities clear to avoid logic drift.
- Prefer incremental, test-backed changes over broad rewrites.

## Module Ownership

- `src/main.rs`: app bootstrap and initial launch request wiring only.
- `src/launch.rs`: parse/validate CLI and `perspecta://` launch inputs.
- `src/dicomweb.rs`: DICOMweb metadata selection and instance download.
- `src/dicom.rs`: DICOM open/classify/decode and GSPS parsing.
- `src/mammo.rs`: mammography ordering/alignment helpers.
- `src/renderer.rs`: pixel buffer to `egui::ColorImage` rendering helpers.
- `src/app.rs`: UI, app state, worker orchestration, interactions, and history.

## Core Invariants

1. Supported group sizes MUST be exactly `1`, `2`, `3`, `4`, or `8`.
2. Multi-view rendering paths MUST apply only to `2`, `3`, `4`, or `8`.
3. Non-image DICOM objects (`DicomPathKind::Other`) MUST NOT be passed to `load_dicom`.
4. GSPS visibility MUST default to off and MUST be user-toggled (`G`).
5. GSPS overlays MUST attach by SOP Instance UID match only.
6. `open_group` MUST be validated/clamped before use.
7. Streaming completion logic MUST compare image counts (not total paths including GSPS).
8. UI state mutations MUST stay on the main thread; workers MUST communicate through channels.

## Change Rules

1. Keep parsing/selection/decode/UI logic in the owning module.
2. Add or update tests when behavior changes.
3. Keep user-facing errors/status messages actionable.
4. Prefer constants and small helpers over repeated literals/branches.
5. If architecture ownership or invariants change, update this file in the same PR.

## Verification Matrix

1. Docs-only changes (`*.md` with no Rust/code changes):
   - Run `cargo fmt --all -- --check`.
   - Run Markdown linting if configured (or apply the docs/no-op CI label flow).
   - Verify Markdown links/rendering.
   - Verify spelling/lint checks pass.
2. UI-only changes (layout/style/labels):
   - Run `cargo fmt --all -- --check`.
   - Run `cargo clippy --all-targets --all-features -- -D warnings`.
   - Run `cargo check --all-targets --all-features`.
3. Launch/parsing/selection changes (`launch.rs`, `dicomweb.rs`, selection logic):
   - Run all UI-only checks above.
   - Run `cargo test --all-targets --all-features`.
4. Streaming/GSPS/history/concurrency changes (`app.rs` load pipeline, GSPS attach, worker channels):
   - Run all launch/parsing checks above.
   - Verify GSPS toggle behavior (default off, `G` works when overlay exists).
