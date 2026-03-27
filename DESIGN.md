# Perspecta Design

This document is intentionally short.
Its primary purpose is consistency during development, not full architecture coverage.

## Design Intent

- Keep behavior predictable across local load, DICOMweb load, GSPS overlays, Parametric Map overlays, SR documents, and history.
- Keep module responsibilities clear to avoid logic drift.
- Prefer incremental, test-backed changes over broad rewrites.

## Module Ownership

- `src/main.rs`: app bootstrap and initial launch request wiring only.
- `src/launch.rs`: parse/validate CLI and `perspecta://` launch inputs.
- `src/dicomweb.rs`: DICOMweb metadata selection and instance download.
- `src/dicom.rs`, `src/dicom/*`: DICOM facade, shared object open/classify/decode helpers, and format-specific parsers.
- `src/mammo.rs`: mammography ordering/alignment helpers.
- `src/renderer.rs`: pixel buffer to `egui::ColorImage` rendering helpers.
- `src/logging.rs`: logging setup and log-level configuration.
- `src/app.rs`: UI, application state, interactions, and worker orchestration.
- `src/app/overlay.rs`: overlay reconciliation, authoritative overlay snapshots, and overlay availability/navigation.
- `src/app/load.rs`: launch/open/load orchestration and DICOMweb/local load pipelines.
- `src/app/history.rs`: history management and preload/orchestration.
- `tools/benchmark`: development-only end-to-end benchmark tools and synthetic DICOM generation.

## Core Invariants

1. Supported primary displayable group sizes MUST be exactly `1`, `2`, `3`, `4`, or `8`; supplementary GSPS/SR/Parametric Map objects do not count toward that total.
2. Multi-view rendering paths MUST apply only to `2`, `3`, `4`, or `8`.
3. Non-image DICOM objects (`DicomPathKind::Other`), Structured Reports, and Parametric Maps MUST NOT be passed to `load_dicom`.
4. Structured Reports MUST load through the dedicated SR parser and single-document UI path.
5. Parametric Maps MUST load through the dedicated Parametric Map parser; they may render as standalone images or attach as supplemental overlays depending on explicit source-image references.
6. Mixed image+SR selections MUST stage SR documents as separate history entries, not image viewports.
7. Supplemental overlay visibility MUST default to off and MUST be user-toggled (`G`).
8. GSPS overlays MUST attach by SOP Instance UID match only.
9. Mammography CAD SR overlays MUST attach by direct referenced-image SOP Instance UID match only, and only `Presentation Required` findings participate in render/navigation.
10. Parametric Map overlays MUST attach only when they carry explicit source-image SOP Instance UID references; no geometry-only or study/series heuristics are allowed.
11. Parametric Map overlays MUST render beneath GSPS and Mammography CAD SR vector overlays.
12. `open_group` MUST be validated/clamped before use.
13. Streaming completion logic MUST compare image counts (not total paths including GSPS/SR).
14. UI state mutations MUST stay on the main thread; workers MUST communicate through channels.
15. Production diagnostics MUST use logging (`log` macros), not `println!/eprintln!`.
16. DICOMweb metadata parsing MUST use top-level instance identifiers; nested reference tags inside GSPS/SR sequences MUST NOT override the owning series or instance identity.
17. Grouped DICOMweb launch MUST resolve and stream the `open_group` before background groups so first-image latency is driven by the active group only.
18. Background DICOMweb groups MUST stage into history as each group download completes; history thumbnails and group switching MUST NOT wait for the final grouped download result.
19. If the user switches away from a streaming DICOMweb active group, remaining active-group work MUST continue staging into history and MUST NOT clear, replace, or visually mask the currently displayed study.
20. Multi-frame images with per-frame `ImagePositionPatient` MUST expose frames in logical patient-position order; if the dominant per-frame patient-position progression increases across stored frames, display and cine MUST reverse with it, and GSPS/SR frame lookups MUST translate the displayed frame back to the referenced stored DICOM frame.
21. DICOM content inside the viewer MUST use explicit `DicomSource` ownership; DICOMweb bytes MUST be represented as `DicomSource::Memory`, not temp files or a global backing store.

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
   - Run `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
   - Run `cargo check --workspace --all-targets --all-features`.
3. Launch/parsing/selection changes (`launch.rs`, `dicomweb.rs`, selection logic):
   - Run all UI-only checks above.
   - Run `cargo test --workspace --all-targets --all-features --locked`.
4. Decode/ordering/rendering changes (`dicom.rs`, `mammo.rs`, `renderer.rs`):
   - Run all UI-only checks above.
   - Run `cargo test --workspace --all-targets --all-features --locked`.
   - Run module-specific validations for decode and renderer output tests.
5. Streaming/overlay/history/concurrency changes (`app/overlay.rs` GSPS/SR/Parametric Map attach helpers and overlay toggle/navigation, `app/load.rs` launch/load pipeline and worker channels, `app/history.rs` history/preload orchestration):
   - Run all launch/parsing checks above.
   - Run paired baseline vs refactor benchmark runs via `make benchmark` with identical `BENCH_*` environment settings.
   - Report median deltas for `total`, `startup`, `dicom_load`, and `render_ui`, and summarize any regressions before approving the PR.
   - Confirm SR-only open uses the dedicated SR parser/UI path and that `load_dicom` rejects SR objects.
   - Confirm Parametric Map-only open uses the dedicated parser/UI path and that `load_dicom` rejects Parametric Map objects.
   - Ensure mixed image+SR selections keep images in active viewports while staging SR documents as separate history entries, without regressing GSPS/SR/Parametric Map/history/streaming invariants.
   - Confirm overlay toggle behavior (default off; `G` only toggles when a GSPS, Mammography CAD SR, or matching Parametric Map overlay is available).
6. Tooling/benchmark changes (`tools/benchmark`, workspace manifests, Makefile/CI command wiring):
   - Update manifests, Makefile targets, and CI command wiring as required.
   - Run `cargo fmt --all -- --check`.
   - Run `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
   - Run `cargo test --workspace --all-targets --all-features --locked`.
   - Run paired baseline vs refactor benchmark runs via `make benchmark` with identical `BENCH_*` environment settings, and include a short summary of median deltas/regressions for `total`, `startup`, `dicom_load`, and `render_ui`.
   - If benchmark launch flow changed, build both `cargo build --release -p perspecta --bin perspecta` and `cargo build --release -p benchmark-tools --bin benchmark_full_single_open`.
