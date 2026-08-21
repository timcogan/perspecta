# Perspecta Design

This document is intentionally short.
Its primary purpose is consistency during development, not full architecture coverage.

## Design Intent

- Keep behavior predictable across local load, DICOMweb load, GSPS overlays, Parametric Map overlays, SR documents, and history.
- Keep module responsibilities clear to avoid logic drift.
- Prefer incremental, test-backed changes over broad rewrites.

## Module Ownership

- `src/main.rs`: thin native executable entry point only.
- `src/lib.rs`: shared crate composition plus native and browser bootstrap wiring.
- `src/platform.rs`: target-specific task scheduling; native threads, deferred browser tasks, and cooperative browser yields.
- `src/launch.rs`: parse/validate CLI and `perspecta://` launch inputs.
- `src/dicomweb.rs`: DICOMweb metadata selection and instance download.
- `src/dicomweb_web.rs`: browser-only unavailable-service shim; the static preview never performs DICOMweb requests.
- `src/dicom.rs`, `src/dicom/*`: DICOM facade, shared object open/classify/decode helpers, pixel spacing extraction, and format-specific parsers.
- `src/mammo.rs`: mammography ordering/alignment helpers.
- `src/renderer.rs`: pixel buffer to `egui::ColorImage` rendering helpers.
- `src/logging.rs`: logging setup and log-level configuration.
- `src/app.rs`: UI, application state, interactions, and worker orchestration.
- `src/app/measurement.rs`: live measurement state, coordinate transforms, and distance formatting.
- `src/app/metadata.rs`: metadata overlay, metadata popup, and active-object metadata presentation.
- `src/app/overlay.rs`: overlay reconciliation, authoritative overlay snapshots, and overlay availability/navigation.
- `src/app/load.rs`: launch/open/load orchestration and DICOMweb/local load pipelines.
- `src/app/history.rs`: history management and preload/orchestration.
- `tools/benchmark`: development-only end-to-end benchmark tools and synthetic DICOM generation.
- `website/layouts/demo`, `website/content/demo`, and `website/static/js/demo-loader.js`: privacy-isolated Hugo shell and discovery metadata for the browser preview.
- `scripts/build-web-demo.sh` and `scripts/verify-web-demo.sh`: reproducible, content-hashed WASM packaging and Pages privacy/artifact verification.

## Core Invariants

1. Supported primary displayable group sizes MUST be exactly `1`, `2`, `3`, `4`, or `8`; supplementary GSPS/SR/Parametric Map objects do not count toward that total.
2. Multi-view rendering paths MUST apply only to `2`, `3`, `4`, or `8`.
3. Non-image DICOM objects (`DicomPathKind::Other`), Structured Reports, and Parametric Maps MUST NOT be passed to `load_dicom`.
4. Structured Reports MUST load through the dedicated SR parser and single-document UI path.
5. Parametric Maps MUST load through the dedicated Parametric Map parser; they may render as standalone images or attach as supplemental overlays depending on explicit source-image references.
6. Mixed image+SR selections MUST stage SR documents as separate history entries, not image viewports.
7. Supplemental overlay visibility MUST default to off and MUST be user-toggled (`G`).
8. GSPS overlays MUST attach by SOP Instance UID match only.
9. Mammography CAD SR overlays MUST attach by direct referenced-image SOP Instance UID match only. Only `Presentation Required` vector findings participate in render/navigation. Visible SR geometry may carry a short text label derived from the same finding metadata (for example finding meaning, laterality/view, and certainty). Non-geometric descriptive SR content remains available through the document view only.
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
22. Visible metadata field settings MUST apply only to the summary overlay; the full metadata popup MUST ignore that filter and show all extracted fields for the active object.
23. Live measurements MUST be stored in image coordinates, not screen coordinates, so zoom and pan do not change their geometry.
24. Live measurements are transient UI state only; they MUST NOT persist into history entries and MUST clear on frame or study/context changes.
25. Native and browser builds MUST share the same `DicomViewerApp`. Platform-specific bootstrap, scheduling, file selection, window controls, caching, and unavailable-service behavior belong in target adapters. UI state and texture uploads stay on the egui thread; native targets use workers for expensive preparation, while browser load tasks start outside the initiating call stack and yield cooperatively between load stages.
26. Browser-selected DICOM content MUST remain in local memory as `DicomSource::Memory`; it MUST NOT be uploaded, persisted, imported by URL, or sent to DICOMweb. The `/demo/` surface remains same-origin, analytics-free, third-party-free, and explicitly not for diagnostic use.
27. The browser preview MUST enforce 512 MiB per file, 1 GiB of uniquely retained input bytes, and 192,000,000 retained decoded pixels using checked preflight accounting. Decode dimensions, caches, and reservations MUST remain bounded by actual resident data, and the active study MUST NOT be evicted implicitly.
28. Browser builds MUST remain single-threaded on Glow/WebGL, use `platform::MonotonicInstant` for shared elapsed-time state, and exclude browser-incompatible JPEG 2000/JPEG-LS codecs without changing desktop defaults.
29. GitHub Pages MUST build from the exact triggering `master` commit and publish that commit identity in the artifact, footer, and `+web.<7-hex>` display version. Local builds MUST disclose a dirty source tree instead of presenting it as clean.
30. The configurable secondary color MUST cover live measurements, selected image/history borders, and GSPS/SR graphics and labels. Desktop builds persist it with the existing user settings; browser builds keep it only for the current session.

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
7. Browser-preview changes (`src/lib.rs` browser bootstrap, target-specific branches, `website/**`, web build scripts, or Pages workflow):
   - Run `cargo clippy --target wasm32-unknown-unknown --all-targets --no-default-features -- -D warnings`.
   - Run `make web-pages`.
   - Exercise picker and drag/drop with synthetic DICOM, covering an accepted selection and resource-limit rejection.
