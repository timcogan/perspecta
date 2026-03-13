# AGENTS Rules

- Do not add private information, secrets, local filesystem paths, PHI, or real patient data to code, docs, tests, fixtures, logs, screenshots, or any other version-controlled file; use placeholders or sanitized examples instead.
- Treat "large code changes" as refactors, behavior changes across multiple functions/modules, or edits affecting multiple call sites.
- After large refactors, run `make benchmark` on the baseline and refactor versions with the same `BENCH_*` settings, and report median deltas for `total`, `startup`, `dicom_load`, and `render_ui`.
- Run `cargo fmt --all -- --check` before sending the final response.
- Run `cargo clippy --all-targets --all-features -- -D warnings` before sending the final response.
- Run `cargo test --all-targets --all-features` after large code changes.
- For small changes, run `cargo check --all-targets --all-features` at minimum.
- Add or update tests when behavior changes; if tests are not needed, state why in the final response.
- Treat `unsafe` as an exception: each `unsafe` block must include a short invariant/safety explanation.
- Avoid `unwrap()` and `expect()` in production paths; allow them in tests with clear messages.
- Prefer typed error enums in library-style modules and contextual error propagation in application flows.
- Keep `DESIGN.md` updated when architecture, module ownership, invariants, or core data flows change.
- Consult `RELEASE.md` when a change affects release process, versioning, tags, or publishing workflow.
