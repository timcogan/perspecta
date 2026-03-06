# AGENTS Rules

- Treat "large code changes" as refactors, behavior changes across multiple functions/modules, or edits affecting multiple call sites.
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
