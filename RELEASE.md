# Release Process

This project uses Semantic Versioning and publishes binaries via GitHub Actions.
The release workflow watches for a version bump in `Cargo.toml` on `main` or
`master`, then creates a draft `vX.Y.Z` GitHub Release and corresponding tag
automatically before `cargo-dist` uploads artifacts and publishes it.
If a release needs to be retried, the workflow can also be started manually with
GitHub Actions `workflow_dispatch`.

## Versioning Rules (SemVer)

- **MAJOR** (`X.0.0`): Breaking changes or incompatible behavior.
- **MINOR** (`0.X.0`): Backwards-compatible features.
- **PATCH** (`0.0.X`): Backwards-compatible bug fixes.

## Steps to Publish a Release

1. Update the version in `Cargo.toml`:
   - `version = "X.Y.Z"`
2. Sync `Cargo.lock` if the package version entry changed.
3. Commit the release change:
   - Example message: `chore(release): vX.Y.Z`
4. Push the commit to `main` or `master`.
5. GitHub Actions will:
   - detect that the package version changed
   - create a draft `vX.Y.Z` GitHub Release and corresponding tag
   - let `cargo-dist` upload release artifacts into that draft
   - publish the finished GitHub Release automatically
6. If a release fails after the version bump landed, rerun the existing workflow
   or start the `Release` workflow manually from the Actions tab on `main` or
   `master`; if the draft release already exists, the workflow will reuse it.

## Notes

- Automatic publishing only happens when the version changes and the
  corresponding `vX.Y.Z` release has not already been published.
- Release automation only supports plain `X.Y.Z` versions (no suffixes).
- `dist-workspace.toml` does not need a version bump for application releases; it only changes when the dist tool version or release targets change.
