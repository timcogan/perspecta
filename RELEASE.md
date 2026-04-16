# Release Process

This project uses Semantic Versioning and publishes binaries via GitHub Actions.
The release workflow watches for a version bump in `Cargo.toml` on `main` or
`master`, then creates the `vX.Y.Z` tag and GitHub Release automatically.
If a release needs to be retried, the workflow can also be started manually with
GitHub Actions `workflow_dispatch`.

## Versioning Rules (SemVer)

- **MAJOR** (`X.0.0`): Breaking changes or incompatible behavior.
- **MINOR** (`0.X.0`): Backwards-compatible features.
- **PATCH** (`0.0.X`): Backwards-compatible bug fixes.

## Steps to Publish a Release

1. Update the version in `Cargo.toml`:
   - `version = "X.Y.Z"`
2. Commit the release change:
   - Example message: `chore(release): vX.Y.Z`
3. Push the commit to `main` or `master`.
4. GitHub Actions will:
   - detect that the package version changed
   - reserve `vX.Y.Z` for cargo-dist
   - create the GitHub Release and corresponding tag automatically
5. If a release fails after the version bump landed, rerun the existing workflow
   or start the `Release` workflow manually from the Actions tab.

## Notes

- Automatic publishing only happens when the version changes and the
  corresponding `vX.Y.Z` release has not already been published.
- Release automation only supports plain `X.Y.Z` versions (no suffixes).
- `dist-workspace.toml` does not need a version bump for application releases; it only changes when the dist tool version or release targets change.
