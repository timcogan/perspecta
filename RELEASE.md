# Release Process

This project uses Semantic Versioning and publishes binaries via GitHub Actions.
The release workflow triggers only on tags that match `vX.Y.Z` (no suffixes).

## Versioning Rules (SemVer)

- **MAJOR** (`X.0.0`): Breaking changes or incompatible behavior.
- **MINOR** (`0.X.0`): Backwards-compatible features.
- **PATCH** (`0.0.X`): Backwards-compatible bug fixes.

## Steps to Publish a Release

1. Update the version in `Cargo.toml`:
   - `version = "X.Y.Z"`
2. Commit the release change:
   - Example message: `chore(release): vX.Y.Z`
3. Create an annotated tag:
   - `git tag -a vX.Y.Z -m "vX.Y.Z"`
4. Push the commit and tag:
   - `git push origin master --tags`

## Notes

- The CI release workflow only triggers on tags that match `vX.Y.Z` exactly.
- `dist-workspace.toml` does not need a version bump for application releases; it only changes when the dist tool version or release targets change.
