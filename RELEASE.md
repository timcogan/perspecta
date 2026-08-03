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

## Local Prerelease Builds

For frequent local debug builds, keep `Cargo.toml` on the plain `X.Y.Z` version
and use timestamped prerelease suffixes only in the local build environment:

```sh
make run-local
make build-local
make build-release-local
```

The local targets use a UTC `YYYYMMDDHHMMSS` timestamp by default, producing
display/package versions like `vX.Y.Z-YYYYMMDDHHMMSS`. To reproduce a specific
local build ID, set `LOCAL_BUILD_TIMESTAMP=YYYYMMDDHHMMSS`.

## Notes

- Automatic publishing only happens when the version changes and the
  corresponding `vX.Y.Z` release has not already been published.
- Release automation only supports plain `X.Y.Z` versions (no suffixes).
- `dist-workspace.toml` does not need a version bump for application releases; it only changes when the dist tool version or release targets change.

## Browser Preview and GitHub Pages

The experimental browser preview is published by the independent `Pages`
workflow from `master` only. A push that changes the site, Rust application
code, Cargo metadata, or the web build scripts rebuilds the release WASM module
and the Hugo site. This deployment does **not** create a tag or GitHub Release
and does not require a `Cargo.toml` version bump. The workflow verifies that its
checkout matches the triggering commit, then records the full commit in the
same-origin asset manifest. The demo footer displays the short commit so the
development build cannot be confused with a tagged desktop
release. The viewer title uses the same seven-character commit as SemVer build
metadata, for example `v0.5.3+web.51e863a`. A manual Pages run selected from
another ref fails before building.

For a local production-equivalent Pages build:

```sh
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version "$(awk '/^name = "wasm-bindgen"$/ { found = 1; next } found && /^version = / { gsub(/"/, "", $3); print $3; exit }' Cargo.lock)" --locked
make web-pages
```

The local build requires Hugo, `jq`, `strings`, and either `sha256sum` or
`shasum`. The web package uses the WASM emitted by the matching `wasm-bindgen`
CLI without a separate Binaryen optimization pass, so local and Pages builds
produce equivalent modules. `make web-pages` writes only to `website/static/demo/assets`,
`website/data/demo_assets.json`, and `website/public`; the underlying scripts
reject command-line arguments and symlinked output locations.

The build emits content-hashed JavaScript and WASM files, builds Hugo, and
verifies that the published demo contains no source maps, debug sections,
build-host filesystem paths, Google Analytics or consent bootstrap, enabled
egui persistence, or externally hosted executable resources. Local builds also
record their current commit and append `+dirty` in the footer when
the source tree contains uncommitted changes; their viewer version similarly
ends in `.dirty`. Pull requests run WASM Clippy with the toolchain pinned in
`rust-toolchain.toml`; the Pages workflow uses that same toolchain. The web
build intentionally disables default Cargo features because the native
`openjp2` JPEG 2000 backend does not link on `wasm32-unknown-unknown`; desktop
builds keep JPEG 2000 enabled by default.

The demo CSP includes the SHA-256 hash of the inline file-picker stylesheet
embedded by the exact `rfd` version in `Cargo.lock`. After an `rfd` upgrade,
exercise the browser picker and update that hash if its embedded CSS changed;
do not broaden `style-src` to allow arbitrary inline styles.
