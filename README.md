<h1>
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="assets/perspecta-light.svg" />
    <img src="assets/perspecta.svg" alt="Perspecta logo" width="20" valign="middle" />
  </picture>
  Perspecta DICOM Viewer
</h1>
<p align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="assets/perspecta-wordmark-light.svg" />
    <img src="assets/perspecta-wordmark.svg" alt="Perspecta" width="512" />
  </picture>
</p>
<p align="center">
  <a href="https://github.com/timcogan/perspecta/actions/workflows/ci.yml"><img alt="Tests" src="https://img.shields.io/github/actions/workflow/status/timcogan/perspecta/ci.yml?label=tests&style=for-the-badge" /></a>
  <a href="https://www.rust-lang.org/"><img alt="MSRV 1.73" src="https://img.shields.io/badge/MSRV-1.73-f39c12?logo=rust&logoColor=white&style=for-the-badge" /></a>
  <a href="LICENSE"><img alt="License" src="https://img.shields.io/github/license/timcogan/perspecta?color=0ea5e9&style=for-the-badge" /></a>
  <a href="https://github.com/timcogan/perspecta/releases"><img alt="Release" src="https://img.shields.io/github/v/release/timcogan/perspecta?display_name=tag&color=0ea5e9&style=for-the-badge" /></a>
  <a href="https://perspecta.cogan.dev/"><img alt="Website" src="https://img.shields.io/badge/Website-perspecta.cogan.dev-0ea5e9?style=for-the-badge" /></a>
</p>

Perspecta DICOM Viewer is an open-source Rust desktop DICOM viewer (`egui`/`eframe`) focused on fast loading, responsive interaction, DICOMweb launch, mammography layouts, SR/GSPS overlays, and simple integration from external systems.

## Highlights

- Open local DICOM files (`.dcm`) in single-image mode.
- Open grouped mammography layouts from 2 up to 8 images (`1x2`, `1x3`, `2x2`, `2x4`) with consistent viewport ordering.
- Decode DICOM `PixelData` through `dicom-pixeldata` (including encapsulated data).
- JPEG 2000 support by default via `openjp2`; optional JPEG-LS support via the `jpeg_ls` feature and `charls`.
- Real-time window/level controls for grayscale workflows.
- Multi-frame cine playback (`C` key or UI control).
- GSPS (Grayscale Softcopy Presentation State) overlay support with manual toggle (`G` key, off by default).
- Mammography CAD SR overlay support on matching images, with the same manual overlay workflow as GSPS.
- Structured Report (SR) DICOM support with a dedicated text/document view.
- Mouse-wheel zoom + drag pan in single-image and multi-view (`1x2` / `1x3` / `2x2` / `2x4`) mammo views.
- Typical DICOM mouse conventions (single modifier): `Shift + wheel` for frame navigation and `Shift + drag` for window/level in multi-view layouts.
- Metadata side panel for quick inspection.
- Launch through a custom URL scheme (`perspecta://...`).
- Launch directly from DICOMweb (study/series/instance aware).

## Getting Started

### Prerequisites

- Rust toolchain (stable)
- Platform graphics support compatible with `eframe` (OpenGL stack available)
- Optional on Linux: `xdg-utils` for URL scheme registration

### Run

```bash
cargo run --release
```

Enable JPEG-LS decoding explicitly when you need it:

```bash
cargo run --release --features jpeg_ls
```

You can also use Make targets:

```bash
make run
make run-release
```

## Opening Studies

### 1. Local Files (CLI)

```bash
cargo run -- "example-data/image.dcm"
cargo run -- "example-data/RCC.dcm" "example-data/LCC.dcm"
cargo run -- "example-data/RCC.dcm" "example-data/RMLO.dcm" "example-data/LMLO.dcm"
cargo run -- "example-data/RCC.dcm" "example-data/LCC.dcm" "example-data/RMLO.dcm" "example-data/LMLO.dcm"
cargo run -- "example-data/current-RCC.dcm" "example-data/current-LCC.dcm" "example-data/current-RMLO.dcm" "example-data/current-LMLO.dcm" "example-data/prior-RCC.dcm" "example-data/prior-LCC.dcm" "example-data/prior-RMLO.dcm" "example-data/prior-LMLO.dcm"
```

- `1` file: opens the standard single-image view.
- `2` files: opens the mammography `1x2` layout.
- `3` files: opens the mammography `1x3` layout.
- `4` files: opens the mammography `2x2` layout.
- `8` files: opens the mammography comparison `2x4` layout (current row + prior row).
- GSPS DICOM files can be included in the same selection, including grouped launch inputs; they act as overlays and do not count as display slots.
- Structured Report (SR) DICOM files can be opened directly in a single-document view.
- If images and SR objects are selected together, Perspecta opens the images first and adds each SR as a separate history entry.

### 2. Custom URL Scheme (`perspecta://`)

```text
perspecta://open?path=example-data%2Fimage.dcm
perspecta://open?path=example-data%2FRCC.dcm&path=example-data%2FLCC.dcm&path=example-data%2FRMLO.dcm&path=example-data%2FLMLO.dcm
perspecta://open?group=example-data%2FRCC.dcm|example-data%2FLCC.dcm|example-data%2FRMLO.dcm|example-data%2FLMLO.dcm&group=example-data%2Freport.dcm&open_group=0
perspecta://open?group=example-data%2Fcurrent-RCC.dcm|example-data%2Fcurrent-LCC.dcm|example-data%2Fcurrent-RMLO.dcm|example-data%2Fcurrent-LMLO.dcm|example-data%2Fprior-RCC.dcm|example-data%2Fprior-LCC.dcm|example-data%2Fprior-RMLO.dcm|example-data%2Fprior-LMLO.dcm
perspecta://open?dicomweb=http%3A%2F%2Flocalhost%3A8042%2Fdicom-web&study=<StudyInstanceUID>&series=<SeriesInstanceUID>
perspecta://open?dicomweb=http%3A%2F%2Flocalhost%3A8042&study=<StudyInstanceUID>&user=<username>&password=<password>
```

### 3. Launch Parameter Reference

| Parameter | Purpose |
| --- | --- |
| `path`, `file` | Add one local file path |
| `paths`, `files` | Add multiple local file paths (comma- or pipe-separated) |
| `group` | Add one local preload group; after filtering supplementary GSPS/SR objects, each group must resolve to `1`, `2`, `3`, `4`, or `8` displayable items |
| `groups` | Add multiple local preload groups separated by `;` |
| `open_group` | Select which preloaded group opens first (default `0`) |
| `dicomweb` | DICOMweb base URL (or full URL containing study/series/instance path segments) |
| `study` | StudyInstanceUID (required for DICOMweb launch) |
| `series` | SeriesInstanceUID (optional) |
| `instance` | SOPInstanceUID (optional) |
| `group_series` | DICOMweb grouped preload by series UID lists; each group must resolve to `1`, `2`, `3`, `4`, or `8` displayable items, while supplementary GSPS/SR objects do not count toward that total |
| `user`, `password` | Optional HTTP basic auth credentials (must be provided together) |
| `auth` | Alternative auth format: `username:password` (percent-encoded) |

Notes:

- URL values should be percent-encoded.
- If `dicomweb` is provided as a server root (for example `http://localhost:8042`), Perspecta normalizes it to `/dicom-web`.
- Grouped mammography launch supports up to `8` images (`2x4` comparison layout).
- You cannot mix local grouped launch (`group=...`) with DICOMweb launch in the same URI.

## Web Integration Example

```js
const dicomwebBase = "http://localhost:8042/dicom-web";
const studyUID = "<StudyInstanceUID>";
const seriesUID = "<SeriesInstanceUID>";

const uri = `perspecta://open?dicomweb=${encodeURIComponent(
  dicomwebBase
)}&study=${encodeURIComponent(studyUID)}&series=${encodeURIComponent(seriesUID)}`;

window.location.href = uri;
```

## Linux: Register `perspecta://` Handler

```bash
cargo build --release
make install-protocol-linux
```

This writes a desktop entry under `~/.local/share/applications`.

## Keyboard Shortcuts

- `C`: toggle cine mode
- `G`: toggle image overlay (GSPS or Mammography CAD SR, when available)
- `N`: jump to the next image/frame with an overlay
- `Tab`: next history item
- `Shift+Tab`: previous history item
- `Cmd/Ctrl+W`: close the active study/group; if the window is already empty, close the window
- `Cmd/Ctrl+Shift+W`: close the window

## Mouse Controls

- Hover + mouse wheel: zoom in/out (single-image and `1x2` / `1x3` / `2x2` / `2x4` mammo viewports)
- `Shift` + mouse wheel: previous/next frame (multi-frame images)
- `Shift` + drag (monochrome images): adjust window/level
- Click + drag: pan when zoomed in
- Double click: reset zoom/pan for the active viewport

## Development

Common commands:

```bash
make check
make test
make fmt-check
make clippy
```

Optional live-reload workflow:

```bash
make install-watch
make dev
```

## Project Layout

- `src/main.rs`: app entry point and native window setup
- `src/app.rs`: UI, state management, interactions, history/cine workflow
- `src/dicom.rs`: DICOM parsing and pixel extraction
- `src/dicomweb.rs`: DICOMweb metadata/download bridge
- `src/renderer.rs`: grayscale and RGB rendering paths
- `src/launch.rs`: CLI + `perspecta://` parser
- `tools/benchmark`: end-to-end benchmark tools and synthetic DICOM helpers
- `scripts/register-protocol-linux.sh`: Linux URL scheme registration helper

## Current Limitations

- Some compressed transfer syntaxes still depend on codec availability at build time.
- No full study/series stack browser yet.
- No MPR, measurement tools, or advanced annotation workflow yet.

## Roadmap

1. Broader transfer syntax and codec coverage.
2. Stronger study/series navigation and indexing.
3. Background decode + smarter cache strategy for large studies.
4. Expanded clinical tools (VOI LUT workflows, overlays, measurements).
5. More reader productivity controls and presets.

## License

MIT License. See `LICENSE`.
