<h1>
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="assets/perspecta-light.svg" />
    <img src="assets/perspecta.svg" alt="Perspecta logo" width="20" valign="middle" />
  </picture>
  Perspecta — Medical Image Viewer
</h1>
<p align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="assets/perspecta-wordmark-light.svg" />
    <img src="assets/perspecta-wordmark.svg" alt="Perspecta" width="512" />
  </picture>
</p>

Perspecta Viewer is a native desktop DICOM viewer written in Rust (`egui`/`eframe`), focused on fast loading, responsive interaction, and simple launch integration from external systems.

## Highlights

- Open local DICOM files (`.dcm`) in single-image mode.
- Open 4-image mammography layouts (`2x2`) with consistent viewport ordering.
- Decode DICOM `PixelData` through `dicom-pixeldata` (including encapsulated data).
- JPEG 2000 support via `openjp2`.
- Real-time window/level controls for grayscale workflows.
- Multi-frame cine playback (`C` key or UI control).
- Metadata side panel for quick inspection.
- Launch through a custom URL scheme (`perspecta://...`).
- Launch directly from DICOMweb (study/series/instance aware).

## Project Status

This project is currently an MVP and actively evolving.

## Getting Started

### Prerequisites

- Rust toolchain (stable)
- Platform graphics support compatible with `eframe` (OpenGL stack available)
- Optional on Linux: `xdg-utils` for URL scheme registration

### Run

```bash
cargo run --release
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
cargo run -- "example-data/RCC.dcm" "example-data/LCC.dcm" "example-data/RMLO.dcm" "example-data/LMLO.dcm"
```

- `1` file: opens the standard single-image view.
- `4` files: opens the mammography `2x2` layout.

### 2. Custom URL Scheme (`perspecta://`)

```text
perspecta://open?path=example-data%2Fimage.dcm
perspecta://open?path=example-data%2FRCC.dcm&path=example-data%2FLCC.dcm&path=example-data%2FRMLO.dcm&path=example-data%2FLMLO.dcm
perspecta://open?group=example-data%2FRCC.dcm|example-data%2FLCC.dcm|example-data%2FRMLO.dcm|example-data%2FLMLO.dcm&group=example-data%2Freport.dcm&open_group=0
perspecta://open?dicomweb=http%3A%2F%2Flocalhost%3A8042%2Fdicom-web&study=<StudyInstanceUID>&series=<SeriesInstanceUID>
perspecta://open?dicomweb=http%3A%2F%2Flocalhost%3A8042&study=<StudyInstanceUID>&user=<username>&password=<password>
```

### 3. Launch Parameter Reference

| Parameter | Purpose |
| --- | --- |
| `path`, `file` | Add one local file path |
| `paths`, `files` | Add multiple local file paths (comma- or pipe-separated) |
| `group` | Add one local preload group (must contain `1` or `4` paths) |
| `groups` | Add multiple local preload groups separated by `;` |
| `open_group` | Select which preloaded group opens first (default `0`) |
| `dicomweb` | DICOMweb base URL (or full URL containing study/series/instance path segments) |
| `study` | StudyInstanceUID (required for DICOMweb launch) |
| `series` | SeriesInstanceUID (optional) |
| `instance` | SOPInstanceUID (optional) |
| `group_series` | DICOMweb grouped preload by series UID lists (each group must contain `1` or `4`) |
| `user`, `password` | Optional HTTP basic auth credentials (must be provided together) |
| `auth` | Alternative auth format: `username:password` (percent-encoded) |

Notes:

- URL values should be percent-encoded.
- If `dicomweb` is provided as a server root (for example `http://localhost:8042`), Perspecta normalizes it to `/dicom-web`.
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
- `Tab`: next history item
- `Shift+Tab`: previous history item
- `Cmd/Ctrl+W`: close window

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
- `scripts/register-protocol-linux.sh`: Linux URL scheme registration helper

## Current Limitations

- Some compressed transfer syntaxes may still depend on codec availability at runtime.
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
