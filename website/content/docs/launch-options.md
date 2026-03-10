+++
title = "Launch Options"
description = "Open studies via files, groups, and custom URLs."
weight = 20
last_updated = "2026-03-05"
+++

## Local Files

Open one or more DICOM files from the UI menu.

Supported local file counts:

- `1` file: opens single-image view (`1x1`)
- `2` files: opens `1x2`
- `3` files: opens `1x3`
- `4` files: opens `2x2`
- `8` files: opens `2x4`
- GSPS DICOM files can be included alongside image DICOM files in the same selection; GSPS files are used as overlays and do not count as display slots.

## GSPS Overlay

- If a GSPS DICOM references the active image, an overlay is available.
- Overlay visibility is `off` by default.
- Use `G` to toggle GSPS overlay on/off.

## Keyboard Shortcuts

- `C`: toggle cine mode
- `G`: toggle GSPS overlay (when available)
- `Tab`: next history item
- `Shift+Tab`: previous history item
- `Cmd/Ctrl+W`: close the active study/group; if the window is already empty, close the window
- `Cmd/Ctrl+Shift+W`: close the window

## Grouped Local Launch

Use grouped launch arguments when you need multi-view layouts.

## Custom URL Scheme

Perspecta supports `perspecta://` URLs for local and DICOMweb launch modes.

```text
perspecta://open?path=example-data%2Fimage.dcm
perspecta://open?path=example-data%2FRCC.dcm&path=example-data%2FLCC.dcm&path=example-data%2FRMLO.dcm&path=example-data%2FLMLO.dcm
perspecta://open?group=example-data%2FRCC.dcm|example-data%2FLCC.dcm|example-data%2FRMLO.dcm|example-data%2FLMLO.dcm&group=example-data%2Freport.dcm&open_group=0
perspecta://open?group=example-data%2Fcurrent-RCC.dcm|example-data%2Fcurrent-LCC.dcm|example-data%2Fcurrent-RMLO.dcm|example-data%2Fcurrent-LMLO.dcm|example-data%2Fprior-RCC.dcm|example-data%2Fprior-LCC.dcm|example-data%2Fprior-RMLO.dcm|example-data%2Fprior-LMLO.dcm
perspecta://open?dicomweb=http%3A%2F%2Flocalhost%3A8042%2Fdicom-web&study=<StudyInstanceUID>&series=<SeriesInstanceUID>
```

## Launch Parameter Reference

| Parameter | Purpose |
| --- | --- |
| `path`, `file` | Add one local file path |
| `paths`, `files` | Add multiple local file paths (comma- or pipe-separated) |
| `group` | Add one local preload group (must contain `1`, `2`, `3`, `4`, or `8` paths) |
| `groups` | Add multiple local preload groups separated by `;` |
| `open_group` | Select which preloaded group opens first (default `0`) |
| `dicomweb` | DICOMweb base URL (or full URL containing study/series/instance path segments) |
| `study` | StudyInstanceUID (required for DICOMweb launch) |
| `series` | SeriesInstanceUID (optional) |
| `instance` | SOPInstanceUID (optional) |
| `group_series` | DICOMweb grouped preload by series UID lists (each group must contain `1`, `2`, `3`, `4`, or `8`) |
| `user`, `password` | Optional HTTP basic auth credentials for local/testing only (must be provided together); avoid in shared or production launch URLs |
| `auth` | Alternative local/testing-only auth format: `username:password` (percent-encoded); avoid in shared or production launch URLs |

## Notes

- URL values should be percent-encoded.
- Do not embed credentials or tokens in URLs outside local testing; URLs are commonly logged and persisted.
- If `dicomweb` is provided as a server root (for example `http://localhost:8042`), Perspecta normalizes it to `/dicom-web`.
- You cannot mix local grouped launch (`group=...`) with DICOMweb launch in the same URI.
