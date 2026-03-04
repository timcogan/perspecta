+++
title = "Launch Options"
description = "Open studies via files, groups, and custom URLs."
weight = 20
last_updated = "2026-03-04"
+++

## Local Files

Open one or more DICOM files from the UI menu.

Supported local file counts:

- `1` file: opens single-image view (`1x1`)
- `2` files: opens `1x2`
- `3` files: opens `1x3`
- `4` files: opens `2x2`
- `8` files: opens `2x4`

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
perspecta://open?dicomweb=http%3A%2F%2Flocalhost%3A8042&study=<StudyInstanceUID>&user=<username>&password=<password>
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
| `user`, `password` | Optional HTTP basic auth credentials (must be provided together) |
| `auth` | Alternative auth format: `username:password` (percent-encoded) |

## Notes

- URL values should be percent-encoded.
- If `dicomweb` is provided as a server root (for example `http://localhost:8042`), Perspecta normalizes it to `/dicom-web`.
- You cannot mix local grouped launch (`group=...`) with DICOMweb launch in the same URI.
