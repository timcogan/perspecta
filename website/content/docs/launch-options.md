+++
title = "Launch Options"
description = "Open local files, grouped studies, reports, and custom launch URLs in Perspecta."
weight = 20
last_updated = "2026-10-08"
+++

This page covers how Perspecta opens local files, grouped review sets, reports, and `perspecta://` URLs from external systems.

For keyboard, mouse, layout, and overlay behavior after content opens, see [Viewer Basics](/docs/viewer-basics/).

## CLI Inspection

Run `perspecta --version` to print `perspecta <version>` and exit without a graphical display or log output.
The version includes any local build suffix, as in the viewer.

Use `inspect` to read a technical summary of one local DICOM file:

```sh
perspecta inspect example-data/image.dcm
perspecta inspect --help
```

The command runs without a graphical display. The default file size limit is **4096 MiB (4 GiB)**.
It rejects files above the selected limit before it calls the reader.
For files within this limit, it reads the complete file into memory through the viewer's reader, including its existing repairs.
It does not modify the file or decode pixels. Successful inspection does not confirm that the pixels can be decoded or validate DICOM conformance.

An [NHS evaluation (Table 3)](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/998318/Practical_evaluation_3Dimensions_tomo.pdf#page=47) reports uncompressed HD tomosynthesis files up to 2835.4 MB.
The default leaves room above that example. Larger objects require an explicit limit.

Use `--max-file-mib` before the file name to set a different limit, such as 8 GiB:

```sh
perspecta inspect --max-file-mib 8192 example-data/image.dcm
```

The value must be a positive whole number of MiB (1 MiB = 1048576 bytes).
Zero, fractions, duplicate options, and values that overflow the byte count produce `invalid_arguments`.
The limit applies to the file size. Parsing and repairs can require additional memory beyond that size.

### JSON Output

Success writes one JSON object and a newline to standard output. Standard error is empty, even when `RUST_LOG` is set.

| Field | JSON type | Meaning |
| --- | --- | --- |
| `schema_version` | integer | Always `1` for this contract |
| `kind` | string | `image`, `structured_report`, `gsps`, `parametric_map`, or `other` |
| `modality` | string or null | Root dataset `Modality` |
| `sop_class_uid` | string or null | Root dataset `SOPClassUID` |
| `transfer_syntax_uid` | string or null | Transfer syntax from file meta information |
| `rows` | integer or null | Root dataset `Rows`, from 1 through 65535 |
| `columns` | integer or null | Root dataset `Columns`, from 1 through 65535 |
| `number_of_frames` | integer or null | Root dataset `NumberOfFrames`, from 1 through 4294967295 |

Absent fields are `null`. Empty text fields are also `null`.
Image and Parametric Map objects can have dimensions and frame counts. These fields are `null` for all other kinds.

An absent `NumberOfFrames` remains `null`, without an assumed value of `1`.
For images and Parametric Maps, present numeric fields must contain one positive integer within the stated range.
Empty, negative, fractional, multiple, or oversized values produce `invalid_metadata`.

```json
{"schema_version":1,"kind":"image","modality":"MG","sop_class_uid":"1.2.840.10008.5.1.4.1.1.1.2","transfer_syntax_uid":"1.2.840.10008.1.2.1","rows":1024,"columns":1024,"number_of_frames":1}
```

The summary includes only the fields in this table. It excludes patient fields, study/series/instance identifiers, file paths, and free-text descriptions.
The command reports selected metadata. It does not anonymize the source file.

### Errors and Exit Codes

File, data, and argument errors leave standard output empty. Standard error contains one JSON error object and a newline:

```json
{"schema_version":1,"error":{"code":"invalid_metadata","message":"Invalid NumberOfFrames value in the DICOM dataset."}}
```

| Exit code | Error code | Meaning |
| --- | --- | --- |
| `0` | None | Success or help |
| `1` | `read_error` | The input is not a readable regular DICOM file, or its encoding cannot be read |
| `1` | `file_too_large` | The input exceeds the selected file size limit |
| `1` | `invalid_metadata` | A selected field has an invalid value |
| `1` | `output_error` | The command could not write or flush its output |
| `2` | `invalid_arguments` | Invalid syntax or file size limit, or the command needs exactly one local file |

Use `error.code` for programmatic decisions. Error messages exclude input paths and raw DICOM values.
For `file_too_large`, the message gives the file size in bytes, the active limit, and the minimum required `--max-file-mib` value.
This required value rounds up to the next whole MiB when necessary. The message also explains that memory use can exceed the file size.
An output error can leave a partial result. Discard output when the exit code is not `0`.
If standard error is unavailable, the command still returns a failure code.

### File Names and Viewer Launch

Use `--` before a file name that starts with a hyphen:

```sh
perspecta inspect -- -example.dcm
```

`perspecta inspect --help`, `perspecta inspect -h`, and `perspecta --help` print usage text and exit with `0`.
To open a file named `inspect` in the viewer, use `perspecta --open inspect` or `perspecta ./inspect`.
Existing file, grouped, and `perspecta://` launches continue to open the viewer.

## Local Files

Open one or more DICOM files from the UI menu.

The UI file picker accepts `.dcm` and `.dicom` suffixes case-insensitively, plus extensionless Part 10 files with a `DICM` prefix.

Supported local file counts:

- `1` file: opens single-image view (`1x1`)
- `2` files: opens `1x2`
- `3` files: opens `1x3`
- `4` files: opens `2x2`
- `8` files: opens `2x4`
- GSPS DICOM files can be included alongside image DICOM files in the same selection; GSPS files are used as overlays and do not count as display slots.

## Grouped Local Launch

Use grouped launch arguments when you need to preload multiple review sets and choose which group opens first. This is useful for current/prior mammography comparisons and image-plus-report launch bundles.

## Custom URL Scheme

Perspecta supports `perspecta://` URLs for both local launch and DICOMweb handoff.

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
| `group` | Add one local preload group; after filtering supplementary GSPS/SR objects, each group must resolve to `1`, `2`, `3`, `4`, or `8` displayable items |
| `groups` | Add multiple local preload groups separated by `;` |
| `open_group` | Select which preloaded group opens first (default `0`) |
| `dicomweb` | DICOMweb base URL (or full URL containing study/series/instance path segments) |
| `study` | StudyInstanceUID (required for DICOMweb launch) |
| `series` | SeriesInstanceUID (optional) |
| `instance` | SOPInstanceUID (optional) |
| `group_series` | DICOMweb grouped preload by series UID lists; each group must resolve to `1`, `2`, `3`, `4`, or `8` displayable items, while supplementary GSPS/SR objects do not count toward that total |
| `user`, `password` | Optional HTTP basic auth credentials for local/testing only (must be provided together); avoid in shared or production launch URLs |
| `auth` | Alternative local/testing-only auth format: `username:password` (percent-encoded); avoid in shared or production launch URLs |

## Notes

- URL values should be percent-encoded.
- Do not embed credentials or tokens in URLs outside local testing; URLs are commonly logged and persisted.
- If `dicomweb` is provided as a server root (for example `http://localhost:8042`), Perspecta normalizes it to `/dicom-web`.
- You cannot mix local grouped launch (`group=...`) with DICOMweb launch in the same URI.

## Related Guides

- [Viewer Basics](/docs/viewer-basics/)
- [DICOMweb](/docs/dicomweb/)
- [Install Perspecta DICOM Viewer](/docs/install/)
