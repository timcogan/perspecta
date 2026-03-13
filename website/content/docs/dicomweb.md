+++
title = "DICOMweb"
description = "Connect to DICOMweb studies and series."
weight = 30
last_updated = "2026-03-04"
+++

## Supported Flow

- Single-image open from study and series metadata
- Grouped open for supported multi-view counts
- Streaming active-group updates when available

## URL Examples

```text
perspecta://open?dicomweb=http%3A%2F%2Flocalhost%3A8042%2Fdicom-web&study=<StudyInstanceUID>
perspecta://open?dicomweb=http%3A%2F%2Flocalhost%3A8042%2Fdicom-web&study=<StudyInstanceUID>&series=<SeriesInstanceUID>
perspecta://open?dicomweb=http%3A%2F%2Flocalhost%3A8042&study=<StudyInstanceUID>&user=<username>&password=<password>
```

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

## Notes

- Provide both `user` and `password` together when using basic auth.
- Grouped DICOMweb preload uses `group_series`; each group must resolve to `1`, `2`, `3`, `4`, or `8` displayable items, while supplementary GSPS/SR objects do not count toward that total.
