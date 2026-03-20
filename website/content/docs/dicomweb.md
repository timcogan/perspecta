+++
title = "DICOMweb"
description = "Launch Perspecta from DICOMweb study, series, and grouped review context."
weight = 30
last_updated = "2026-03-20"
+++

This page covers DICOMweb launch into Perspecta from a web app, worklist, or imaging system. Use it when you want to open the desktop viewer with study, series, or grouped review context already selected.

## Supported Flow

- Study-level open from DICOMweb metadata
- Series-level open when you want to target a specific series
- Grouped open for supported multi-view review counts
- Streaming active-group updates when available
- Optional HTTP basic auth for local setups or controlled environments

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

## Related Guides

- [Launch Options](/docs/launch-options/)
- [Install Perspecta DICOM Viewer](/docs/install/)
