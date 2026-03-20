+++
title = "Open Source DICOM Viewer"
description = "Perspecta is an open-source desktop DICOM viewer in Rust for local files, DICOMweb launch, mammography layouts, GSPS overlays, and Structured Reports."
last_updated = "2026-03-20"
+++

Perspecta is an open-source desktop DICOM viewer written in Rust for developers and individual users. It opens local DICOM files, accepts `perspecta://` and DICOMweb launch handoff, supports grouped mammography layouts, shows GSPS and Mammography CAD SR overlays, and opens Structured Reports in a dedicated document view.

## Who Perspecta is for

Perspecta fits developers and individual users who need a lightweight desktop viewer and, in some cases, a way to connect that viewer to an existing web application or imaging workflow.

It is especially useful when you need an open-source DICOM viewer that can be inspected and built locally, launched from another system, and used for practical review workflows such as mammography comparison and overlay-aware image review.

## What Perspecta supports

- Local DICOM file open for single-image review
- Grouped mammography layouts in `1x2`, `1x3`, `2x2`, and `2x4`
- `perspecta://` launch integration from external systems
- DICOMweb launch using study, series, and instance context
- GSPS overlay toggle on matching images
- Mammography CAD SR overlay rendering on matching images
- Structured Report viewing in a dedicated document mode

## What Perspecta does not try to be

Perspecta is not positioned as a full PACS replacement, a browser-first imaging framework, or a broad study-management system. It focuses on fast desktop review and integration into an existing workflow.

## Common questions

### Is Perspecta a web viewer or a desktop viewer?

Perspecta is a desktop DICOM viewer. It can be launched from a web application or another system, but the viewer itself runs as a native desktop app.

### Does Perspecta support DICOMweb?

Yes. Perspecta supports DICOMweb launch with study, series, and instance context, plus grouped preload flows for supported review layouts. See [DICOMweb Integration](/docs/dicomweb/).

### Does Perspecta support mammography review?

Yes. Perspecta supports grouped mammography layouts in `1x2`, `1x3`, `2x2`, and `2x4`, which makes it suitable for current and prior comparison workflows.

### Does Perspecta support GSPS and CAD overlays?

Yes. Perspecta can show GSPS overlays and Mammography CAD SR overlays when they reference the active image. See [Viewer Basics](/docs/viewer-basics/) and [Launch Options](/docs/launch-options/).

### Does Perspecta support Structured Reports?

Yes. Structured Reports open in a dedicated document mode instead of being treated like image viewports.

### Can I inspect and build Perspecta myself?

Yes. Perspecta is open source and the repository includes local build instructions, packaged releases, and platform-specific install guides.

## Learn more

- [Install Perspecta DICOM Viewer](/docs/install/)
- [Viewer Basics](/docs/viewer-basics/)
- [Launch Options](/docs/launch-options/)
- [DICOMweb Integration](/docs/dicomweb/)
- [Source Code on GitHub](https://github.com/timcogan/perspecta)
- [Releases](https://github.com/timcogan/perspecta/releases)
