+++
title = "Viewer Basics"
description = "Learn the main viewer layouts, controls, and overlay behavior in Perspecta."
weight = 15
last_updated = "2026-03-20"
+++

Use this page after a study or report is already open. It covers the main viewer layouts, navigation, zoom and pan behavior, cine, and overlays.

## Viewer layouts

- `1` image: single-image review (`1x1`)
- `2` images: side-by-side review (`1x2`)
- `3` images: three-up review (`1x3`)
- `4` images: quad view (`2x2`)
- `8` images: comparison grid (`2x4`)
- GSPS DICOM objects can accompany image files as overlays and do not consume display slots
- Structured Reports open in a dedicated document view instead of an image layout

## Navigation

- `Tab`: move to the next history item
- `Shift+Tab`: move to the previous history item
- `Cmd/Ctrl+W`: close the active study or group; if the window is already empty, close the window
- `Cmd/Ctrl+Shift+W`: close the window

## Image controls

- Hover + mouse wheel: zoom in or out in single-image and supported multi-image layouts
- `Shift` + mouse wheel: move to the previous or next frame in multi-frame images
- `Shift` + drag on monochrome images: adjust window and level
- Click + drag: pan when zoomed in
- Double-click: reset zoom and pan for the active viewport
- `C`: toggle cine mode

## Overlay behavior

- `G`: toggle the active image overlay when a matching GSPS or Mammography CAD SR object is available
- `N`: jump to the next image or frame that contains an overlay
- Overlay visibility is off by default when a study first opens
- Supplementary GSPS and Mammography CAD SR objects augment the active image view and do not count as display slots

## Related Guides

- [Launch Options](/docs/launch-options/)
- [DICOMweb](/docs/dicomweb/)
- [Install Perspecta DICOM Viewer](/docs/install/)
