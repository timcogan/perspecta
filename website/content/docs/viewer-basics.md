+++
title = "Viewer Basics"
description = "Learn the main viewer layouts, controls, and overlay behavior in Perspecta."
weight = 15
last_updated = "2026-04-10"
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
- The metadata overlay on the left shows selected fields for the active object; use `View all fields (V)` to open the full field list with expandable sequences

## Navigation

- `Tab`: move to the next history item
- `Shift+Tab`: move to the previous history item
- `Cmd/Ctrl+W`: close the active study or group; if the window is already empty, close the window
- `Cmd/Ctrl+Shift+W`: close the window
- `V`: open or close the full metadata field popup for the active object
- `Esc`: exit live measurement mode; if no measurement is active, close the full metadata field popup

## Image controls

- Hover + mouse wheel: zoom in or out in single-image and supported multi-image layouts
- `Shift` + mouse wheel: move to the previous or next frame in multi-frame images
- `Shift` + drag on monochrome images: adjust window and level
- Click + drag: pan when zoomed in
- Right-click inside the image: start a live distance measurement or reset its anchor point
- Move the mouse: update the measurement endpoint without holding a button
- Left-click: clear the live measurement
- Double-click: reset zoom and pan for the active viewport
- `C`: toggle cine mode

## Overlay behavior

- `G`: toggle the active image overlay when a matching GSPS or Mammography CAD SR object with vector marks is available, or when a matching Parametric Map object is available
- `N`: jump to the next image or frame with a GSPS, Mammography CAD SR, or Parametric Map overlay
- Overlay visibility is off by default when a study first opens
- Supplementary GSPS and Mammography CAD SR objects augment the active image view and do not count as display slots. CAD SR overlays render only when the report provides vector marks, and supported SR findings can add a short text label next to the geometry
- Parametric Map overlays are supplementary, do not count as display slots, and follow the same visibility rules as the other overlay types

## Related Guides

- [Launch Options](/docs/launch-options/)
- [DICOMweb](/docs/dicomweb/)
- [Install Perspecta DICOM Viewer](/docs/install/)
