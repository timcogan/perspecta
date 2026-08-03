const loader = document.querySelector("script[data-perspecta-loader]");
const status = document.getElementById("demo-status");
const statusMessage = status?.querySelector(".demo-status-message");
const notice = document.getElementById("demo-notice");
const noticeHeading = notice?.querySelector("h2");
const noticeMessage = notice?.querySelector(".demo-notice-message");
const noticeDismiss = notice?.querySelector(".demo-notice-dismiss");
const maxFileBytes = 512 * 1024 * 1024;
const maxSelectionBytes = 1024 * 1024 * 1024;
let noticeTimer;
let viewerReady = false;
let queueDroppedFiles;
let dropReadInProgress = false;

function trustedModuleUrl(candidate) {
  if (typeof candidate !== "string") {
    return null;
  }
  try {
    const resolved = new URL(candidate, window.location.href);
    if (
      resolved.origin !== window.location.origin ||
      !/^\/demo\/assets\/perspecta_web-[0-9a-f]{16}\.js$/.test(resolved.pathname)
    ) {
      return null;
    }
    return resolved.href;
  } catch (_error) {
    return null;
  }
}

async function resolveModuleUrl() {
  const manifestCandidate = loader?.dataset.manifestUrl ?? "/demo/assets/manifest.json";
  try {
    const manifestUrl = new URL(manifestCandidate, window.location.href);
    if (manifestUrl.origin !== window.location.origin) {
      throw new Error("Cross-origin manifests are not allowed");
    }
    const response = await fetch(manifestUrl, {
      cache: "no-store",
      credentials: "same-origin",
    });
    if (!response.ok) {
      throw new Error("Asset manifest is unavailable");
    }
    const manifest = await response.json();
    const currentModuleUrl = trustedModuleUrl(manifest?.js);
    if (currentModuleUrl) {
      return currentModuleUrl;
    }
  } catch (_error) {
    // The build-time URL remains a safe fallback for offline/static mirrors.
  }
  return trustedModuleUrl(loader?.dataset.moduleUrl);
}

function hideNotice() {
  if (!notice) {
    return;
  }

  notice.hidden = true;
  if (noticeTimer !== undefined) {
    window.clearTimeout(noticeTimer);
    noticeTimer = undefined;
  }
}

function showNotice(message, headingText = "DICOM files were not opened") {
  if (!notice || !noticeHeading || !noticeMessage) {
    return;
  }

  noticeHeading.textContent = headingText;
  noticeMessage.textContent = message;
  notice.hidden = false;
  if (noticeTimer !== undefined) {
    window.clearTimeout(noticeTimer);
  }
  noticeTimer = window.setTimeout(hideNotice, 8000);
}

function showError(message, headingText = "Perspecta could not start") {
  if (!status || !statusMessage) {
    return;
  }

  hideNotice();
  status.dataset.state = "error";
  status.setAttribute("aria-hidden", "false");
  const heading = status.querySelector("h2");
  if (heading) {
    heading.textContent = headingText;
  }
  statusMessage.textContent = message;
}

function clearViewerDropHover() {
  const canvasId = loader?.dataset.canvasId;
  const canvas = canvasId ? document.getElementById(canvasId) : null;
  if (canvas && typeof DragEvent === "function") {
    canvas.dispatchEvent(
      new DragEvent("dragleave", { bubbles: true, cancelable: true }),
    );
  }
}

noticeDismiss?.addEventListener("click", hideNotice);

async function submitDroppedFiles(files) {
  dropReadInProgress = true;
  hideNotice();
  try {
    await queueDroppedFiles(files);
  } catch (_error) {
    showNotice(
      "The browser could not read that dropped selection. Try the file picker or open each DICOM individually to identify an unsupported file.",
    );
  } finally {
    dropReadInProgress = false;
  }
}

window.addEventListener(
  "dragover",
  (event) => {
    // Keep an early file or URL drag from navigating away while WASM starts.
    event.preventDefault();
  },
  true,
);

window.addEventListener(
  "drop",
  (event) => {
    // Read the complete selection here so large files cannot reach eframe in
    // separate repaint frames and lose their 2x2/2x4 grouping.
    event.preventDefault();
    event.stopImmediatePropagation();
    // The capture handler bypasses eframe's own drop listener. Send its canvas
    // the matching leave event so the native drop preview is cleared.
    clearViewerDropHover();
    const files = Array.from(event.dataTransfer?.files ?? []);
    if (files.length === 0) {
      showNotice("Drop local DICOM files, not a URL or content from another page.");
      return;
    }

    if (
      dropReadInProgress ||
      document.documentElement.dataset.perspectaLoading === "true"
    ) {
      showNotice(
        "Perspecta is still opening the previous selection. Wait for it to finish, then try again.",
      );
      return;
    }

    const selectionBytes = files.reduce((total, file) => total + file.size, 0);
    const retainedBytes = Number.parseInt(
      document.documentElement.dataset.perspectaSessionBytes ?? "0",
      10,
    );
    const sessionBytes = Number.isFinite(retainedBytes) ? retainedBytes : 0;
    if (
      files.some((file) => file.size > maxFileBytes) ||
      selectionBytes > maxSelectionBytes ||
      sessionBytes + selectionBytes > maxSelectionBytes
    ) {
      showNotice(
        "That drop exceeds the browser safety limits (512 MiB per file and 1 GiB retained in this session). No file was read.",
      );
      return;
    }

    if (!viewerReady) {
      showNotice("Perspecta is still starting. Wait for the viewer, then drop the files again.");
      return;
    }

    if (typeof queueDroppedFiles !== "function") {
      showNotice("The browser file-drop bridge is unavailable. Reload the page and try again.");
      return;
    }

    void submitDroppedFiles(files);
  },
  true,
);

async function start() {
  if (!loader || !status) {
    return;
  }

  if (window.top && window.top !== window.self) {
    showError(
      "For privacy and clickjacking protection, open this page directly in a top-level browser tab.",
      "Open Perspecta directly",
    );
    return;
  }

  const canvasId = loader.dataset.canvasId;
  if (!canvasId) {
    showError(
      "The browser preview assets are unavailable. Build the web assets or use the desktop release.",
    );
    return;
  }

  try {
    const moduleUrl = await resolveModuleUrl();
    if (!moduleUrl) {
      showError(
        "The browser preview assets are unavailable. Reload the page or use the desktop release.",
      );
      return;
    }
    const module = await import(moduleUrl);
    if (
      typeof module.default !== "function" ||
      typeof module.start_perspecta !== "function" ||
      typeof module.queue_dropped_files !== "function"
    ) {
      showError(
        "The browser preview module is incomplete. Reload the page or use the desktop release.",
      );
      return;
    }

    await module.default();
    await module.start_perspecta(canvasId);
    queueDroppedFiles = module.queue_dropped_files;
    viewerReady = true;
    status.dataset.state = "ready";
    status.setAttribute("aria-hidden", "true");
  } catch (_error) {
    showError(
      "Reload the page in a current desktop browser. If the problem continues, use the desktop release.",
    );
  }
}

void start();
