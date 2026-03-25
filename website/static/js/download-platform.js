const lowercase = (value) => (typeof value === "string" ? value.toLowerCase() : "");

const detectBasePlatform = () => {
  const signals = [
    lowercase(navigator.userAgentData && navigator.userAgentData.platform),
    lowercase(navigator.platform),
    lowercase(navigator.userAgent),
  ].join(" ");

  if (/windows|win32|win64/.test(signals)) {
    return "windows";
  }

  if ((/ubuntu|linux|x11/.test(signals) && !/android/.test(signals)) || /cros/.test(signals)) {
    return "linux";
  }

  if (/macintosh|mac os x|mac|darwin/.test(signals) && !/iphone|ipad|ipod/.test(signals)) {
    return "macos";
  }

  return "default";
};

const detectMacArchitecture = async () => {
  const uaData = navigator.userAgentData;
  if (uaData && typeof uaData.getHighEntropyValues === "function") {
    try {
      const entropy = await uaData.getHighEntropyValues(["architecture"]);
      const architecture = lowercase(entropy.architecture);
      if (/(arm|arm64|aarch64)/.test(architecture)) {
        return "arm";
      }
      if (/(x86|x64|intel)/.test(architecture)) {
        return "x86";
      }
    } catch {
      // Ignore and fall back to lower-confidence user-agent checks.
    }
  }

  const userAgent = lowercase(navigator.userAgent);
  if (/(arm|arm64|aarch64|m1|m2|silicon)/.test(userAgent)) {
    return "arm";
  }
  if (/(x86|x86_64|x86-64|amd64|intel|win64|wow64)/.test(userAgent)) {
    return "x86";
  }

  return "unknown";
};

const detectDownloadTarget = async () => {
  const basePlatform = detectBasePlatform();
  if (basePlatform !== "macos") {
    return basePlatform;
  }

  const architecture = await detectMacArchitecture();
  return architecture === "arm" ? "macos" : "macos-fallback";
};

const resolveDownloadConfig = (button, target) => {
  const defaultConfig = {
    url: button.dataset.downloadDefaultUrl,
    text: button.dataset.downloadDefaultText,
    label: button.dataset.downloadDefaultLabel,
    title: button.dataset.downloadDefaultTitle,
  };

  if (target === "windows" && button.dataset.downloadWindowsUrl) {
    return {
      url: button.dataset.downloadWindowsUrl,
      text: button.dataset.downloadWindowsText,
      label: button.dataset.downloadWindowsLabel,
      title: button.dataset.downloadWindowsTitle,
    };
  }

  if (target === "linux" && button.dataset.downloadLinuxUrl) {
    return {
      url: button.dataset.downloadLinuxUrl,
      text: button.dataset.downloadLinuxText,
      label: button.dataset.downloadLinuxLabel,
      title: button.dataset.downloadLinuxTitle,
    };
  }

  if (target === "macos" && button.dataset.downloadMacosUrl) {
    return {
      url: button.dataset.downloadMacosUrl,
      text: button.dataset.downloadMacosText,
      label: button.dataset.downloadMacosLabel,
      title: button.dataset.downloadMacosTitle,
    };
  }

  if (target === "macos-fallback" && button.dataset.downloadMacosFallbackUrl) {
    return {
      url: button.dataset.downloadMacosFallbackUrl,
      text: button.dataset.downloadMacosFallbackText,
      label: button.dataset.downloadMacosFallbackLabel,
      title: button.dataset.downloadMacosFallbackTitle,
    };
  }

  return defaultConfig;
};

const applyDownloadConfig = (button, config) => {
  if (config.url) {
    button.href = config.url;
  }
  if (config.label) {
    button.setAttribute("aria-label", config.label);
  }
  if (config.title) {
    button.title = config.title;
  }

  const label = button.querySelector("[data-download-label]");
  if (label && config.text) {
    label.textContent = config.text;
  }
};

const initDownloadButtons = async () => {
  const buttons = Array.from(document.querySelectorAll('[data-download-button="adaptive"]'));
  if (!buttons.length) {
    return;
  }

  const target = await detectDownloadTarget();
  buttons.forEach((button) => {
    applyDownloadConfig(button, resolveDownloadConfig(button, target));
  });
};

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", () => {
    void initDownloadButtons();
  });
} else {
  void initDownloadButtons();
}
