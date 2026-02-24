const CONSENT_KEY = "perspecta_consent_v1";

const getStoredConsent = () => {
  try {
    return JSON.parse(localStorage.getItem(CONSENT_KEY));
  } catch {
    return null;
  }
};

const storeConsent = (value) => {
  try {
    localStorage.setItem(CONSENT_KEY, JSON.stringify(value));
    return true;
  } catch (error) {
    console.warn(`Failed to store consent in ${CONSENT_KEY}.`, error);
    return false;
  }
};

const updateConsent = (granted) => {
  if (typeof gtag === "function") {
    gtag("consent", "update", {
      analytics_storage: granted ? "granted" : "denied",
    });
  }
};

const hideBanner = () => {
  const banner = document.querySelector(".cookie-banner");
  if (banner) {
    banner.classList.add("is-hidden");
  }
};

const showBanner = () => {
  const banner = document.querySelector(".cookie-banner");
  if (banner) {
    banner.classList.remove("is-hidden");
  }
  const decline = document.querySelector(".cookie-decline");
  if (decline && typeof decline.focus === "function") {
    decline.focus();
  }
};

const applyConsent = (granted) => {
  storeConsent({ granted, ts: Date.now() });
  updateConsent(granted);
  hideBanner();
};

const initConsent = () => {
  const stored = getStoredConsent();
  if (stored && typeof stored.granted === "boolean") {
    updateConsent(stored.granted);
    hideBanner();
  } else {
    showBanner();
  }

  const accept = document.querySelector(".cookie-accept");
  const decline = document.querySelector(".cookie-decline");
  if (accept) {
    accept.addEventListener("click", () => applyConsent(true));
  }
  if (decline) {
    decline.addEventListener("click", () => applyConsent(false));
  }

  document.querySelectorAll(".cookie-manage").forEach((link) => {
    link.addEventListener("click", (event) => {
      event.preventDefault();
      showBanner();
    });
  });
};

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initConsent);
} else {
  initConsent();
}
