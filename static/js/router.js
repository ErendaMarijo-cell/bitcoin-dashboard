// ==================================================
// router.js – Client-Side Routing for Bitcoin Dashboard
// ==================================================
// - Clean URLs for tabs & subtabs
// - SEO-friendly (one URL per semantic state)
// - No framework, no refactor of existing UI logic
// ==================================================


/* ==================================================
   🔗 ROUTE CONFIGURATION
================================================== */

const ROUTES = {
  "": { tab: "home", subtab: null },

  // Revolution
  "revolution/history": { tab: "revolution", subtab: "REVOLUTION_HISTORY" },
  "revolution/pioneers": { tab: "revolution", subtab: "REVOLUTION_PIONEERS" },
  "revolution/whitepaper": { tab: "revolution", subtab: "REVOLUTION_WHITEPAPER" },

  // Network
  "network/structure": { tab: "network", subtab: "NETWORK_STRUCTURE" },
  "network/technology": { tab: "network", subtab: "NETWORK_TECHNOLOGY" },
  "network/nodes": { tab: "network", subtab: "NETWORK_NODES" },
  "network/miners": { tab: "network", subtab: "NETWORK_MINER" },

  // Metrics
  "metrics/price": { tab: "metrics", subtab: "METRICS_BTC_USD_EUR" },
  "metrics/difficulty": { tab: "metrics", subtab: "METRICS_BTC_DIFFICULTY" },
  "metrics/tx-volume": { tab: "metrics", subtab: "METRICS_BTC_TX_VOLUME" },
  "metrics/tx-amount": { tab: "metrics", subtab: "METRICS_BTC_TX_AMOUNT" },
  "metrics/tx-fees": { tab: "metrics", subtab: "METRICS_BTC_TX_FEES" },
  "metrics/hashrate": { tab: "metrics", subtab: "METRICS_BTC_HASHRATE" },

  // Explorer
  "explorer/transaction": { tab: "explorer", subtab: "EXPLORER_TXID" },
  "explorer/address": { tab: "explorer", subtab: "EXPLORER_ADDRESS" },
  "explorer/wallet": { tab: "explorer", subtab: "EXPLORER_WALLET" },

  // Treasuries
  "treasuries/companies": { tab: "treasuries", subtab: "TREASURIES_COMPANIES" },
  "treasuries/institutions": { tab: "treasuries", subtab: "TREASURIES_INSTITUTIONS" },
  "treasuries/countries": { tab: "treasuries", subtab: "TREASURIES_COUNTRIES" },

  // Market Cap
  "market-cap/crypto": { tab: "market_cap", subtab: "MARKET_CAP_COINS" },
  "market-cap/companies": { tab: "market_cap", subtab: "MARKET_CAP_COMPANIES" },
  "market-cap/currencies": { tab: "market_cap", subtab: "MARKET_CAP_CURRENCIES" },
  "market-cap/commodities": { tab: "market_cap", subtab: "MARKET_CAP_COMMODITIES" },

  // Indicators
  "indicators/pi-cycle-top": { tab: "indicators", subtab: "INDICATORS_PI_CYCLE_TOP" },
  "indicators/golden-ratio": { tab: "indicators", subtab: "INDICATORS_GOLDEN_RATIO" },
  "indicators/rainbow": { tab: "indicators", subtab: "INDICATORS_RAINBOW_CHART" },
  "indicators/mayer-multiple": { tab: "indicators", subtab: "INDICATORS_MAYER_MULTIPLE" },
  "indicators/stock-to-flow": { tab: "indicators", subtab: "INDICATORS_STOCK_TO_FLOW" },
  "indicators/btc-vs-m2": { tab: "indicators", subtab: "INDICATORS_BTC_M2" },
  "indicators/sp500-vs-btc": { tab: "indicators", subtab: "INDICATORS_S&P500_BTC" },

  // Info
  "info/status": { tab: "info", subtab: "INFO_STATUS" },
  "info/traffic": { tab: "info", subtab: "INFO_DASHBOARD_TRAFFIC" },
  "info/imprint": { tab: "info", subtab: "INFO_IMPRESSUM" }
};


/* ==================================================
   🧭 ROUTER CORE – URL → UI
================================================== */

function routeTo(path) {
  const cleanPath = path.replace(/^\/|\/$/g, "");
  const route = ROUTES[cleanPath];
  if (!route) return;

  const { tab, subtab } = route;

  // 1️⃣ Haupttab aktivieren
  const mainBtn = document.querySelector(
    `.tabButton[data-tab="${tab}"]`
  );
  if (!mainBtn) return;

  mainBtn.click();

  // 2️⃣ Subtab aktivieren (nach UI-Init)
  if (subtab) {
    setTimeout(() => {
      const subBtn = document.querySelector(
        `.subTabButton[data-subtab="${subtab}"]`
      );
      if (subBtn && typeof showSubTab === "function") {
        showSubTab(subBtn);
      }
    }, 50);
  }
}


/* ==================================================
   ⬅️➡️ BACK / FORWARD
================================================== */

window.addEventListener("popstate", e => {
  if (e.state?.path !== undefined) {
    routeTo(e.state.path);
  }
});


/* ==================================================
   🚀 INITIAL LOAD
================================================== */

document.addEventListener("DOMContentLoaded", () => {
  const path = location.pathname.replace(/^\/|\/$/g, "");
  if (ROUTES[path]) routeTo(path);
});


/* ==================================================
   🧭 MAIN TAB CLICK → /revolution
================================================== */

document.addEventListener("click", e => {
  const btn = e.target.closest(".tabButton");
  if (!btn) return;

  const tab = btn.dataset.tab;

  // 👉 Default-Subtab-Route finden
  const entry = Object.entries(ROUTES)
    .find(([_, r]) => r.tab === tab);

  if (entry) {
    const [path] = entry;
    history.pushState({ path }, "", "/" + path);
  }
});



/* ==================================================
   🖱️ SUBTAB CLICK → /revolution/history
================================================== */

document.addEventListener("click", e => {
  const btn = e.target.closest(".subTabButton");
  if (!btn) return;

  const subtab = btn.dataset.subtab;
  const entry = Object.entries(ROUTES)
    .find(([_, r]) => r.subtab === subtab);

  if (entry) {
    const [path] = entry;
    history.pushState({ path }, "", "/" + path);
  }
});
