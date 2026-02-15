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
  "revolution/history":     { tab: "revolution", subtab: "REVOLUTION_HISTORY" },
  "revolution/pioneers":    { tab: "revolution", subtab: "REVOLUTION_PIONEERS" },
  "revolution/whitepaper":  { tab: "revolution", subtab: "REVOLUTION_WHITEPAPER" },

  // Network
  "network/structure":  { tab: "network", subtab: "NETWORK_STRUCTURE" },
  "network/technology": { tab: "network", subtab: "NETWORK_TECHNOLOGY" },
  "network/nodes":      { tab: "network", subtab: "NETWORK_NODES" },
  "network/miners":     { tab: "network", subtab: "NETWORK_MINER" },

  // Metrics
  "metrics/price": { tab: "metrics", subtab: "METRICS_BTC_USD_EUR" },

  "metrics/difficulty": { tab: "metrics", subtab: "METRICS_BTC_DIFFICULTY" },
  "metrics/difficulty/1y": { tab: "metrics", subtab: "METRICS_BTC_DIFFICULTY", subsubtab: "METRICS_BTC_DIFFICULTY_1Y" },
  "metrics/difficulty/5y": { tab: "metrics", subtab: "METRICS_BTC_DIFFICULTY", subsubtab: "METRICS_BTC_DIFFICULTY_5Y" },
  "metrics/difficulty/10y": { tab: "metrics", subtab: "METRICS_BTC_DIFFICULTY", subsubtab: "METRICS_BTC_DIFFICULTY_10Y" },
  "metrics/difficulty/ever": { tab: "metrics", subtab: "METRICS_BTC_DIFFICULTY", subsubtab: "METRICS_BTC_DIFFICULTY_EVER" },

  "metrics/tx-volume": { tab: "metrics", subtab: "METRICS_BTC_TX_VOLUME" },
  "metrics/tx-volume/1h": { tab: "metrics", subtab: "METRICS_BTC_TX_VOLUME", subsubtab: "METRICS_BTC_TX_VOLUME_1H" },
  "metrics/tx-volume/24h": { tab: "metrics", subtab: "METRICS_BTC_TX_VOLUME", subsubtab: "METRICS_BTC_TX_VOLUME_24H" },
  "metrics/tx-volume/1w": { tab: "metrics", subtab: "METRICS_BTC_TX_VOLUME", subsubtab: "METRICS_BTC_TX_VOLUME_1W" },
  "metrics/tx-volume/1m": { tab: "metrics", subtab: "METRICS_BTC_TX_VOLUME", subsubtab: "METRICS_BTC_TX_VOLUME_1M" },
  "metrics/tx-volume/1y": { tab: "metrics", subtab: "METRICS_BTC_TX_VOLUME", subsubtab: "METRICS_BTC_TX_VOLUME_1J" },

  "metrics/tx-amount": { tab: "metrics", subtab: "METRICS_BTC_TX_AMOUNT" },
  "metrics/tx-amount/mempool": { tab: "metrics", subtab: "METRICS_BTC_TX_AMOUNT", subsubtab: "METRICS_BTC_TX_AMOUNT_NOW" },
  "metrics/tx-amount/24h": { tab: "metrics", subtab: "METRICS_BTC_TX_AMOUNT", subsubtab: "METRICS_BTC_TX_AMOUNT_24H" },
  "metrics/tx-amount/1w": { tab: "metrics", subtab: "METRICS_BTC_TX_AMOUNT", subsubtab: "METRICS_BTC_TX_AMOUNT_1W" },
  "metrics/tx-amount/1m": { tab: "metrics", subtab: "METRICS_BTC_TX_AMOUNT", subsubtab: "METRICS_BTC_TX_AMOUNT_1M" },
  "metrics/tx-amount/1y": { tab: "metrics", subtab: "METRICS_BTC_TX_AMOUNT", subsubtab: "METRICS_BTC_TX_AMOUNT_1Y" },
  "metrics/tx-amount/halving": { tab: "metrics", subtab: "METRICS_BTC_TX_AMOUNT", subsubtab: "METRICS_BTC_TX_AMOUNT_HALVING" },
  "metrics/tx-amount/ever": { tab: "metrics", subtab: "METRICS_BTC_TX_AMOUNT", subsubtab: "METRICS_BTC_TX_AMOUNT_EVER" },

  "metrics/tx-fees": { tab: "metrics", subtab: "METRICS_BTC_TX_FEES" },
  "metrics/tx-fees/24h": { tab: "metrics", subtab: "METRICS_BTC_TX_FEES", subsubtab: "METRICS_BTC_TX_FEES_24H" },
  "metrics/tx-fees/1w": { tab: "metrics", subtab: "METRICS_BTC_TX_FEES", subsubtab: "METRICS_BTC_TX_FEES_1W" },
  "metrics/tx-fees/1m": { tab: "metrics", subtab: "METRICS_BTC_TX_FEES", subsubtab: "METRICS_BTC_TX_FEES_1M" },
  "metrics/tx-fees/1y": { tab: "metrics", subtab: "METRICS_BTC_TX_FEES", subsubtab: "METRICS_BTC_TX_FEES_1J" },

  "metrics/hashrate": { tab: "metrics", subtab: "METRICS_BTC_HASHRATE" },
  "metrics/hashrate/1y": { tab: "metrics", subtab: "METRICS_BTC_HASHRATE", subsubtab: "METRICS_BTC_HASHRATE_1Y" },
  "metrics/hashrate/5y": { tab: "metrics", subtab: "METRICS_BTC_HASHRATE", subsubtab: "METRICS_BTC_HASHRATE_5Y" },
  "metrics/hashrate/10y": { tab: "metrics", subtab: "METRICS_BTC_HASHRATE", subsubtab: "METRICS_BTC_HASHRATE_10Y" },
  "metrics/hashrate/ever": { tab: "metrics", subtab: "METRICS_BTC_HASHRATE", subsubtab: "METRICS_BTC_HASHRATE_EVER" },


  // Review
  "review/btc-fiat":   { tab: "review", subtab: "REVIEW_BTC_VS_FIAT" },
  "review/btc-gold":   { tab: "review", subtab: "REVIEW_BTC_VS_GOLD" },
  "review/btc-silver": { tab: "review", subtab: "REVIEW_BTC_VS_SILVER" },
  "review/tx-volume":  { tab: "review", subtab: "REVIEW_BTC_TX_VOLUME" },
  "review/tx-amount":  { tab: "review", subtab: "REVIEW_BTC_TX_AMOUNT" },
  "review/tx-fees":    { tab: "review", subtab: "REVIEW_BTC_TX_FEES" },

  // Explorer
  "explorer/transaction": { tab: "explorer", subtab: "EXPLORER_TXID" },
  "explorer/address":     { tab: "explorer", subtab: "EXPLORER_ADDRESS" },
  "explorer/wallet":      { tab: "explorer", subtab: "EXPLORER_WALLET" },

  // Treasuries
  "treasuries/companies":     { tab: "treasuries", subtab: "TREASURIES_COMPANIES" },
  "treasuries/institutions":  { tab: "treasuries", subtab: "TREASURIES_INSTITUTIONS" },
  "treasuries/countries":     { tab: "treasuries", subtab: "TREASURIES_COUNTRIES" },

  // Market Cap
  "market-cap/crypto":      { tab: "market_cap", subtab: "MARKET_CAP_COINS" },
  "market-cap/companies":   { tab: "market_cap", subtab: "MARKET_CAP_COMPANIES" },
  "market-cap/currencies":  { tab: "market_cap", subtab: "MARKET_CAP_CURRENCIES" },
  "market-cap/commodities": { tab: "market_cap", subtab: "MARKET_CAP_COMMODITIES" },

  // Indicators
  "indicators/pi-cycle-top":    { tab: "indicators", subtab: "INDICATORS_PI_CYCLE_TOP" },
  "indicators/golden-ratio":    { tab: "indicators", subtab: "INDICATORS_GOLDEN_RATIO" },
  "indicators/rainbow":         { tab: "indicators", subtab: "INDICATORS_RAINBOW_CHART" },
  "indicators/mayer-multiple":  { tab: "indicators", subtab: "INDICATORS_MAYER_MULTIPLE" },
  "indicators/stock-to-flow":   { tab: "indicators", subtab: "INDICATORS_STOCK_TO_FLOW" },
  "indicators/btc-vs-m2":       { tab: "indicators", subtab: "INDICATORS_BTC_M2" },
  "indicators/sp500-vs-btc":    { tab: "indicators", subtab: "INDICATORS_S&P500_BTC" },

  // Info
  "info/status":  { tab: "info", subtab: "INFO_STATUS" },
  "info/traffic": { tab: "info", subtab: "INFO_DASHBOARD_TRAFFIC" },
  "info/imprint": { tab: "info", subtab: "INFO_IMPRESSUM" }
};

/* ==================================================
   🧭 ROUTER CORE – URL → UI (SubSubTab Ready)
================================================== */

function routeTo(path) {

  const cleanPath = path.replace(/^\/|\/$/g, "");
  const route = ROUTES[cleanPath];
  if (!route) return;

  const { tab, subtab, subsubtab } = route;

  // 🧠 Router-Flag aktivieren
  window.__ROUTER_ACTIVE__ = true;

  // 1️⃣ Haupttab aktivieren
  const mainBtn = document.querySelector(
    `.tabButton[data-tab="${tab}"]`
  );
  if (!mainBtn) return;

  mainBtn.click();

  // 2️⃣ Subtab aktivieren
  if (subtab) {
    setTimeout(() => {

      const subBtn = document.querySelector(
        `.subTabButton[data-subtab="${subtab}"]`
      );

      if (subBtn && typeof showSubTab === "function") {
        showSubTab(subBtn);
      }

    }, 60);
  }

  // 3️⃣ Sub-SubTab aktivieren
  if (subsubtab) {
    setTimeout(() => {

      const btn = document.querySelector(
        `[data-subsubtab="${subsubtab}"]`
      );

      if (btn && typeof btn.click === "function") {
        btn.click();
      }

      // Router Init abgeschlossen
      window.__ROUTER_ACTIVE__ = false;

    }, 140);

  } else {
    window.__ROUTER_ACTIVE__ = false;
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

  if (ROUTES[path]) {
    routeTo(path);
  }

});


/* ==================================================
   🧭 MAIN TAB CLICK → /revolution
================================================== */

document.addEventListener("click", e => {

  const btn = e.target.closest(".tabButton");
  if (!btn) return;

  const tab = btn.dataset.tab;

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


/* ==================================================
   🖱️ SUBSUBTAB CLICK → /metrics/tx-amount/24h
================================================== */

document.addEventListener("click", e => {

  const btn = e.target.closest("[data-subsubtab]");
  if (!btn) return;

  // 🚫 Router-Init ignorieren (sonst Loop)
  if (window.__ROUTER_ACTIVE__) return;

  const subsubtab = btn.dataset.subsubtab;

  const entry = Object.entries(ROUTES)
    .find(([_, r]) => r.subsubtab === subsubtab);

  if (entry) {
    const [path] = entry;

    history.pushState(
      { path },
      "",
      "/" + path
    );
  }

});
