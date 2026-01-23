// =======================================
// 🌍 I18N LABELS – BITCOIN DASHBOARD (EN)
// =======================================

const I18N = {
  en: {
    // =====================
    // NAVIGATION
    // =====================
    nav: {
      home: "Home",
      revolution: "Bitcoin Revolution",
      network: "Bitcoin Network",
      metrics: "Metrics",
      explorer: "Explorer",
      treasuries: "Bitcoin Treasuries",
      market_cap: "Market Capitalization",
      indicators: "Indicators",
      info: "Info"
    },


    // ==========================
    // HOME
    // ==========================
    home: {

      // ---------------------
      // SEO (Invisible Intro)
      // ---------------------
      seo: {
          title: "BITCOIN LIVE DASHBOARD – Blockchain Insights",

          text1:
            "Bitcoin Dashboard is a real-time Bitcoin analytics platform providing live blockchain data, " +
            "current Bitcoin price information, network hashrate metrics, mempool volume, transaction fees, " +
            "and on-chain statistics across the Bitcoin mainnet.",

          text2:
            "The dashboard tracks Bitcoin from the genesis block to the present day, featuring live BTC price updates, " +
            "halving countdowns, top and large Bitcoin transactions in the mempool, unconfirmed transactions, " +
            "fee market conditions, average confirmation times, and real-time network activity."
        },

      // ------
      // TITLES
      // ------
      titles: {
        bitcoin: "Bitcoin Overview",
        blockchain: "Blockchain Status",
        mempool: "Mempool Overview",
        top10: "Top 10 Mempool Transactions (live)",
        top50: "Top 50 Bitcoin Transactions in 2026",
        node: "Node Information",
        dashboard: "Dashboard Traffic",
        meta: "System Status",
        donation: "Support This Project"
      },

      // ---------
      // BITCOIN INFO
      // ---------
      bitcoin_info: {
        genesis: "Since Genesis Block",
        halving_time: "Estimated Time Until Halving",
        halving_blocks: "Blocks Remaining Until Halving",
        price: "Current BTC Price"
      },

      // ---------
      // BLOCKCHAIN INFO
      // ---------
      blockchain_info: {
        chain: "Chain",
        height: "Block Height",
        hashrate: "Current Hashrate",
        winner_hash: "Winning Block Hash",
        block_age: "Current Block Age",
        reward: "Current Block Reward",
        tx_last_block: "Transactions in Latest Block"
      },

      // ---------
      // MEMPOOL INFO
      // ---------
      mempool_info: {
        open_tx: "Unconfirmed Transactions",
        volume: "Mempool Volume",
        volume_1h: "Volume Last Hour",
        volume_24h: "Volume Last 24 Hours",
        avg_tx: "Average Transaction Size",
        min_fee: "Minimum Fee",
        avg_fee: "Average Fee",
        total_fee: "Total Fees",
        avg_wait: "Estimated Average Confirmation Time"
      },

      // ---------
      // TABLE HEADERS
      // ---------
      tables: {
        nr: "NR",
        txid: "TXID",
        amount: "BTC"
      },

      // ---------
      // NODE INFO
      // ---------
      node_info: {
        peers: "Peers",
        version: "Version",
        subversion: "Sub-version",
        protocol: "Protocol Version"
      },

      // ---------
      // DASHBOARD TRAFFIC
      // ---------
      dashboard: {
        live: "Current Traffic (live)",
        total: "Total Traffic Since Launch",
        launch: "Launch Since"
      },

      // ---------
      // META DASHBOARD
      // ---------
      meta: {
        cpu: "CPU",
        ram: "RAM",
        swap: "Swap",
        nvme_read: "NVMe Read",
        nvme_write: "NVMe Write",
        nvme_free: "NVMe Storage",
        redis_hits: "Successful Internal Requests",
        redis_misses: "Failed Internal Requests",
        req_sec: "Direct Requests per Second",
        req_day: "Direct Requests per Day",
        redis_used: "Request Memory Usage",
        net_down: "Internet Download",
        net_up: "Internet Upload",
        lan_down: "LAN Download"
      },

      // ---------
      // DONATION
      // ---------
      donation: {
        text:
          "This project is built out of personal interest and passion for development. " +
          "If you would like to support it, you can do so with a voluntary Bitcoin donation.",
        address: "Bitcoin Address",
        copy: "Copy"
      }
    },

    // =====================
    // FOOTER
    // =====================
    footer: "© 2026 BITCOIN DASHBOARD | Powered by Open Data & Passion for Code."
  }
};

// =====================
// ACTIVE LANGUAGE
// =====================
const T = I18N.en;


// =========================================================
// SEO META – Haupt-Tabs (Google-relevant)
// =========================================================
const META_BY_MAIN_TAB = {
  HOME: {
    title: "BITCOIN LIVE DASHBOARD – Blockchain Insights",
    description: "Real-time Bitcoin network statistics including mempool, hashrate, blocks, fees, and on-chain metrics."
  },
  REVOLUTION: {
    title: "What Is Bitcoin? – History & Origins",
    description: "Learn how Bitcoin was created, why it exists, and how it started a global monetary revolution."
  },
  NETWORK: {
    title: "Bitcoin Network – Nodes, Mining & Structure",
    description: "Understand how the Bitcoin network works, including nodes, miners, consensus, and underlying technology."
  },
  METRICS: {
    title: "Bitcoin Metrics – Price, Hashrate & Transactions",
    description: "Live Bitcoin metrics including price, hashrate, difficulty and on-chain activity."
  },
  EXPLORER: {
    title: "Bitcoin Explorer – Transactions & Addresses",
    description: "Analyze Bitcoin transactions, addresses, and wallets using real-time blockchain data."
  },
  TREASURIES: {
    title: "Bitcoin Treasuries – Companies, Institutions & Countries",
    description: "Discover which companies, institutions, and countries hold Bitcoin as part of their treasury strategy."
  },
  MARKET_CAP: {
    title: "Market Capitalization – Bitcoin & Global Assets",
    description: "Compare market capitalizations of Bitcoin, cryptocurrencies, companies, fiat currencies, and commodities."
  },
  INDICATORS: {
    title: "Bitcoin Indicators – On-Chain & Market Cycles",
    description: "Track Bitcoin indicators such as M2 money supply, market cycles, power law models, and sentiment indices."
  },
  INFO: {
    title: "Dashboard Information – Status & Project Details",
    description: "View system status, dashboard traffic statistics, and legal information about the Bitcoin Dashboard project."
  }
};


// =========================================================
// Apply SEO Meta for Main Tab
// =========================================================
function updateMetaForMainTab(tabId) {
  if (!tabId) return;
  const key = tabId.toUpperCase();
  const meta = META_BY_MAIN_TAB[key];
  if (!meta) return;
  document.title = meta.title;
  let desc = document.querySelector('meta[name="description"]');
  if (!desc) {
    desc = document.createElement("meta");
    desc.name = "description";
    document.head.appendChild(desc);
  }
  desc.setAttribute("content", meta.description);
}
