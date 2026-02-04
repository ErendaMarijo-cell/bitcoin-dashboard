(() => {

/*
====================================================================
REVIEW_BTC_VS_FIAT.js
- Bitcoin Value vs Fiat (USD / EUR / JPY)
- JSONL (close-only, daily)
- State-driven
- Single Chart Instance
====================================================================
*/

// --------------------------------------------------
// 🧠 Review Chart State (Single Source of Truth)
// --------------------------------------------------
const reviewChartState = {
    assetClass: 'fiat',
    fiat: 'usd',                  // 'usd' | 'eur' | 'jpy'
    timeMode: 'all',               // 'all' | 'year' | 'halving'
    year: null,
    halvingCycle: null,
    logScale: true
};

// --------------------------------------------------
// 📦 Internal Cache
// --------------------------------------------------
const dataCache = {
    usd: null,
    eur: null,
    jpy: null
};

let chartInstance = null;
let initialized   = false;

// --------------------------------------------------
// 🗓️ Halving Date Ranges (UTC, inclusive)
// --------------------------------------------------
const HALVING_RANGES = {
    '2009_2012': ['2009-01-03', '2012-11-28'],
    '2012_2016': ['2012-11-28', '2016-07-09'],
    '2016_2020': ['2016-07-09', '2020-05-11'],
    '2020_2024': ['2020-05-11', '2024-04-20'],
    '2024_now':  ['2024-04-20', null]
};

// --------------------------------------------------
// 🔄 Load JSONL (once per fiat)
// --------------------------------------------------
async function loadJSONL(path) {
    const res = await fetch(path);
    if (!res.ok) throw new Error(`Failed to load ${path}`);
    return (await res.text())
        .trim()
        .split('\n')
        .map(l => JSON.parse(l))
        .map(p => ({ date: new Date(p.date), value: p.value }));
}

async function ensureDataLoaded() {
    if (!dataCache.usd)
        dataCache.usd = await loadJSONL('/data/review/bitcoin_value/btc_vs_fiat/usd/btc_vs_usd_all.jsonl');
    if (!dataCache.eur)
        dataCache.eur = await loadJSONL('/data/review/bitcoin_value/btc_vs_fiat/eur/btc_vs_eur_all.jsonl');
    if (!dataCache.jpy)
        dataCache.jpy = await loadJSONL('/data/review/bitcoin_value/btc_vs_fiat/jpy/btc_vs_jpy_all.jsonl');
}

// --------------------------------------------------
// 🔍 Filter
// --------------------------------------------------
function filterData(raw) {
    let out = raw;

    if (reviewChartState.timeMode === 'year' && reviewChartState.year)
        out = out.filter(p => p.date.getUTCFullYear() === reviewChartState.year);

    if (reviewChartState.timeMode === 'halving' && reviewChartState.halvingCycle) {
        const [s, e] = HALVING_RANGES[reviewChartState.halvingCycle];
        const start = new Date(s);
        const end   = e ? new Date(e) : null;
        out = out.filter(p => p.date >= start && (!end || p.date <= end));
    }

    return out;
}

// --------------------------------------------------
// 📊 Chart
// --------------------------------------------------
function updateChart() {
    const canvas = document.getElementById('REVIEW_BTC_VS_FIAT_CANVAS');
    if (!canvas) return;

    const ctx = canvas.getContext('2d');
    const raw = dataCache[reviewChartState.fiat];
    const data = filterData(raw);

    const labels = data.map(p => p.date);
    const values = data.map(p => p.value);

    if (!chartInstance) {
        chartInstance = new Chart(ctx, {
            type: 'line',
            data: {
                labels,
                datasets: [{
                    label: `BTC vs ${reviewChartState.fiat.toUpperCase()}`,
                    data: values,
                    borderWidth: 2,
                    tension: 0.25,
                    fill: true
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'top' },
                    zoom: {
                        pan: { enabled: true, mode: 'x' },
                        zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: 'x' }
                    }
                },
                scales: {
                    x: { type: 'time' },
                    y: {
                        type: reviewChartState.logScale ? 'logarithmic' : 'linear',
                        title: { display: true, text: 'Bitcoin Value' }
                    }
                }
            }
        });

        canvas.addEventListener('dblclick', () => chartInstance.resetZoom());
    } else {
        chartInstance.data.labels = labels;
        chartInstance.data.datasets[0].data = values;
        chartInstance.data.datasets[0].label = `BTC vs ${reviewChartState.fiat.toUpperCase()}`;
        chartInstance.options.scales.y.type = reviewChartState.logScale ? 'logarithmic' : 'linear';
        chartInstance.update('none');
    }
}

// --------------------------------------------------
// 🚀 PUBLIC LOADER (Metrics-konform)
// --------------------------------------------------
async function loadReviewBtcVsFiat() {
    if (!initialized) {
        await ensureDataLoaded();
        initialized = true;
    }
    updateChart();
}

// 🌍 Export für Router
window.loadReviewBtcVsFiat = loadReviewBtcVsFiat;

})();
