(() => {

/*
====================================================================
REVIEW_BTC_VS_FIAT.js
FINAL VERIFIED VERSION
Features:
✔ Line Chart only (Macro focus)
✔ Correct auto-fit
✔ Double-click zoom reset (re-filter safe)
✔ Adaptive X-Axis labeling (ALL/YEAR/HALVING/CUSTOM by range)
✔ Log / Linear toggle
✔ Custom Range (from/to date inputs)
✔ Tooltip easier to trigger (magnetic hover)
✔ Minimal invasive / keeps your architecture
====================================================================
*/

// --------------------------------------------------
// 🧠 State
// --------------------------------------------------
const reviewChartState = {
    fiat: 'usd',
    timeMode: 'all',       // all | year | halving | custom
    year: null,
    halvingCycle: null,
    logScale: true,

    // 🆕 Custom Range
    customStart: null,     // "YYYY-MM-DD"
    customEnd: null        // "YYYY-MM-DD"
};

// --------------------------------------------------
// 📦 Cache
// --------------------------------------------------
const dataCache = {
    usd: null,
    eur: null,
    jpy: null,

    gbp: null,
    chf: null,

    aud: null,
    cad: null,
    nzd: null,

    cny: null,
    inr: null,
    sgd: null,

    // 🆕 Eurasia
    rub: null,

    // 🆕 Inflation / Debasement
    try: null,
    ars: null,
    brl: null,
    zar: null
};



let chartInstance = null;
let initialized   = false;

// --------------------------------------------------
// 🗓️ Halving Ranges
// --------------------------------------------------
const HALVING_RANGES = {
    '2009_2012': ['2009-01-03','2012-11-28'],
    '2012_2016': ['2012-11-28','2016-07-09'],
    '2016_2020': ['2016-07-09','2020-05-11'],
    '2020_2024': ['2020-05-11','2024-04-20'],
    '2024_now':  ['2024-04-20', null]
};

// --------------------------------------------------
// 🧮 Helpers
// --------------------------------------------------
function clampDateOrder(start, end){
    if(!start || !end) return { start, end };
    if(start > end) return { start: end, end: start };
    return { start, end };
}

function diffYears(start, end){
    const ms = end - start;
    return ms / (1000 * 60 * 60 * 24 * 365);
}

function pad2(n){
    return String(n).padStart(2,'0');
}

function formatISODateUTC(d){
    // YYYY-MM-DD (UTC)
    return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth()+1)}-${pad2(d.getUTCDate())}`;
}

// --------------------------------------------------
// 🧭 Adaptive Time Scale Config (unit + tick callback)
// --------------------------------------------------
function getTimeScaleConfig(data){

    // Fallback
    const fallback = {
        unit: 'month',
        callback: (value) => {
            const d = new Date(value);
            return d.toLocaleString('en-US',{month:'short'});
        }
    };

    if(!data?.length) return fallback;

    const start = data[0].x;
    const end   = data.at(-1).x;
    const years = diffYears(start, end);

    // -----------------------------
    // ALL: keep your existing macro style
    // -----------------------------
    if(reviewChartState.timeMode === 'all'){
        return {
            unit: 'quarter',
            callback: (value) => {
                const d = new Date(value);
                const q = Math.floor(d.getUTCMonth() / 3) + 1;
                return `Q${q} ${d.getUTCFullYear()}`;
            }
        };
    }

    // -----------------------------
    // YEAR: keep your existing month labels
    // -----------------------------
    if(reviewChartState.timeMode === 'year'){
        return {
            unit: 'month',
            callback: (value) => {
                const d = new Date(value);
                return d.toLocaleString('en-US',{month:'short'});
            }
        };
    }

    // -----------------------------
    // HALVING: keep your existing month + yy labels
    // -----------------------------
    if(reviewChartState.timeMode === 'halving'){
        return {
            unit: 'month',
            callback: (value) => {
                const d = new Date(value);
                return d.toLocaleString('en-US',{month:'short', year:'2-digit'});
            }
        };
    }

    // -----------------------------
    // CUSTOM: adaptive by range length
    // < 1y  -> month (Mon)
    // 1-5y  -> quarter (Qn YY)
    // > 5y  -> year (YYYY)
    // -----------------------------
    if(reviewChartState.timeMode === 'custom'){

        if(years < 1){
            return {
                unit: 'month',
                callback: (value) => {
                    const d = new Date(value);
                    return d.toLocaleString('en-US',{month:'short'});
                }
            };
        }

        if(years < 5){
            return {
                unit: 'quarter',
                callback: (value) => {
                    const d = new Date(value);
                    const q = Math.floor(d.getUTCMonth() / 3) + 1;
                    const yy = String(d.getUTCFullYear()).slice(-2);
                    return `Q${q} '${yy}`;
                }
            };
        }

        return {
            unit: 'year',
            callback: (value) => {
                const d = new Date(value);
                return String(d.getUTCFullYear());
            }
        };
    }

    return fallback;
}

// --------------------------------------------------
// 🔄 JSONL Loader
// --------------------------------------------------
async function loadJSONL(path){

    const res = await fetch(path);
    if(!res.ok) throw new Error(path);

    const txt = (await res.text()).trim();
    if(!txt) return [];

    return txt
        .split('\n')
        .map(l => JSON.parse(l))
        .map(p => ({
            x: new Date(p.date),

            // ✅ FIX: value statt c
            y: p.value
        }));
}


// --------------------------------------------------
async function ensureDataLoaded(){

    if(!dataCache.usd){
        dataCache.usd = await loadJSONL(
            '/data/review/btc_vs_fiat/usd/btc_vs_usd_all.jsonl'
        );
    }

    if(!dataCache.eur){
        dataCache.eur = await loadJSONL(
            '/data/review/btc_vs_fiat/eur/btc_vs_eur_all.jsonl'
        );
    }

    if(!dataCache.jpy){
        dataCache.jpy = await loadJSONL(
            '/data/review/btc_vs_fiat/jpy/btc_vs_jpy_all.jsonl'
        );
    }

    if(!dataCache.gbp){
        dataCache.gbp = await loadJSONL(
            '/data/review/btc_vs_fiat/gbp/btc_vs_gbp_all.jsonl'
        );
    }

    if(!dataCache.chf){
        dataCache.chf = await loadJSONL(
            '/data/review/btc_vs_fiat/chf/btc_vs_chf_all.jsonl'
        );
    }

    if(!dataCache.aud){
        dataCache.aud = await loadJSONL(
            '/data/review/btc_vs_fiat/aud/btc_vs_aud_all.jsonl'
        );
    }

    if(!dataCache.cad){
        dataCache.cad = await loadJSONL(
            '/data/review/btc_vs_fiat/cad/btc_vs_cad_all.jsonl'
        );
    }

    if(!dataCache.nzd){
        dataCache.nzd = await loadJSONL(
            '/data/review/btc_vs_fiat/nzd/btc_vs_nzd_all.jsonl'
        );
    }

    if(!dataCache.cny){
        dataCache.cny = await loadJSONL(
            '/data/review/btc_vs_fiat/cny/btc_vs_cny_all.jsonl'
        );
    }

    if(!dataCache.inr){
        dataCache.inr = await loadJSONL(
            '/data/review/btc_vs_fiat/inr/btc_vs_inr_all.jsonl'
        );
    }

    if(!dataCache.sgd){
        dataCache.sgd = await loadJSONL(
            '/data/review/btc_vs_fiat/sgd/btc_vs_sgd_all.jsonl'
        );
    }

    if(!dataCache.rub){
        dataCache.rub = await loadJSONL(
            '/data/review/btc_vs_fiat/rub/btc_vs_rub_all.jsonl'
        );
    }

    if(!dataCache.try){
        dataCache.try = await loadJSONL(
            '/data/review/btc_vs_fiat/try/btc_vs_try_all.jsonl'
        );
    }

    if(!dataCache.ars){
        dataCache.ars = await loadJSONL(
            '/data/review/btc_vs_fiat/ars/btc_vs_ars_all.jsonl'
        );
    }

    if(!dataCache.brl){
        dataCache.brl = await loadJSONL(
            '/data/review/btc_vs_fiat/brl/btc_vs_brl_all.jsonl'
        );
    }

    if(!dataCache.zar){
        dataCache.zar = await loadJSONL(
            '/data/review/btc_vs_fiat/zar/btc_vs_zar_all.jsonl'
        );
    }

}


// --------------------------------------------------
// 🔍 Filter Logic
// --------------------------------------------------
function filterData(raw){

    let out = raw;

    // YEAR
    if(reviewChartState.timeMode === 'year'
       && reviewChartState.year){

        out = out.filter(p =>
            p.x.getUTCFullYear() === reviewChartState.year
        );
    }

    // HALVING
    if(reviewChartState.timeMode === 'halving'
       && reviewChartState.halvingCycle){

        const [s, e] = HALVING_RANGES[reviewChartState.halvingCycle];

        const start = new Date(s);
        const end   = e ? new Date(e) : null;

        out = out.filter(p =>
            p.x >= start && (!end || p.x <= end)
        );
    }

    // 🆕 CUSTOM RANGE
    if(reviewChartState.timeMode === 'custom'
       && reviewChartState.customStart
       && reviewChartState.customEnd){

        let start = new Date(reviewChartState.customStart);
        let end   = new Date(reviewChartState.customEnd);

        // Ensure correct order
        ({ start, end } = clampDateOrder(start, end));

        // Include full "end day" by bumping end to 23:59:59.999 (UTC-ish)
        end = new Date(end.getTime() + (24*60*60*1000) - 1);

        out = out.filter(p => p.x >= start && p.x <= end);
    }

    return out;
}

// --------------------------------------------------
// 📈 Performance Calculation
// --------------------------------------------------
function calculatePerformance(data){

    if(!data?.length) return null;

    const start = data[0].y;
    const end   = data.at(-1).y;

    if(!start || !end) return null;

    return ((end - start) / start) * 100;
}

// --------------------------------------------------
// 🖊️ Performance Overlay Plugin
// --------------------------------------------------
const performanceOverlayPlugin = {

    id: 'performanceOverlay',

    afterDraw(chart){

        const raw = dataCache[reviewChartState.fiat];
        if(!raw) return;

        const filtered = filterData(raw);
        if(!filtered.length) return;

        const perf = calculatePerformance(filtered);
        if(perf === null) return;

        const ctx = chart.ctx;
        const sign = perf >= 0 ? '+' : '';
        const text = `Performance: ${sign}${perf.toFixed(1)} %`;

        ctx.save();

        ctx.font = '600 1.05rem Inter, system-ui';
        ctx.fillStyle = perf >= 0 ? '#16c784' : '#ea3943';
        ctx.textAlign = 'left';

        ctx.fillText(
            text,
            chart.chartArea.left + 80,
            chart.chartArea.top - 8
        );

        ctx.restore();
    }
};

// --------------------------------------------------
// 🧭 Auto-Fit
// --------------------------------------------------
function fitTimeDomain(chart, data){

    if(!chart || !data?.length) return;

    chart.options.scales.x.min = data[0].x;
    chart.options.scales.x.max = data.at(-1).x;
}

// --------------------------------------------------
// 💱 Fiat Display Names
// --------------------------------------------------
const FIAT_DISPLAY_NAMES = {
    usd: 'US Dollar',
    eur: 'Euro',
    jpy: 'Japanese Yen',

    gbp: 'British Pound',
    chf: 'Swiss Franc',

    aud: 'Australian Dollar',
    cad: 'Canadian Dollar',
    nzd: 'New Zealand Dollar',

    cny: 'Chinese Yuan',
    inr: 'Indian Rupee',
    sgd: 'Singapore Dollar',

    // 🆕 Eurasia
    rub: 'Russian Ruble',

    // 🆕 Inflation / Debasement
    try: 'Turkish Lira',
    ars: 'Argentine Peso',
    brl: 'Brazilian Real',
    zar: 'South African Rand',
};


// --------------------------------------------------
// 📊 Dataset Builder (Line Only)
// --------------------------------------------------
function buildDataset(data){

    const fiatKey   = reviewChartState.fiat;
    const fiatLabel = FIAT_DISPLAY_NAMES[fiatKey] || fiatKey.toUpperCase();
    const legendLabel = `Bitcoin  •  ${fiatLabel}`;

    return {
        type: 'line',
        label: legendLabel,
        data,

        borderWidth: 1.6,
        tension: 0.15,
        fill: false,

        // Punkte normal unsichtbar
        pointRadius: 0,

        // 🧲 Magnetischer Hover-Fangbereich
        pointHitRadius: 30,

        // ✅ Punkt erscheint beim Hover
        pointHoverRadius: 4
    };
}


// --------------------------------------------------
// 📊 Render
// --------------------------------------------------
function updateChart(){

    const canvas = document.getElementById('REVIEW_BTC_VS_FIAT_CANVAS');
    if(!canvas) return;

    const ctx = canvas.getContext('2d');

    const raw = dataCache[reviewChartState.fiat];
    if(!raw) return;

    const data = filterData(raw);
    if(!data.length) return;

    const dataset = buildDataset(data);
    const timeCfg = getTimeScaleConfig(data);

    // --------------------------------------------------
    // 🆕 Chart Creation
    // --------------------------------------------------
    if(!chartInstance){

        chartInstance = new Chart(ctx,{

            plugins: [
                performanceOverlayPlugin
            ],

            data: {
                datasets: [dataset]
            },

            options: {
                responsive: true,
                maintainAspectRatio: false,

                // 🧲 Tooltip “magnetic” behaviour
                interaction: {
                    mode: 'index',
                    intersect: false
                },


                plugins: {

                    // --------------------------------------------------
                    // 🏷️ LEGEND (final styling)
                    // --------------------------------------------------
                    legend: {
                        position: 'top',

                        labels: {

                            // Typography
                            font: {
                                size: 16,
                                weight: '300',
                                family: 'Inter, system-ui'
                            },

                            // Text color (slightly softened for dark UI)
                            color: 'rgba(232, 241, 255, 0.85)',

                            // Legend line appearance
                            boxWidth: 18,
                            boxHeight: 2,

                            // Spacing between legend + text
                            padding: 14,

                            // Optional: use point style instead of box
                            usePointStyle: false
                        }
                    },

                    // --------------------------------------------------
                    // 🧲 TOOLTIP
                    // --------------------------------------------------
                    tooltip: {
                        callbacks: {

                            // Exact date
                            title: (items) => {
                                const x = items?.[0]?.parsed?.x;
                                if(!x) return '';
                                return formatISODateUTC(new Date(x));
                            },

                            // Monetary Ratio Label
                            label: (ctx) => {

                                const v = ctx.parsed?.y;
                                if(v === null || v === undefined) return '';

                                // Whole number rounding
                                const rounded = Math.round(v);

                                // Thousands separator
                                const formatted = rounded.toLocaleString('en-US');

                                // Fiat code (USD, EUR, etc.)
                                const fiatKey =
                                    reviewChartState.fiat?.toUpperCase() || '';

                                return `1 Bitcoin = ${formatted} ${fiatKey}`;
                            }
                        }
                    },

                    // --------------------------------------------------
                    // 🔍 ZOOM
                    // --------------------------------------------------
                    zoom: {
                        pan: { enabled: true, mode: 'x' },
                        zoom: { wheel: { enabled: true }, mode: 'x' }
                    }
                },


                scales: {
                    x: {
                        type: 'time',
                        time: { unit: timeCfg.unit },
                        ticks: {
                            autoSkip: true,
                            maxTicksLimit: 12,
                            callback: timeCfg.callback
                        }
                    },
                    y: {
                        type: reviewChartState.logScale
                            ? 'logarithmic'
                            : 'linear'
                    }
                }
            }
        });

        // Initial fit
        fitTimeDomain(chartInstance, data);
        chartInstance.update('none');

        // --------------------------------------------------
        // 🖱️ Double-Click Reset (LIVE DATA FIX)
        // --------------------------------------------------
        canvas.addEventListener('dblclick', () => {

            if(!chartInstance) return;

            const raw = dataCache[reviewChartState.fiat];
            if(!raw) return;

            const filtered = filterData(raw);
            if(!filtered.length) return;

            chartInstance.resetZoom?.();
            fitTimeDomain(chartInstance, filtered);
            chartInstance.update('none');
        });

    // --------------------------------------------------
    // 🔄 Chart Update
    // --------------------------------------------------
    } else {

        chartInstance.data.datasets[0] = dataset;

        chartInstance.options.scales.y.type =
            reviewChartState.logScale
                ? 'logarithmic'
                : 'linear';

        chartInstance.options.scales.x.time.unit =
            timeCfg.unit;

        chartInstance.options.scales.x.ticks.callback =
            timeCfg.callback;

        fitTimeDomain(chartInstance, data);
        chartInstance.update('none');
    }
}

// --------------------------------------------------
// 🎛️ Controls
// --------------------------------------------------
function bindControls(){

    const fiatSelect      = document.getElementById('review-fiat-select');
    const timeModeSelect  = document.getElementById('review-time-mode-select');
    const yearSelect      = document.getElementById('review-year-select');
    const halvingSelect   = document.getElementById('review-halving-select');
    const logToggle       = document.getElementById('review-log-scale-toggle');

    const yearControl     = document.getElementById('review-year-control');
    const halvingControl  = document.getElementById('review-halving-control');

    // 🆕 Custom controls
    const customControl   = document.getElementById('review-custom-control');
    const dateStartInput  = document.getElementById('review-date-start');
    const dateEndInput    = document.getElementById('review-date-end');

    if(fiatSelect){
        fiatSelect.onchange = (e) => {
            reviewChartState.fiat = e.target.value;
            updateChart();
        };
    }

    if(timeModeSelect){
        timeModeSelect.onchange = (e) => {

            reviewChartState.timeMode = e.target.value;

            // Reset unrelated state
            if(reviewChartState.timeMode !== 'year')
                reviewChartState.year = null;

            if(reviewChartState.timeMode !== 'halving')
                reviewChartState.halvingCycle = null;

            if(reviewChartState.timeMode !== 'custom'){
                reviewChartState.customStart = null;
                reviewChartState.customEnd   = null;

                // Also clear UI inputs if present
                if(dateStartInput) dateStartInput.value = '';
                if(dateEndInput)   dateEndInput.value = '';
            }

            // Toggle controls
            if(yearControl){
                yearControl.classList.toggle(
                    'review-control-hidden',
                    e.target.value !== 'year'
                );
            }

            if(halvingControl){
                halvingControl.classList.toggle(
                    'review-control-hidden',
                    e.target.value !== 'halving'
                );
            }

            if(customControl){
                customControl.classList.toggle(
                    'review-control-hidden',
                    e.target.value !== 'custom'
                );
            }

            updateChart();
        };
    }

    if(yearSelect){
        yearSelect.onchange = (e) => {
            reviewChartState.year = Number(e.target.value);
            updateChart();
        };
    }

    if(halvingSelect){
        halvingSelect.onchange = (e) => {
            reviewChartState.halvingCycle = e.target.value;
            updateChart();
        };
    }

    if(logToggle){
        logToggle.onchange = (e) => {
            reviewChartState.logScale = e.target.checked;
            updateChart();
        };
    }

    // 🆕 Custom date inputs
    if(dateStartInput){
        dateStartInput.addEventListener('change', (e) => {
            reviewChartState.customStart = e.target.value || null;
            updateChart();
        });
    }

    if(dateEndInput){
        dateEndInput.addEventListener('change', (e) => {
            reviewChartState.customEnd = e.target.value || null;
            updateChart();
        });
    }
}

// --------------------------------------------------
// 🗓️ Populate Years (unchanged, based on USD history)
// --------------------------------------------------
function populateYearSelect(){

    const raw = dataCache.usd;
    if(!raw?.length) return;

    const years = [
        ...new Set(raw.map(p => p.x.getUTCFullYear()))
    ].sort((a,b) => b-a);

    const select = document.getElementById('review-year-select');
    if(!select) return;

    select.innerHTML = years
        .map(y => `<option value="${y}">${y}</option>`)
        .join('');
}

// --------------------------------------------------
// 🚀 Loader
// --------------------------------------------------
async function loadReviewBtcVsFiat(){

    if(!initialized){

        await ensureDataLoaded();
        populateYearSelect();
        bindControls();

        initialized = true;
    }

    updateChart();
}

window.loadReviewBtcVsFiat = loadReviewBtcVsFiat;

})();
