(() => {

/*
====================================================================
REVIEW_BTC_VS_FIAT.js
FINAL CLEAN VERSION (Line Only + Adaptive Time Axis)

Features:
✔ Line Chart only (Macro focus)
✔ Correct auto-fit
✔ Double-click zoom reset
✔ Adaptive X-Axis labeling
✔ Log / Linear toggle
✔ Minimal invasive
====================================================================
*/

// --------------------------------------------------
// 🧠 State
// --------------------------------------------------
const reviewChartState = {
    fiat: 'usd',
    timeMode: 'all',
    year: null,
    halvingCycle: null,
    logScale: true
};

// --------------------------------------------------
// 📦 Cache
// --------------------------------------------------
const dataCache = {
    usd: null,
    eur: null,
    jpy: null
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
// 🧭 Adaptive Time Scale Config
// --------------------------------------------------
function getTimeScaleConfig(){

    if(reviewChartState.timeMode === 'all'){
        return {
            unit: 'quarter',
            callback:(value)=>{
                const d = new Date(value);
                const q = Math.floor(d.getUTCMonth()/3)+1;
                return `Q${q} ${d.getUTCFullYear()}`;
            }
        };
    }

    if(reviewChartState.timeMode === 'year'){
        return {
            unit:'month',
            callback:(value)=>{
                const d = new Date(value);
                return d.toLocaleString(
                    'en-US',
                    {month:'short'}
                );
            }
        };
    }

    if(reviewChartState.timeMode === 'halving'){
        return {
            unit:'month',
            callback:(value)=>{
                const d = new Date(value);
                return d.toLocaleString(
                    'en-US',
                    {month:'short',year:'2-digit'}
                );
            }
        };
    }

    return {};
}

// --------------------------------------------------
// 🔄 JSONL Loader
// --------------------------------------------------
async function loadJSONL(path){

    const res = await fetch(path);
    if(!res.ok) throw new Error(path);

    return (await res.text())
        .trim()
        .split('\n')
        .map(l => JSON.parse(l))
        .map(p => ({
            x: new Date(p.date),
            y: p.c
        }));
}

// --------------------------------------------------
async function ensureDataLoaded(){

    if(!dataCache.usd)
        dataCache.usd =
            await loadJSONL(
                '/data/review/bitcoin_value/btc_vs_fiat/usd/btc_vs_usd_all.jsonl'
            );

    if(!dataCache.eur)
        dataCache.eur =
            await loadJSONL(
                '/data/review/bitcoin_value/btc_vs_fiat/eur/btc_vs_eur_all.jsonl'
            );

    if(!dataCache.jpy)
        dataCache.jpy =
            await loadJSONL(
                '/data/review/bitcoin_value/btc_vs_fiat/jpy/btc_vs_jpy_all.jsonl'
            );
}

// --------------------------------------------------
// 🔍 Filter Logic
// --------------------------------------------------
function filterData(raw){

    let out = raw;

    if(reviewChartState.timeMode==='year'
       && reviewChartState.year){

        out = out.filter(p =>
            p.x.getUTCFullYear() ===
            reviewChartState.year
        );
    }

    if(reviewChartState.timeMode==='halving'
       && reviewChartState.halvingCycle){

        const [s,e] =
            HALVING_RANGES[
                reviewChartState.halvingCycle
            ];

        const start = new Date(s);
        const end   = e ? new Date(e) : null;

        out = out.filter(p =>
            p.x >= start &&
            (!end || p.x <= end)
        );
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

    const perf =
        ((end - start) / start) * 100;

    return perf;
}

// --------------------------------------------------
// 🖊️ Performance Overlay Plugin
// --------------------------------------------------
const performanceOverlayPlugin = {

    id: 'performanceOverlay',

    afterDraw(chart){

        const ctx = chart.ctx;

        const raw =
            dataCache[reviewChartState.fiat];

        if(!raw) return;

        const filtered =
            filterData(raw);

        if(!filtered.length) return;

        const perf =
            calculatePerformance(filtered);

        if(perf === null) return;

        const sign =
            perf >= 0 ? '+' : '';

        const text =
            `Performance: ${sign}${perf.toFixed(1)} %`;

        ctx.save();

        ctx.font =
            '600 14px Inter, system-ui';

        ctx.fillStyle =
            perf >= 0
                ? '#16c784'
                : '#ea3943';

        ctx.textAlign = 'left';

        ctx.fillText(
            text,
            chart.chartArea.left + 10,
            chart.chartArea.top + 20
        );

        ctx.restore();
    }
};



// --------------------------------------------------
// 🧭 Auto-Fit
// --------------------------------------------------
function fitTimeDomain(chart,data){

    if(!chart || !data?.length) return;

    chart.options.scales.x.min = data[0].x;
    chart.options.scales.x.max = data.at(-1).x;
}

// --------------------------------------------------
// 📊 Dataset Builder (Line Only)
// --------------------------------------------------
function buildDataset(data){

    // --------------------------------------------------
    // 🏷️ Dynamic Fiat Label
    // --------------------------------------------------
    const fiatLabel =
        reviewChartState.fiat.toUpperCase();

    // --------------------------------------------------
    // 🧾 Legend Label
    // BTC • USD / EUR / JPY
    // --------------------------------------------------
    const legendLabel =
        `BTC • ${fiatLabel}`;

    return {
        type:'line',
        label: legendLabel,
        data,
        borderWidth:1.6,
        tension:0.15,
        fill:false,
        pointRadius:0
    };
}


// --------------------------------------------------
// 📊 Render
// --------------------------------------------------
function updateChart(){

    const canvas =
        document.getElementById(
            'REVIEW_BTC_VS_FIAT_CANVAS'
        );
    if(!canvas) return;

    const ctx  = canvas.getContext('2d');
    const raw  = dataCache[reviewChartState.fiat];
    if(!raw) return;

    const data    = filterData(raw);
    if(!data.length) return;

    const dataset = buildDataset(data);
    const timeCfg = getTimeScaleConfig();

    // --------------------------------------------------
    // 🆕 Chart Creation
    // --------------------------------------------------
    if(!chartInstance){

        chartInstance = new Chart(ctx,{

            // --------------------------------------------------
            // 🧩 Plugin Registrierung
            // --------------------------------------------------
            plugins:[
                performanceOverlayPlugin
            ],

            data:{
                datasets:[dataset]
            },

            options:{
                responsive:true,
                maintainAspectRatio:false,

                plugins:{
                    legend:{position:'top'},
                    zoom:{
                        pan:{enabled:true,mode:'x'},
                        zoom:{wheel:{enabled:true},mode:'x'}
                    }
                },

                scales:{
                    x:{
                        type:'time',
                        time:{ unit: timeCfg.unit },
                        ticks:{
                            autoSkip:true,
                            maxTicksLimit:12,
                            callback: timeCfg.callback
                        }
                    },
                    y:{
                        type: reviewChartState.logScale
                            ? 'logarithmic'
                            : 'linear'
                    }
                }
            }
        });

        // --------------------------------------------------
        // 🧭 Initial Domain Fit
        // --------------------------------------------------
        fitTimeDomain(chartInstance,data);
        chartInstance.update('none');

        // --------------------------------------------------
        // 🖱️ Double-Click Reset (LIVE DATA FIX)
        // --------------------------------------------------
        canvas.addEventListener(
            'dblclick',
            ()=>{

                if(!chartInstance) return;

                // 🔄 Always recompute filtered dataset
                const raw =
                    dataCache[
                        reviewChartState.fiat
                    ];

                if(!raw) return;

                const filtered =
                    filterData(raw);

                if(!filtered.length) return;

                // 🔄 Reset zoom
                chartInstance.resetZoom?.();

                // 🎯 Re-fit correct domain
                fitTimeDomain(
                    chartInstance,
                    filtered
                );

                chartInstance.update('none');
            }
        );

    // --------------------------------------------------
    // 🔄 Chart Update
    // --------------------------------------------------
    } else {

        chartInstance.data.datasets[0] =
            dataset;

        chartInstance.options.scales.y.type =
            reviewChartState.logScale
                ? 'logarithmic'
                : 'linear';

        chartInstance.options.scales.x.time.unit =
            timeCfg.unit;

        chartInstance.options.scales.x.ticks.callback =
            timeCfg.callback;

        fitTimeDomain(chartInstance,data);
        chartInstance.update('none');
    }
}


// --------------------------------------------------
// 🎛️ Controls
// --------------------------------------------------
function bindControls(){

    document
    .getElementById('review-fiat-select')
    .onchange = e=>{
        reviewChartState.fiat = e.target.value;
        updateChart();
    };

    document
    .getElementById('review-time-mode-select')
    .onchange = e=>{

        reviewChartState.timeMode =
            e.target.value;

        if(reviewChartState.timeMode!=='year')
            reviewChartState.year=null;

        if(reviewChartState.timeMode!=='halving')
            reviewChartState.halvingCycle=null;

        document
        .getElementById('review-year-control')
        .classList.toggle(
            'review-control-hidden',
            e.target.value!=='year'
        );

        document
        .getElementById('review-halving-control')
        .classList.toggle(
            'review-control-hidden',
            e.target.value!=='halving'
        );

        updateChart();
    };

    document
    .getElementById('review-year-select')
    .onchange = e=>{
        reviewChartState.year =
            Number(e.target.value);
        updateChart();
    };

    document
    .getElementById('review-halving-select')
    .onchange = e=>{
        reviewChartState.halvingCycle =
            e.target.value;
        updateChart();
    };

    document
    .getElementById('review-log-scale-toggle')
    .onchange = e=>{
        reviewChartState.logScale =
            e.target.checked;
        updateChart();
    };
}

// --------------------------------------------------
// 🗓️ Populate Years
// --------------------------------------------------
function populateYearSelect(){

    const years=[
        ...new Set(
            dataCache.usd.map(
                p=>p.x.getUTCFullYear()
            )
        )
    ].sort((a,b)=>b-a);

    const select =
        document.getElementById(
            'review-year-select'
        );

    select.innerHTML =
        years.map(y=>
            `<option value="${y}">${y}</option>`
        ).join('');
}

// --------------------------------------------------
// 🚀 Loader
// --------------------------------------------------
async function loadReviewBtcVsFiat(){

    if(!initialized){

        await ensureDataLoaded();
        populateYearSelect();
        bindControls();

        initialized=true;
    }

    updateChart();
}

window.loadReviewBtcVsFiat =
    loadReviewBtcVsFiat;

})();
