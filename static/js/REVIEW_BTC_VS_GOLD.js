(() => {

/*
====================================================================
REVIEW_BTC_VS_GOLD.js
Bitcoin vs Gold (troy ounce)

Template Clone of BTC vs Fiat Leaf
✔ All Time Modes
✔ Halving Cycles
✔ Custom Range
✔ Log / Linear
✔ Performance Overlay
✔ Zoom + Reset
✔ Adaptive Time Scale
✔ Gold Styling
====================================================================
*/

// --------------------------------------------------
// 🧠 State
// --------------------------------------------------
const reviewGoldState = {

    timeMode: 'all',   // all | year | halving | custom
    year: null,
    halvingCycle: null,
    logScale: true,

    customStart: null,
    customEnd: null
};

// --------------------------------------------------
// 📦 Cache
// --------------------------------------------------
let goldData   = null;
let chart      = null;
let initialized = false;

// --------------------------------------------------
// 🗓️ Halving Ranges
// --------------------------------------------------
const HALVING_RANGES = {
    '2009_2012':['2009-01-03','2012-11-28'],
    '2012_2016':['2012-11-28','2016-07-09'],
    '2016_2020':['2016-07-09','2020-05-11'],
    '2020_2024':['2020-05-11','2024-04-20'],
    '2024_now':['2024-04-20',null]
};

// --------------------------------------------------
// 🧮 Helpers
// --------------------------------------------------
function clampDateOrder(start,end){
    if(!start || !end) return {start,end};
    if(start > end) return {start:end,end:start};
    return {start,end};
}

function diffYears(start,end){
    return (end-start)/(1000*60*60*24*365);
}

function formatISODateUTC(d){
    return `${d.getUTCFullYear()}-${
        String(d.getUTCMonth()+1).padStart(2,'0')
    }-${
        String(d.getUTCDate()).padStart(2,'0')
    }`;
}

// --------------------------------------------------
// 🧭 Adaptive Time Scale
// --------------------------------------------------
function getTimeScaleConfig(data){

    if(!data?.length){
        return {unit:'month'};
    }

    const start = data[0].x;
    const end   = data.at(-1).x;
    const years = diffYears(start,end);

    if(reviewGoldState.timeMode === 'all'){
        return {
            unit:'quarter',
            callback:v=>{
                const d=new Date(v);
                const q=Math.floor(d.getUTCMonth()/3)+1;
                return `Q${q} ${d.getUTCFullYear()}`;
            }
        };
    }

    if(reviewGoldState.timeMode === 'year'){
        return {
            unit:'month',
            callback:v=>{
                const d=new Date(v);
                return d.toLocaleString(
                    'en-US',{month:'short'}
                );
            }
        };
    }

    if(reviewGoldState.timeMode === 'halving'){
        return {
            unit:'month',
            callback:v=>{
                const d=new Date(v);
                return d.toLocaleString(
                    'en-US',
                    {month:'short',year:'2-digit'}
                );
            }
        };
    }

    if(reviewGoldState.timeMode === 'custom'){

        if(years < 1){
            return {
                unit:'month',
                callback:v=>{
                    const d=new Date(v);
                    return d.toLocaleString(
                        'en-US',{month:'short'}
                    );
                }
            };
        }

        if(years < 5){
            return {
                unit:'quarter',
                callback:v=>{
                    const d=new Date(v);
                    const q=Math.floor(
                        d.getUTCMonth()/3
                    )+1;
                    const yy=String(
                        d.getUTCFullYear()
                    ).slice(-2);
                    return `Q${q} '${yy}`;
                }
            };
        }

        return {
            unit:'year',
            callback:v=>{
                const d=new Date(v);
                return String(
                    d.getUTCFullYear()
                );
            }
        };
    }

    return {unit:'month'};
}

// --------------------------------------------------
// 📥 Loader
// --------------------------------------------------
async function loadJSONL(path){

    const res = await fetch(path);
    if(!res.ok) throw new Error(path);

    const txt = (await res.text()).trim();

    return txt.split('\n')
        .map(l=>JSON.parse(l))
        .map(p=>({
            x:new Date(p.date),
            y:p.value
        }));
}

async function ensureLoaded(){

    if(!goldData){
        goldData = await loadJSONL(
            '/data/review/btc_vs_gold/btc_vs_gold_all.jsonl'
        );
    }
}

// --------------------------------------------------
// 🔍 Filter
// --------------------------------------------------
function filterData(){

    let out = goldData;

    if(reviewGoldState.timeMode==='year'
       && reviewGoldState.year){

        out = out.filter(p =>
            p.x.getUTCFullYear()
            === reviewGoldState.year
        );
    }

    if(reviewGoldState.timeMode==='halving'
       && reviewGoldState.halvingCycle){

        const [s,e] =
            HALVING_RANGES[
                reviewGoldState.halvingCycle
            ];

        const start=new Date(s);
        const end=e?new Date(e):null;

        out = out.filter(p =>
            p.x>=start && (!end || p.x<=end)
        );
    }

    if(reviewGoldState.timeMode==='custom'
       && reviewGoldState.customStart
       && reviewGoldState.customEnd){

        let start=new Date(
            reviewGoldState.customStart
        );

        let end=new Date(
            reviewGoldState.customEnd
        );

        ({start,end} =
            clampDateOrder(start,end));

        end=new Date(
            end.getTime()
            +86400000-1
        );

        out = out.filter(p =>
            p.x>=start && p.x<=end
        );
    }

    return out;
}

// --------------------------------------------------
// 📈 Performance
// --------------------------------------------------
function calculatePerformance(data){

    if(!data?.length) return null;

    const s=data[0].y;
    const e=data.at(-1).y;

    return ((e-s)/s)*100;
}

// --------------------------------------------------
// 🖊️ Overlay
// --------------------------------------------------
const performanceOverlayPlugin={

    id:'performanceOverlay',

    afterDraw(chart){

        const data=filterData();
        if(!data?.length) return;

        const perf=
            calculatePerformance(data);
        if(perf===null) return;

        const ctx=chart.ctx;

        const sign=perf>=0?'+':'';
        const text=
            `Performance: ${sign}${perf.toFixed(1)} %`;

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
// 📊 Dataset
// --------------------------------------------------
function buildDataset(data){

    return {
        type:'line',
        label:'Bitcoin  •  Gold (oz)',
        data,

        borderColor:'#f2c94c',
        backgroundColor:'#f2c94c',

        borderWidth:1.8,
        tension:0.15,
        pointRadius:0,
        pointHitRadius:30
    };
}

// --------------------------------------------------
// 📈 Render
// --------------------------------------------------
function updateChart(){

    const canvas=
        document.getElementById(
            'REVIEW_BTC_VS_GOLD_CANVAS'
        );

    if(!canvas || !goldData) return;

    const ctx=canvas.getContext('2d');
    const data=filterData();
    if(!data.length) return;

    const ds=buildDataset(data);
    const timeCfg=
        getTimeScaleConfig(data);

    if(!chart){

        chart=new Chart(ctx,{

            plugins:[
                performanceOverlayPlugin
            ],

            data:{datasets:[ds]},

            options:{
                responsive:true,
                maintainAspectRatio:false,

                interaction:{
                    mode:'index',
                    intersect:false
                },

                
                plugins:{

                    // --------------------------------------------------
                    // 🏷️ LEGEND (Fiat parity)
                    // --------------------------------------------------
                    legend: {
                        position: 'top',

                        labels: {
                            font: {
                                size: 16,
                                weight: '300',
                                family: 'Inter, system-ui'
                            },

                            color: 'rgba(242, 201, 76, 0.9)',

                            boxWidth: 18,
                            boxHeight: 2,
                            padding: 14,
                            usePointStyle: false
                        }
                    },

                    // --------------------------------------------------
                    // 🧲 TOOLTIP (unverändert)
                    // --------------------------------------------------
                    tooltip:{
                        callbacks:{
                            title:i=>
                                formatISODateUTC(
                                    new Date(i[0].parsed.x)
                                ),

                            label:c=>{

                                const v = c.parsed?.y;
                                if(v===null||v===undefined) return '';

                                return `1 Bitcoin = ${v.toFixed(2)} Gold (oz)`;
                            }
                        }
                    },

                    // --------------------------------------------------
                    // 🔍 ZOOM (unverändert)
                    // --------------------------------------------------
                    zoom:{
                        pan:{enabled:true,mode:'x'},
                        zoom:{
                            wheel:{enabled:true},
                            mode:'x'
                        }
                    }
                },


                scales:{
                    x:{
                        type:'time',
                        time:{unit:timeCfg.unit},
                        ticks:{
                            callback:
                                timeCfg.callback
                        }
                    },

                    y:{
                        type:
                            reviewGoldState.logScale
                            ?'logarithmic'
                            :'linear'
                    }
                }
            }
        });

        canvas.addEventListener(
            'dblclick',
            ()=>{
                chart.resetZoom?.();
                chart.update('none');
            }
        );

    } else {

        chart.data.datasets[0]=ds;

        chart.options.scales.y.type=
            reviewGoldState.logScale
            ?'logarithmic'
            :'linear';

        chart.update('none');
    }
}

// --------------------------------------------------
// 🎛️ Controls
// --------------------------------------------------
function bindControls(){

    const timeSel=
        document.getElementById(
            'review-gold-time-mode-select'
        );

    const yearSel=
        document.getElementById(
            'review-gold-year-select'
        );

    const halvingSel=
        document.getElementById(
            'review-gold-halving-select'
        );

    const logToggle=
        document.getElementById(
            'review-gold-log-toggle'
        );

    const yearCtrl=
        document.getElementById(
            'review-gold-year-control'
        );

    const halvCtrl=
        document.getElementById(
            'review-gold-halving-control'
        );

    const custCtrl=
        document.getElementById(
            'review-gold-custom-control'
        );

    const startInput=
        document.getElementById(
            'review-gold-date-start'
        );

    const endInput=
        document.getElementById(
            'review-gold-date-end'
        );

    if(timeSel){

        timeSel.onchange=e=>{

            reviewGoldState.timeMode=
                e.target.value;

            yearCtrl?.classList.toggle(
                'review-control-hidden',
                e.target.value!=='year'
            );

            halvCtrl?.classList.toggle(
                'review-control-hidden',
                e.target.value!=='halving'
            );

            custCtrl?.classList.toggle(
                'review-control-hidden',
                e.target.value!=='custom'
            );

            updateChart();
        };
    }

    if(yearSel){
        yearSel.onchange=e=>{
            reviewGoldState.year=
                Number(e.target.value);
            updateChart();
        };
    }

    if(halvingSel){
        halvingSel.onchange=e=>{
            reviewGoldState.halvingCycle=
                e.target.value;
            updateChart();
        };
    }

    if(logToggle){
        logToggle.onchange=e=>{
            reviewGoldState.logScale=
                e.target.checked;
            updateChart();
        };
    }

    if(startInput){
        startInput.onchange=e=>{
            reviewGoldState.customStart=
                e.target.value;
            updateChart();
        };
    }

    if(endInput){
        endInput.onchange=e=>{
            reviewGoldState.customEnd=
                e.target.value;
            updateChart();
        };
    }
}

// --------------------------------------------------
// 🗓️ Year Populate
// --------------------------------------------------
function populateYearSelect(){

    if(!goldData?.length) return;

    const years=[
        ...new Set(
            goldData.map(p=>
                p.x.getUTCFullYear()
            )
        )
    ].sort((a,b)=>b-a);

    const sel=
        document.getElementById(
            'review-gold-year-select'
        );

    if(!sel) return;

    sel.innerHTML=years
        .map(y=>
            `<option value="${y}">${y}</option>`
        )
        .join('');
}

// --------------------------------------------------
// 🚀 Loader
// --------------------------------------------------
async function loadReviewBtcVsGold(){

    if(!initialized){

        await ensureLoaded();
        populateYearSelect();
        bindControls();

        initialized=true;
    }

    updateChart();
}

window.loadReviewBtcVsGold=
    loadReviewBtcVsGold;

})();
