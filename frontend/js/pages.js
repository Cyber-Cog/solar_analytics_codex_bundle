// frontend/js/pages.js  - UAT-fixed version
// Fixed: File upload in Metadata, auth-safe template download,
//        onboarding banners on Dashboard & Analytics, better empty states
const { useState, useEffect, useCallback } = React;
const { BarChart, Bar, LineChart, Line, ComposedChart, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } = Recharts;

// ── Shared upload helper (multipart/form-data with JWT) ───────────────────────
async function uploadExcel(endpoint, file) {
  const form = new FormData();
  form.append('file', file);
  const resp = await fetch((window.SolarAPI && window.SolarAPI.apiBase || 'http://localhost:8081') + endpoint, {
    method: 'POST',
    headers: { Authorization: `Bearer ${window.SolarAPI.getToken()}` },
    body: form,
  });
  const json = await resp.json();
  if (!resp.ok) throw new Error(json.detail || 'Upload failed');
  return json;
}

// ── Auth-safe template download via blob ──────────────────────────────────────
async function downloadTemplate(endpoint, filename) {
  try {
    const resp = await fetch((window.SolarAPI && window.SolarAPI.apiBase || 'http://localhost:8081') + endpoint, {
      headers: { Authorization: `Bearer ${window.SolarAPI.getToken()}` },
    });
    if (!resp.ok) { 
      const err = await resp.text();
      alert(`Download failed: ${err}`); 
      return; 
    }
    const blob = await resp.blob();
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href = url; a.download = filename; 
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  } catch(e) {
    alert("Error downloading template: " + e.message);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// AUTH PAGE
// ═══════════════════════════════════════════════════════════════════════════════
window.AuthPage = ({ onLogin }) => {
  const [mode, setMode]   = useState('login');
  const [email, setEmail] = useState('');
  const [name, setName]   = useState('');
  const [pass, setPass]   = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const h = React.createElement;

  const submit = async (e) => {
    e.preventDefault(); setError(''); setLoading(true);
    try {
      const res = mode === 'login'
        ? await window.SolarAPI.Auth.login(email, pass)
        : await window.SolarAPI.Auth.signup(email, name, pass);
      window.SolarAPI.setToken(res.access_token);
      window.SolarAPI.setUser(res.user);
      onLogin(res.user);
    } catch(e) { setError(e.message); }
    finally { setLoading(false); }
  };

  const SunIcon = h('svg', { width: 28, height: 28, viewBox: '0 0 24 24', fill: 'none', stroke: 'rgba(9,17,26,0.85)', strokeWidth: 1.8, strokeLinecap: 'round', strokeLinejoin: 'round' },
    h('circle', { cx: 12, cy: 12, r: 5 }),
    h('line', { x1: 12, y1: 1, x2: 12, y2: 3 }), h('line', { x1: 12, y1: 21, x2: 12, y2: 23 }),
    h('line', { x1: 4.22, y1: 4.22, x2: 5.64, y2: 5.64 }), h('line', { x1: 18.36, y1: 18.36, x2: 19.78, y2: 19.78 }),
    h('line', { x1: 1, y1: 12, x2: 3, y2: 12 }), h('line', { x1: 21, y1: 12, x2: 23, y2: 12 }),
    h('line', { x1: 4.22, y1: 19.78, x2: 5.64, y2: 18.36 }), h('line', { x1: 18.36, y1: 5.64, x2: 19.78, y2: 4.22 })
  );

  return h('div', { className:'auth-page' },
    h('div', { className:'auth-bg-overlay' }),
    h('div', { className:'auth-bg-orb auth-bg-orb-1' }),
    h('div', { className:'auth-bg-orb auth-bg-orb-2' }),
    h('div', { className:'auth-page-inner' },
      h('div', { className:'auth-card-wrap' },
        h('div', { className:'auth-card' },
          h('div', { className:'auth-logo' },
            h('div', { className:'logo-circle' }, SunIcon),
            h('h1', null, 'Photon Intelligence Centre'),
            h('p', null, 'Sign in to continue'),
          ),
          error && h('div', { className:'auth-error' }, error),
          h('form', { className:'auth-form', onSubmit: submit },
            mode === 'signup' && h('div', { className:'form-group' },
              h('label', { className:'form-label' }, 'Full Name'),
              h('input', { className:'form-input', value:name, onChange:e=>setName(e.target.value), placeholder:'Your name', required:true }),
            ),
            h('div', { className:'form-group' },
              h('label', { className:'form-label' }, 'Email'),
              h('input', { className:'form-input', type:'email', value:email, onChange:e=>setEmail(e.target.value), placeholder:'you@company.com', required:true, autoComplete:'username' }),
            ),
            h('div', { className:'form-group' },
              h('label', { className:'form-label' }, 'Password'),
              h('input', { className:'form-input', type:'password', value:pass, onChange:e=>setPass(e.target.value), placeholder:'Password', required:true, autoComplete: mode==='login' ? 'current-password' : 'new-password' }),
            ),
            h('button', { className:'btn btn-primary', type:'submit', disabled:loading, style:{width:'100%',justifyContent:'center',height:42} },
              loading ? h(Spinner) : (mode==='login' ? 'Sign In' : 'Create Account'),
            ),
          ),
          h('div', { className:'auth-switch' },
            mode === 'login'
              ? h('span', null, "Don't have an account? ", h('a', { onClick:()=>setMode('signup') }, 'Sign up'))
              : h('span', null, "Already have an account? ", h('a', { onClick:()=>setMode('login') }, 'Sign in')),
          ),
        ),
      ),
    ),
  );
};

// ═══════════════════════════════════════════════════════════════════════════════
// DASHBOARD PAGE
// ═══════════════════════════════════════════════════════════════════════════════
window.DashboardPage = ({ plantId, dateFrom, dateTo, onNavigate }) => {
  const h = React.createElement;
  const [station, setStation]   = useState(null);
  const [kpis, setKpis]         = useState(null);
  const [wms, setWms]           = useState(null);
  const [energy, setEnergy]     = useState([]);
  const [invTable, setInvTable] = useState([]);
  const [invViewMode, setInvViewMode] = useState('table'); // table or heatmap
  const [powerGti, setPowerGti] = useState([]);
  const [noData, setNoData]     = useState(false);
  const [targetGeneration, setTargetGeneration] = useState(null);

  const loadDashboardData = useCallback((currentDateFrom, currentDateTo) => {
    if (!plantId) return () => {};
    let ignore = false;
    setNoData(false);
    window.SolarAPI.Dashboard.bundle(plantId, currentDateFrom, currentDateTo)
      .then((b) => {
        if (ignore) return;
        setStation(b.station || null);
        setKpis(b.kpis || null);
        setWms(b.wms || null);
        setEnergy(Array.isArray(b.energy) ? b.energy : []);
        setInvTable(Array.isArray(b.inverter_performance) ? b.inverter_performance : []);
        setPowerGti(Array.isArray(b.power_vs_gti) ? b.power_vs_gti : []);
        setTargetGeneration(b.target_generation && typeof b.target_generation === 'object' ? b.target_generation : null);
        setNoData(
          (Array.isArray(b.power_vs_gti) ? b.power_vs_gti.length : 0) === 0 &&
          (Array.isArray(b.energy) ? b.energy.length : 0) === 0
        );
      })
      .catch(() => {
        if (ignore) return;
        Promise.all([
          window.SolarAPI.Dashboard.stationDetails(plantId).catch(() => null),
          window.SolarAPI.Dashboard.kpis(plantId, currentDateFrom, currentDateTo).catch(() => null),
          window.SolarAPI.Dashboard.wmsKpis(plantId, currentDateFrom, currentDateTo).catch(() => null),
          window.SolarAPI.Dashboard.energy(plantId, currentDateFrom, currentDateTo).catch(() => []),
          window.SolarAPI.Dashboard.inverterPerf(plantId, currentDateFrom, currentDateTo).catch(() => []),
          window.SolarAPI.Dashboard.powerVsGti(plantId, currentDateFrom, currentDateTo).catch(() => []),
        ]).then(([station, kpis, wms, energyRes, invTableRes, powerGtiRes]) => {
          if (ignore) return;
          setStation(station || null);
          setKpis(kpis || null);
          setWms(wms || null);
          setEnergy(Array.isArray(energyRes) ? energyRes : []);
          setInvTable(Array.isArray(invTableRes) ? invTableRes : []);
          setPowerGti(Array.isArray(powerGtiRes) ? powerGtiRes : []);
          setTargetGeneration(null);
          setNoData(
            (Array.isArray(powerGtiRes) ? powerGtiRes.length : 0) === 0 &&
            (Array.isArray(energyRes) ? energyRes.length : 0) === 0
          );
        });
      });
    return () => { ignore = true; };
  }, [plantId]);

  useEffect(() => {
    const cleanup = loadDashboardData(dateFrom, dateTo);
    return cleanup;
  }, [loadDashboardData, dateFrom, dateTo]);

  const fmtNum = (v, d=1) => v != null ? Number(v).toFixed(d) : '-';
  /** Prefer energy_export_kwh / net_generation_kwh as source of truth so MWh is never confused with kWh. */
  const formatEnergyKpi = (kwhField, mwhField) => {
    const kwh = kwhField != null && kwhField !== '' ? Number(kwhField) : null;
    if (kwh != null && !Number.isNaN(kwh)) {
      if (Math.abs(kwh) < 1000) return { value: fmtNum(kwh, 2), unit: 'kWh' };
      return { value: fmtNum(kwh / 1000, 2), unit: 'MWh' };
    }
    const mwh = mwhField != null && mwhField !== '' ? Number(mwhField) : null;
    if (mwh != null && !Number.isNaN(mwh)) return { value: fmtNum(mwh, 2), unit: 'MWh' };
    return { value: '-', unit: 'MWh' };
  };
  const energyExportKpi = formatEnergyKpi(kpis?.energy_export_kwh, kpis?.energy_export_mwh);
  const netGenKpi = formatEnergyKpi(kpis?.net_generation_kwh, kpis?.net_generation_mwh);
  const invColumns = [
    {
      key: 'inverter_id',
      label: 'Inverter',
      render: (row) => h('strong', null, row.inverter_id),
      csvValue: (row) => row.inverter_id,
    },
    {
      key: 'generation_kwh',
      label: 'Generation (kWh)',
      sortValue: (row) => row.generation_kwh ?? -Infinity,
      render: (row) => fmtNum(row.generation_kwh, 2),
      csvValue: (row) => fmtNum(row.generation_kwh, 2),
    },
    {
      key: 'dc_capacity_kwp',
      label: 'DC Capacity (kWp)',
      sortValue: (row) => row.dc_capacity_kwp ?? -Infinity,
      render: (row) => fmtNum(row.dc_capacity_kwp, 2),
      csvValue: (row) => fmtNum(row.dc_capacity_kwp, 2),
    },
    {
      key: 'yield_kwh_kwp',
      label: 'Yield (kWh/kWp)',
      sortValue: (row) => row.yield_kwh_kwp ?? -Infinity,
      render: (row) => fmtNum(row.yield_kwh_kwp, 2),
      csvValue: (row) => fmtNum(row.yield_kwh_kwp, 2),
    },
    {
      key: 'efficiency_pct',
      label: 'Eff.',
      sortValue: (row) => row.efficiency_pct ?? -Infinity,
      render: (row) => row.efficiency_pct != null
        ? h(Badge, { type: row.efficiency_pct >= 95 ? 'green' : 'amber' }, `${fmtNum(row.efficiency_pct)}%`)
        : '-',
      csvValue: (row) => row.efficiency_pct != null ? `${fmtNum(row.efficiency_pct)}%` : '-',
    },
    {
      key: 'pr_pct',
      label: 'PR',
      sortValue: (row) => row.pr_pct ?? -Infinity,
      render: (row) => row.pr_pct != null
        ? h('span', { style:{color: row.pr_pct > 75 ? '#10B981' : row.pr_pct > 60 ? '#F59E0B' : '#EF4444', fontWeight:700} }, `${row.pr_pct}%`)
        : '-',
      csvValue: (row) => row.pr_pct != null ? `${row.pr_pct}%` : '-',
    },
    {
      key: 'plf_pct',
      label: 'PLF',
      sortValue: (row) => row.plf_pct ?? -Infinity,
      render: (row) => row.plf_pct != null
        ? h('span', { style:{ color: row.plf_pct > 100 ? '#EF4444' : row.plf_pct > 25 ? '#10B981' : '#F59E0B', fontWeight:700 } }, `${fmtNum(row.plf_pct, 1)}%`)
        : '-',
      csvValue: (row) => row.plf_pct != null ? `${row.plf_pct}%` : '-',
    },
  ];

  const prHeatVals = invTable.map(r => r.pr_pct).filter(v => v != null && Number.isFinite(Number(v))).map(Number);
  const heatMin = prHeatVals.length ? prHeatVals.reduce((a, b) => a < b ? a : b, Infinity) : 0;
  const heatMax = prHeatVals.length ? prHeatVals.reduce((a, b) => a > b ? a : b, -Infinity) : 100;
  const heatSpan = heatMax > heatMin ? heatMax - heatMin : null;
  const prToHeatStyle = (pr) => {
    if (pr == null || !Number.isFinite(Number(pr))) return { bg: '#E2E8F0', fg: '#64748b' };
    const t = heatSpan == null ? 0.5 : Math.max(0, Math.min(1, (Number(pr) - heatMin) / heatSpan));
    const hue = Math.round(120 * t);
    const bg = `hsl(${hue}, 72%, 42%)`;
    const fg = t > 0.45 ? '#0f172a' : '#ffffff';
    return { bg, fg };
  };

  /** Calendar days in [dateFrom, dateTo] for Power vs GTI axis labels */
  const dashboardDaySpan = React.useMemo(() => {
    try {
      if (!dateFrom || !dateTo) return 1;
      const a = new Date(`${String(dateFrom).slice(0, 10)}T00:00:00`);
      const b = new Date(`${String(dateTo).slice(0, 10)}T00:00:00`);
      return Math.max(1, Math.floor((b - a) / 86400000) + 1);
    } catch (_) {
      return 1;
    }
  }, [dateFrom, dateTo]);

  // ECharts Power vs GTI option — built here so useMemo can depend on powerGti + dashboardDaySpan
  const powerGtiOption = React.useMemo(() => {
    if (!powerGti.length) return null;
    const fmtTs = (v) => {
      const s = String(v || '');
      if (dashboardDaySpan <= 1) return s.length >= 16 ? s.slice(11,16) : s;
      return s.length >= 16 ? s.slice(5,10) + ' ' + s.slice(11,16) : s;
    };
    const rawTs   = powerGti.map(p => String(p.timestamp || ''));
    const pwrData  = powerGti.map(p => p.active_power_kw != null ? +p.active_power_kw : null);
    const gtiData  = powerGti.map(p => p.gti != null ? +p.gti : null);
    return {
      backgroundColor: 'transparent',
      animation: true, animationDuration: 700, animationEasing: 'cubicOut',
      tooltip: {
        trigger: 'axis',
        backgroundColor: 'rgba(9,18,29,0.96)',
        borderColor: 'rgba(62,183,223,0.22)',
        textStyle: { color:'#ecf2f8', fontSize:12 },
        axisPointer: { type:'cross', lineStyle:{ color:'rgba(62,183,223,0.25)' }, label:{ backgroundColor:'#162334' } },
        formatter: (items) => {
          const rows = Array.isArray(items) ? items : [items];
          if (!rows.length) return '';
          const title = String(rows[0].axisValue || '');
          const body = rows.map((item) => {
            const value = item.value == null || item.value === '' ? '—' : Number(item.value).toLocaleString(undefined, { maximumFractionDigits: 2 });
            return `${item.marker}${item.seriesName}: ${value}`;
          });
          return [title, ...body].join('<br/>');
        },
      },
      legend: {
        top: 4, icon:'roundRect', itemWidth:14, itemHeight:3,
        textStyle:{ color:'#a8b8c8', fontSize:11 },
        inactiveColor: '#435a6e',
      },
      grid: { top:46, right:62, bottom:56, left:54, containLabel:true },
      xAxis: {
        type:'category', data:rawTs,
        axisLine:{ lineStyle:{ color:'rgba(255,255,255,0.1)' } },
        axisTick:{ show:false },
        axisLabel:{ color:'#71849a', fontSize:9, formatter: (value) => fmtTs(value) },
        splitLine:{ show:false },
      },
      yAxis: [
        { type:'value', name:'kW', nameTextStyle:{ color:'#71849a', fontSize:10, padding:[0,0,0,4] },
          min: 0, alignTicks: true, splitNumber: 5,
          axisLabel:{ color:'#71849a', fontSize:9 },
          splitLine:{ lineStyle:{ color:'rgba(255,255,255,0.05)', type:'dashed' } },
          axisLine:{ show:false }, axisTick:{ show:false } },
        { type:'value', name:'W/m²', nameTextStyle:{ color:'#e4a146', fontSize:10 },
          min: 0, alignTicks: true, splitNumber: 5,
          axisLabel:{ color:'#71849a', fontSize:9 },
          splitLine:{ show:false }, axisLine:{ show:false }, axisTick:{ show:false } },
      ],
      series: [
        { name:'AC Power (kW)', type:'line', data:pwrData, yAxisIndex:0,
          smooth:true, showSymbol:false, connectNulls:true,
          lineStyle:{ color:'#3eb7df', width:2 },
          areaStyle:{ color:{ type:'linear',x:0,y:0,x2:0,y2:1,
            colorStops:[{offset:0,color:'rgba(62,183,223,0.22)'},{offset:1,color:'rgba(62,183,223,0)'}] } } },
        { name:'GTI (W/m²)', type:'line', data:gtiData, yAxisIndex:1,
          smooth:true, showSymbol:false, connectNulls:true,
          lineStyle:{ color:'#e4a146', width:2, type:'dashed' } },
      ],
      // Plotly-like axis stretching: X via wheel/drag, each Y via Shift+wheel or drag on the axis itself.
      dataZoom: [
        { type:'inside', xAxisIndex:0, zoomOnMouseWheel: true, moveOnMouseMove: true },
        { type:'inside', yAxisIndex:0, zoomOnMouseWheel: 'shift', moveOnMouseMove: 'shift', filterMode: 'none' },
        { type:'inside', yAxisIndex:1, zoomOnMouseWheel: 'shift', moveOnMouseMove: 'shift', filterMode: 'none' },
      ],
    };
  }, [powerGti, dashboardDaySpan]);

  // ── Minimal hero data (only used for status strip, NOT duplicating KPI tiles) ──
  const heroStatus   = station?.status || 'Unknown';
  const heroOnline   = heroStatus === 'Active' || heroStatus === 'active';
  const heroCapacity = station?.capacity_mwp ? `${station.capacity_mwp} MWp` : null;

  return h('div', null,
    // ── Plant context strip: operational metadata that gives meaning to the KPIs below.
    // Deliberately does NOT repeat Energy/Power/PR — those live in the KPI tiles. ──
    station && h('div', { className:'plant-hero', style: { justifyContent: 'space-between' } },
      h('div', { style: { display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' } },
        h('div', { className:`hero-status-dot ${heroOnline ? '' : 'offline'}` }),
        h('div', { style:{display:'flex', flexDirection:'column', gap:1} },
          h('span', { className:'hero-name' }, station.plant_id || 'Solar Plant'),
          h('span', { style:{fontSize:10, color:'var(--text-muted)', fontWeight:600, lineHeight:1.3} },
            heroOnline ? 'Active' : 'Offline'),
        ),
        h('div', { className:'hero-divider' }),
        heroCapacity && h('span', { className:'hero-capacity-badge' }, heroCapacity),
        station.technology && h('span', { className:'hero-capacity-badge' }, station.technology),
        station.plant_age_years && h('span', { className:'hero-capacity-badge' }, `${station.plant_age_years} yrs`),
      ),
      h('div', { style: { display: 'flex', alignItems: 'center', gap: 20, flexWrap: 'wrap' } },
        station.ppa_tariff && h('div', { style:{display:'flex',flexDirection:'column',gap:2,textAlign:'right'} },
          h('div', { className:'hero-metric-label' }, 'PPA Tariff'),
          h('div', { style:{fontSize:13,fontWeight:800,color:'var(--accent)',fontVariantNumeric:'tabular-nums'} },
            `₹${station.ppa_tariff}/kWh`),
        ),
        station.cod_date && h('div', { style:{display:'flex',flexDirection:'column',gap:2,textAlign:'right'} },
          h('div', { className:'hero-metric-label' }, 'COD Date'),
          h('div', { style:{fontSize:13,fontWeight:700,color:'var(--text-soft)'} }, station.cod_date),
        )
      )
    ),

    // ── Onboarding banner when no operational data exists ──────────────────
    noData && h('div', {
      className: 'dashboard-no-data-banner',
      style: {
        background: 'linear-gradient(135deg, rgba(14,165,233,0.14), rgba(2,132,199,0.08))',
        border: '1px solid rgba(14,165,233,0.28)',
        borderRadius: 12,
        padding: '16px 20px',
        marginBottom: 16,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: 16,
        color: 'var(--text)',
      },
    },
      h('div', { style: { minWidth: 0 } },
        h('div', { style: { fontWeight: 700, fontSize: 14, marginBottom: 4, color: 'var(--text)' } }, 'No operational data yet for this date range'),
        h('div', { style: { fontSize: 12, color: 'var(--text-soft)', lineHeight: 1.45 } }, 'Upload your plant architecture and raw data via Metadata to start seeing analytics.'),
      ),
      onNavigate && h('button', {
        className:'btn btn-primary', onClick:()=>onNavigate('Metadata'),
        style:{ fontSize:13 },
      }, 'Go to Metadata →'),
    ),

    // ── Station Details + KPI Cards ────────────────────────────────────────
    // ── Full-width KPI grid — 3 columns × 2 rows, no Station Details sidebar ──
    h(Card, { title:'Performance KPIs', style:{marginBottom:16} },
      h('div', { className:'kpi-grid kpi-grid--performance', style:{gridTemplateColumns:'repeat(3, 1fr)'} },
        h(KpiCard, { variant:'performance', subVariant:'today-energy', label:'Energy Export',  value: energyExportKpi.value, unit: energyExportKpi.unit, color:'#166534', icon: 'Leaf' }),
        h(KpiCard, { variant:'performance', subVariant:'total-power',  label:'Peak Power',     value:fmtNum(kpis?.peak_power_kw),     unit:'kW', color:'#92400e', icon: 'Zap' }),
        h(KpiCard, { variant:'performance', subVariant:'current',      label:'Active Power',   value:fmtNum(kpis?.active_power_kw),   unit:'kW', color:'#0f766e', icon: 'Activity' }),
        h(KpiCard, { variant:'performance', subVariant:'total-energy', label:'Perf. Ratio',    value:fmtNum(kpis?.performance_ratio), unit:'%',  color:'#6b21a8', icon: 'TrendingUp' }),
        h(KpiCard, { variant:'performance', subVariant:'voltage',      label:'PLF',            value:fmtNum(kpis?.plant_load_factor), unit:'%',  color:'#0369a1', icon: 'Gauge' }),
        h(KpiCard, { variant:'performance', subVariant:'co2-avoided',  label:'Net Generation', value: netGenKpi.value, unit: netGenKpi.unit, color:'#1d4ed8', icon: 'Cloud' }),
      ),
    ),

    // ── DS Insights ─────────────────────────────────────────────────────────
    // ── Target vs Actual Generation (Hourly Cumulate, Manual input) ────────
    h('div', { style:{display:'grid', gridTemplateColumns:'2fr 1fr', gap:16, marginBottom:16, alignItems:'stretch'} },
      h(window.DashboardTargetGeneration, { plantId, dateFrom, dateTo, targetGeneration }),
      h(Card, { title:'WMS - Weather Sensors', style:{ minHeight: 480, display:'flex', flexDirection:'column' } },
        h('div', { className:'kpi-grid kpi-grid--performance', style:{ gridTemplateColumns:'repeat(2, 1fr)', flex:1 } },
          h(KpiCard, { variant:'performance', subVariant:'wms-ghi', label:'Insolation (GHI)', value:fmtNum(wms?.ghi,2), unit:'kWh/m²', color:'#ca8a04', icon:'Sun' }),
          h(KpiCard, { variant:'performance', subVariant:'wms-gti', label:'Insolation (GTI)', value:fmtNum(wms?.gti,2), unit:'kWh/m²', color:'#ea580c', icon:'CloudSun' }),
          h(KpiCard, { variant:'performance', subVariant:'wms-irr-tilt', label:'Irradiance (tilt)', value:fmtNum(wms?.irradiance_tilt, 1), unit:'W/m²', color:'#b45309', icon:'Gauge' }),
          h(KpiCard, { variant:'performance', subVariant:'wms-irr-horz', label:'Irradiance (Horizontal)', value:fmtNum(wms?.irradiance_horizontal, 1), unit:'W/m²', color:'#0f766e', icon:'Compass' }),
          h(KpiCard, { variant:'performance', subVariant:'wms-ambient', label:'Amb. Temp', value:fmtNum(wms?.ambient_temp), unit:'°C', color:'#0369a1', icon:'Thermometer' }),
          h(KpiCard, { variant:'performance', subVariant:'wms-module', label:'Mod. Temp', value:fmtNum(wms?.module_temp), unit:'°C', color:'#be185d', icon:'Flame' }),
          h(KpiCard, { variant:'performance', subVariant:'wms-wind', label:'Wind Speed', value:fmtNum(wms?.wind_speed), unit:'m/s', color:'#4f46e5', icon:'Wind' }),
          h(KpiCard, { variant:'performance', subVariant:'wms-rain', label:'Rain gauge', value:fmtNum(wms?.rainfall_mm, 2), unit:'mm', color:'#0e7490', icon:'CloudRain' }),
        ),
      ),
    ),

    // ── Power vs GTI + Inverter table ──────────────────────────────────────
    h('div', { style:{display:'grid', gridTemplateColumns:'1fr', gap:16, marginBottom:16} },
      h(Card, { title:'⚡ Active Power vs GTI — Intraday' },
        powerGtiOption
          ? h(window.EChart, { option: powerGtiOption, style:{ height:340 } })
          : h('div', { className:'empty-state', style:{minHeight:320} }, 'No power data for this period'),
      ),
      h(Card, { 
        title: h('div', { style:{display:'flex', justifyContent:'space-between', alignItems:'center', width:'100%'} },
          h('span', null, 'Inverter Performance'),
          h('div', { className:'btn-group', style:{display:'flex', gap:4} },
            h('button', { className:`btn ${invViewMode==='table'?'btn-primary':'btn-outline'}`, style:{padding:'2px 8px', fontSize:11}, onClick:()=>setInvViewMode('table') }, 'Table'),
            h('button', { className:`btn ${invViewMode==='heatmap'?'btn-primary':'btn-outline'}`, style:{padding:'2px 8px', fontSize:11}, onClick:()=>setInvViewMode('heatmap') }, 'Heatmap')
          )
        )
      },
        invViewMode === 'table' ? (
          h(window.DataTable, {
            columns: invColumns,
            rows: invTable,
            emptyMessage: 'No inverter data',
            filename: `inverter_performance_${plantId || 'plant'}_${dateFrom}_${dateTo}.csv`,
            maxHeight: 420,
            initialSortKey: 'inverter_id',
          })
        ) : (
          h('div', { style:{ padding:'10px 0' } },
            h('div', { style:{ display:'flex', alignItems:'center', gap:10, marginBottom:10, flexWrap:'wrap' } },
              h('span', { style:{ fontSize:11, color:'var(--text-muted)', fontWeight:600 } }, 'PR gradient (min → max):'),
              h('div', { style:{
                flex:1, minWidth:120, maxWidth:280, height:14, borderRadius:7,
                background:'linear-gradient(90deg, hsl(0,72%,42%), hsl(60,72%,42%), hsl(120,72%,42%))',
                border:'1px solid var(--line)',
              } }),
              h('span', { style:{ fontSize:11, color:'var(--text-soft)', fontVariantNumeric:'tabular-nums' } },
                `${prHeatVals.length ? heatMin.toFixed(1) : '—'}% → ${prHeatVals.length ? heatMax.toFixed(1) : '—'}%`),
            ),
            h('div', { style:{ display:'flex', flexWrap:'wrap', gap:4 } },
              invTable.map(r => {
                const pr = r.pr_pct;
                const { bg, fg } = prToHeatStyle(pr);
                return h('div', {
                  key: r.inverter_id,
                  style: {
                    width:'calc(16.66% - 4px)', minWidth: 54, height: 46, background: bg, color: fg, borderRadius: 6,
                    display:'flex', flexDirection:'column', alignItems:'center', justifyContent:'center', fontSize: 9,
                    boxShadow: '0 1px 4px rgba(0,0,0,0.10)', cursor:'default', border: '1px solid rgba(0,0,0,0.05)',
                    padding: '2px 4px',
                  },
                  title: `${r.inverter_id}: PR ${pr != null ? pr + '%' : 'N/A'} | Yield ${r.yield_kwh_kwp ?? '—'} kWh/kWp`,
                },
                  h('div', { style:{ fontWeight:800, lineHeight:1.05 } }, r.inverter_id.replace('INV-', '')),
                  h('div', { style:{ opacity: 0.95, marginTop: 1, fontSize: 8.5 } }, pr != null ? `${pr}%` : 'N/A'),
                );
              }),
            ),
          )
        )
      ),
    )
  );
};

// ═══════════════════════════════════════════════════════════════════════════════
// ANALYTICS LAB PAGE
// ═══════════════════════════════════════════════════════════════════════════════
window.AnalyticsPage = ({ plantId, dateFrom, dateTo, onNavigate }) => {
  const h = React.createElement;
  const LEVELS = ['inverter', 'scb', 'string'];
  const PARAMS = ['dc_current','dc_voltage','dc_power','ac_power','irradiance'];
  const PARAM_LABELS = { dc_current:'DC Current (A)', dc_voltage:'DC Voltage (V)', dc_power:'DC Power (kW)', ac_power:'AC Power (kW)', irradiance:'Irradiance (W/m²)' };
  const COLORS = ['#0EA5E9','#F59E0B','#10B981','#8B5CF6','#EC4899','#EF4444','#14B8A6','#F97316'];

  const [level, setLevel]         = useState('inverter');
  const [equipList, setEquipList] = useState([]);
  const [selected, setSelected]   = useState([]);
  const [search, setSearch]       = useState('');
  const [params, setParams]       = useState(['dc_current','dc_voltage']);
  const [normalize, setNormalize] = useState(false);
  const [tsData, setTsData]       = useState([]);
  const [avail, setAvail]         = useState(0);
  const [loading, setLoading]     = useState(false);
  const [equipLoading, setEquipLoading] = useState(false);
  // ECharts replaces Recharts for the timeseries panel — no hiddenLines state needed.

  useEffect(() => {
    if (!plantId) return;
    setEquipLoading(true);
    window.SolarAPI.Analytics.equipment(level, plantId)
      .then(r => { setEquipList(r.equipment_ids || []); setSelected([]); })
      .catch(()=>setEquipList([]))
      .finally(()=>setEquipLoading(false));
  }, [level, plantId]);

  const toggleParam = (p) => setParams(prev => prev.includes(p) ? prev.filter(x=>x!==p) : [...prev,p]);

  const fetchData = async () => {
    if (!selected.length) { (window.Toast||{error:alert}).error('Select at least one equipment ID first.'); return; }
    if (!params.length)   { (window.Toast||{error:alert}).error('Select at least one parameter to plot.'); return; }
    setLoading(true);
    try {
      const r = await window.SolarAPI.Analytics.timeseries(
        selected.join(','), params.join(','), plantId, dateFrom, dateTo, normalize, 'raw',
      );
      setTsData(r.data || []); setAvail(r.availability_pct || 0);
      if ((r.data||[]).length === 0) (window.Toast||{info:()=>{}}).info('No data returned for selected range.');
    } catch(e) { (window.Toast||{error:alert}).error('Fetch failed', e.message); }
    finally { setLoading(false); }
  };

  const chartData = {};
  tsData.forEach(d => {
    if (!chartData[d.timestamp]) chartData[d.timestamp] = { timestamp: d.timestamp };
    chartData[d.timestamp][`${d.equipment_id}|${d.signal}`] = d.value;
  });
  const chartRows = Object.values(chartData).sort((a,b)=>a.timestamp.localeCompare(b.timestamp));
  const topSigs = ['dc_current','dc_power','ac_power','irradiance'].filter(s=>params.includes(s));
  const botSigs = ['dc_voltage'].filter(s=>params.includes(s));

  // ── ECharts option builder for Analytics Lab timeseries ─────────────────
  function buildAnalyticsOption(sigList) {
    const palette = ['#3eb7df','#e4a146','#34c889','#8b5cf6','#ec4899','#ef6b6b','#14b8a6','#f97316'];
    const timestamps = chartRows.map(r => {
      const s = String(r.timestamp || '');
      return s.length >= 16 ? s.slice(11,16) : s;
    });
    const series = [];
    let ci = 0;
    // Strip trailing "(unit)" from signal label so the legend stays compact
    // (axis/tooltip still show units).
    const shortSig = (sig) => String(PARAM_LABELS[sig] || sig).replace(/\s*\([^)]*\)\s*$/, '').trim();
    selected.forEach(function(eq) {
      sigList.forEach(function(sig) {
        const color = palette[ci % palette.length]; ci++;
        const vals = chartRows.map(r => r[eq+'|'+sig] != null ? +r[eq+'|'+sig] : null);
        series.push({
          name: eq + ' · ' + shortSig(sig),
          type: 'line', data: vals,
          smooth: true, showSymbol: false, connectNulls: true,
          lineStyle: { color, width: 1.8 },
          areaStyle: selected.length === 1 && sigList.length === 1
            ? { color:{ type:'linear',x:0,y:0,x2:0,y2:1, colorStops:[{offset:0,color:color+'33'},{offset:1,color:color+'00'}] } }
            : undefined,
        });
      });
    });
    const legendRows = Math.min(3, Math.max(1, Math.ceil(series.length / 4)));
    const gridTop = 30 + legendRows * 22;
    return {
      backgroundColor: 'transparent',
      animation: true, animationDuration: 600, animationEasing: 'cubicOut',
      tooltip: {
        trigger: 'axis',
        backgroundColor: 'rgba(9,18,29,0.96)',
        borderColor: 'rgba(62,183,223,0.22)',
        textStyle: { color:'#ecf2f8', fontSize:12 },
        axisPointer: { type:'cross', lineStyle:{ color:'rgba(62,183,223,0.25)' }, label:{ backgroundColor:'#162334' } },
      },
      legend: {
        top: 4, type:'scroll', left:'center', width:'78%',
        icon:'roundRect', itemWidth:14, itemHeight:8, itemGap: 14,
        textStyle:{ color:'#a8b8c8', fontSize:11 },
        inactiveColor: '#435a6e',
      },
      grid: { top:gridTop, right:20, bottom:56, left:56, containLabel:true },
      xAxis: {
        type:'category', data:timestamps,
        axisLine:{ lineStyle:{ color:'rgba(255,255,255,0.1)' } },
        axisTick:{ show:false },
        axisLabel:{ color:'#71849a', fontSize:9 },
        splitLine:{ show:false },
      },
      yAxis: {
        type:'value', splitNumber: 5,
        axisLabel:{ color:'#71849a', fontSize:9 },
        splitLine:{ lineStyle:{ color:'rgba(255,255,255,0.05)', type:'dashed' } },
        axisLine:{ show:false }, axisTick:{ show:false },
      },
      dataZoom: [
        { type:'inside', xAxisIndex:0, zoomOnMouseWheel: true, moveOnMouseMove: true },
        { type:'inside', yAxisIndex:0, zoomOnMouseWheel: 'shift', moveOnMouseMove: 'shift', filterMode: 'none' },
      ],
      series,
    };
  }

  return h('div', null,
    // ── Guide banner if no equipment found ────────────────────────────────
    !equipLoading && equipList.length === 0 && h('div', {
      style:{ background:'#FFF7ED', border:'1px solid #FED7AA', borderRadius:10, padding:'14px 18px', marginBottom:16, display:'flex', alignItems:'center', justifyContent:'space-between' },
    },
      h('div', null,
        h('div', { style:{fontWeight:700,fontSize:13,color:'#92400E',marginBottom:4} }, 'No equipment data found in database'),
        h('div', { style:{fontSize:12,color:'#B45309'} }, 'Upload raw time-series data (raw_data_generic) and plant architecture to use the Analytics Lab.'),
      ),
      onNavigate && h('button', { className:'btn btn-outline', onClick:()=>onNavigate('Metadata'), style:{borderColor:'#F59E0B', color:'#B45309'} }, 'Go to Metadata'),
    ),

    // ── Control panel ─────────────────────────────────────────────────────
    h('div', { style:{display:'grid', gridTemplateColumns:'140px 1fr 1fr', gap:16, marginBottom:16} },
      h(Card, { title:'Hierarchy Level' },
        h('div', { style:{display:'flex',flexDirection:'column',gap:8} },
          LEVELS.map(l => h('label', { key:l, style:{display:'flex',gap:8,alignItems:'center',cursor:'pointer',fontSize:13,padding:'6px 8px',borderRadius:6,background:level===l?'rgba(14,165,233,0.08)':'transparent'} },
            h('input', { type:'radio', name:'level', value:l, checked:level===l, onChange:()=>setLevel(l) }),
            h('span', { style:{fontWeight: level===l?700:400, color:level===l?'var(--accent)':'var(--text-primary)'} }, l.toUpperCase()),
          )),
        ),
      ),
      h(Card, { title: equipLoading ? 'Equipment - Loading...' : `Equipment  (${selected.length} of ${equipList.length} selected)` },
        equipLoading
          ? h('div', { className:'empty-state', style:{minHeight:80} }, h(Spinner), 'Loading...')
          : h(EquipmentPicker, {
              ids:equipList, selected, search, onSearch:setSearch,
              onToggle: id => setSelected(p => p.includes(id) ? p.filter(x=>x!==id) : [...p,id]),
              onSelectAll: (filtered, sel) => setSelected(prev => sel ? [...new Set([...prev,...filtered])] : prev.filter(id=>!filtered.includes(id))),
            }),
      ),
      h(Card, { title:'Parameters to Plot', action: h(Toggle, { label:'Normalize', value:normalize, onChange:setNormalize }) },
        h('div', { style:{display:'flex',flexDirection:'column',gap:8,marginBottom:14} },
          PARAMS.map(p => h('label', { key:p, style:{display:'flex',gap:8,alignItems:'center',cursor:'pointer',fontSize:13} },
            h('input', { type:'checkbox', checked:params.includes(p), onChange:()=>toggleParam(p) }),
            PARAM_LABELS[p],
          )),
        ),
        h('button', { className:'btn btn-primary', onClick:fetchData, style:{width:'100%',justifyContent:'center'}, disabled:loading },
          loading ? h(Spinner) : 'Plot Charts',
        ),
      ),
    ),

    // ── Data availability bar ─────────────────────────────────────────────
    tsData.length > 0 && h('div', { style:{display:'flex',alignItems:'center',gap:12,marginBottom:12,padding:'10px 16px',borderRadius:10,border:'1px solid var(--line)',background:'rgba(255,255,255,0.02)'} },
      h('span', { style:{fontSize:12,fontWeight:700,color:'var(--text-soft)',whiteSpace:'nowrap'} }, `Data Availability: ${avail}%`),
      h('div', { className:'avail-bar-track', style:{flex:1} }, h('div', { className:'avail-bar-fill', style:{width:`${avail}%`} })),
      h('span', { style:{fontSize:11,color:'var(--text-muted)',whiteSpace:'nowrap'} }, `${tsData.length.toLocaleString()} data points`),
    ),

    tsData.length > 0 && topSigs.length > 0 && h(Card, { title:'📈 Time Series — Current / Power / Irradiance', style:{marginBottom:12} },
      h(window.EChart, { option: buildAnalyticsOption(topSigs), style:{ height:320 } }),
    ),

    tsData.length > 0 && botSigs.length > 0 && h(Card, { title:'⚡ DC Voltage (V)' },
      h(window.EChart, { option: buildAnalyticsOption(botSigs), style:{ height:260 } }),
    ),

    tsData.length === 0 && equipList.length > 0 && h('div', { className:'empty-state', style:{minHeight:260,background:'white',borderRadius:12,border:'1px solid var(--border)'} },
      h('span', null, 'Select equipment and at least one parameter, then click "Plot Charts"'),
    ),
  );
};

// Fault Diagnostics: `window.FaultPage` is defined in js/fault_page.js (loaded after this file in index.html boot order).

// ═══════════════════════════════════════════════════════════════════════════════
// METADATA PAGE  - FIXED: Upload buttons + auth-safe template download
// ═══════════════════════════════════════════════════════════════════════════════
window.MetadataPage = ({ plantId }) => {
  const h = React.createElement;
  const [tab, setTab]             = useState('architecture');
  const [archView, setArchView]   = useState('diagram'); // 'diagram' | 'table'
  const [archList, setArchList]   = useState([]);
  const [archTotal, setArchTotal] = useState(0);
  const [archLoading, setArchLoading] = useState(false);
  const [specList, setSpecList]   = useState([]);
  const [loading, setLoading]     = useState(false);
  const [uploading, setUploading] = useState(false);
  const [uploadMsg, setUploadMsg] = useState(null); // { type:'success'|'error', text }
  const [rawSummary, setRawSummary] = useState(null);
  const [rawPreview, setRawPreview] = useState([]);
  const [rawPreviewLoading, setRawPreviewLoading] = useState(false);
  const [rawPreviewFilters, setRawPreviewFilters] = useState({
    equipmentLevel: 'scb',
    equipmentId: '',
    signal: '',
    dateFrom: '',
    dateTo: '',
  });
  
  const [mapData, setMapData] = useState(null); 
  const [mapping, setMapping] = useState({});
  
  // Load equipment first so Metadata tab opens fast; rawSummary in background. Architecture loads only when user opens that sub-tab.
  const loadData = useCallback(() => {
    if (!plantId) {
      setArchList([]);
      setArchTotal(0);
      setSpecList([]);
      setRawSummary({ total_rows: 0, date_range: null, levels: {} });
      setRawPreview([]);
      setLoading(false);
      return;
    }
    setLoading(true);
    setRawSummary(null);
    setRawPreview([]);
    setArchList([]);
    setArchTotal(0);
    Promise.all([
      window.SolarAPI.Metadata.equipment(plantId).then(setSpecList).catch(() => setSpecList([])),
    ]).finally(() => setLoading(false));
    window.SolarAPI.Metadata.rawSummary(plantId)
      .then(setRawSummary)
      .catch(() => setRawSummary({ total_rows: 0, date_range: null, levels: {} }));
  }, [plantId]);

  const handleMetadataRefresh = useCallback(async () => {
    if (plantId && tab === 'rawdata') {
      try {
        await window.SolarAPI.Metadata.reindexRawEquipment(plantId);
      } catch (e) {
        console.warn('reindex-raw-equipment', e);
      }
    }
    loadData();
  }, [plantId, tab, loadData]);

  useEffect(loadData, [loadData]);

  useEffect(() => {
    if (tab !== 'rawdata' || !plantId) return;
    setRawPreviewLoading(true);
    window.SolarAPI.Metadata.rawDataPreview(plantId, rawPreviewFilters)
      .then((res) => setRawPreview(res.rows || []))
      .catch(() => setRawPreview([]))
      .finally(() => setRawPreviewLoading(false));
  }, [tab, plantId, rawPreviewFilters]);

  // Lazy-load architecture only when user opens the Architecture sub-tab.
  // Paginate through the full dataset so the diagram shows every inverter/SCB/string,
  // not just the first 10k rows (large plants can have 40k+ architecture rows).
  useEffect(() => {
    if (tab !== 'architecture' || !plantId) return;
    let cancelled = false;
    const PAGE = 20000;
    setArchLoading(true);
    setArchList([]);
    setArchTotal(0);
    (async () => {
      try {
        // 1. First page tells us the total row count.
        const first = await window.SolarAPI.Metadata.architecture(plantId, PAGE, 0);
        if (cancelled) return;
        const total = first.total != null ? first.total : (first.items || first).length;
        const firstBatch = (first.items || first).slice();
        setArchTotal(total);
        setArchList(firstBatch);

        // 2. Fire the remaining pages IN PARALLEL instead of one-at-a-time.
        //    For a 40k-row plant this used to be 3 round-trips serialised
        //    (~900 ms); now all three run in flight together (~330 ms).
        const offsets = [];
        for (let off = firstBatch.length; off < total; off += PAGE) offsets.push(off);
        if (offsets.length === 0) return;

        const batches = await Promise.all(
          offsets.map((off) => window.SolarAPI.Metadata.architecture(plantId, PAGE, off))
        );
        if (cancelled) return;
        const merged = firstBatch.slice();
        for (const next of batches) {
          const batch = (next.items || next);
          if (batch && batch.length) merged.push.apply(merged, batch);
        }
        setArchList(merged);
      } catch (_) {
        if (!cancelled) { setArchList([]); setArchTotal(0); }
      } finally {
        if (!cancelled) setArchLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, [tab, plantId]);

  const handleUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    setUploading(true); setUploadMsg(null);
    try {
      if (tab === 'rawdata') {
        const res = await window.SolarAPI.Metadata.analyzeRawData(file);
        
        if (res.is_ntpc) {
          // Exactly the NTPC report format, skip mapper and auto-ingest
          const f = new FormData(); 
          f.append('plant_id', plantId);
          f.append('file', file);
          const endpoint = res.ntpc_type === 'inv_pwr' ? '/api/metadata/upload-raw-data-inv-pwr' : '/api/metadata/upload-raw-data-ntpc';
          const result = await apiFetch(endpoint, { method:'POST', body: f });
          setUploadMsg({ type:'success', text:`NTPC ${res.ntpc_type==='inv_pwr'?'Power':'SCB'} Report Imported! ${result.rows_imported} rows.` });
          setTimeout(loadData, 800);
        } else {
          // Generic upload, use mapper UI
          const reqFields = ['timestamp', 'inverter_id', 'scb_id', 'scb_current', 'dc_voltage'];
          const optFields = ['inverter_status', 'string_count'];
          const autoMap = {};
          [...reqFields, ...optFields].forEach(f => {
            const match = res.columns.find(c => c.toLowerCase().replace(/ /g, '_') === f || c.toLowerCase().replace(/[^a-z0-9]/g,'') === f.replace(/_/g, ''));
            autoMap[f] = match || '';
          });
          setMapData({ file, columns: res.columns });
          setMapping(autoMap);
        }
      } else {
        const endpoint = tab === 'architecture' ? '/api/metadata/upload-architecture' 
                       : `/api/metadata/upload-equipment?plant_id=${plantId}`;
        const result = await uploadExcel(endpoint, file);
        setUploadMsg({ type:'success', text:`Successfully imported ${result.rows_imported} rows!` });
        setTimeout(loadData, 800);
      }
    } catch(err) {
      setUploadMsg({ type:'error', text: err.message });
    } finally {
      if (tab !== 'rawdata' || !mapData) {
        e.target.value = ''; // Reset input unless showing modal
      }
      setUploading(false);
    }
  };

  const submitMapping = async () => {
    setUploading(true); setUploadMsg(null);
    try {
      const result = await window.SolarAPI.Metadata.uploadMappedRawData(plantId, mapping, mapData.file);
      setUploadMsg({ type:'success', text:`Successfully mapped and imported ${result.rows_imported} rows! DS diagnostics run.` });
      setMapData(null);
      setTimeout(loadData, 800);
    } catch(err) {
      setUploadMsg({ type:'error', text: err.message });
    } finally {
      setUploading(false);
      document.getElementById('upload-rawdata').value = '';
    }
  };

  const fmtNull = v => (v == null || v === '') ? '-' : v;
  const architectureColumns = [
    {
      key:'plant_id',
      label:'Plant ID',
      render:(r)=>h('span', { style:{fontWeight:600,color:'var(--accent)'} }, r.plant_id),
      csvValue:(r)=>r.plant_id,
    },
    { key:'inverter_id', label:'Inverter', csvValue:(r)=>r.inverter_id },
    { key:'scb_id', label:'SCB', csvValue:(r)=>r.scb_id },
    { key:'string_id', label:'String', csvValue:(r)=>r.string_id },
    { key:'modules_per_string', label:'Modules/String', csvValue:(r)=>fmtNull(r.modules_per_string) },
    { key:'strings_per_scb', label:'Strings/SCB', csvValue:(r)=>fmtNull(r.strings_per_scb) },
    { key:'dc_capacity_kw', label:'DC Cap (kW)', csvValue:(r)=>fmtNull(r.dc_capacity_kw) },
  ];
  const [equipFormOpen, setEquipFormOpen] = useState(false);
  const [equipFormMode, setEquipFormMode] = useState('inverter'); // 'inverter' | 'module'
  const [editingSpec, setEditingSpec] = useState(null);
  const [equipFormData, setEquipFormData] = useState({});
  const [equipFormFile, setEquipFormFile] = useState(null);
  const [equipSaving, setEquipSaving] = useState(false);

  const openEquipForm = (mode, spec) => {
    setEquipFormMode(mode);
    setEditingSpec(spec || null);
    if (spec) {
      setEquipFormData({
        plant_id: spec.plant_id || plantId,
        equipment_id: spec.equipment_id,
        equipment_type: spec.equipment_type || mode,
        manufacturer: spec.manufacturer ?? '',
        model: spec.model ?? '',
        rated_power: spec.rated_power ?? '',
        target_efficiency: spec.target_efficiency ?? 98.5,
        imp: spec.imp ?? '', vmp: spec.vmp ?? '', isc: spec.isc ?? '', voc: spec.voc ?? '',
        ac_capacity_kw: spec.ac_capacity_kw ?? '', dc_capacity_kwp: spec.dc_capacity_kwp ?? '',
        rated_efficiency: spec.rated_efficiency ?? '', mppt_voltage_min: spec.mppt_voltage_min ?? '',
        mppt_voltage_max: spec.mppt_voltage_max ?? '',         voltage_limit: spec.voltage_limit ?? '',
        current_set_point: spec.current_set_point ?? '',
        degradation_loss_pct: spec.degradation_loss_pct ?? '',
        temp_coefficient_per_deg: spec.temp_coefficient_per_deg ?? '',
        impp: spec.impp ?? '', vmpp: spec.vmpp ?? '', pmax: spec.pmax ?? '',
        degradation_year1_pct: spec.degradation_year1_pct ?? '', degradation_year2_pct: spec.degradation_year2_pct ?? '',
        degradation_annual_pct: spec.degradation_annual_pct ?? '', module_efficiency_pct: spec.module_efficiency_pct ?? '',
        alpha_stc: spec.alpha_stc ?? '', beta_stc: spec.beta_stc ?? '', gamma_stc: spec.gamma_stc ?? '',
        alpha_noct: spec.alpha_noct ?? '', beta_noct: spec.beta_noct ?? '', gamma_noct: spec.gamma_noct ?? '',
      });
    } else {
      setEquipFormData({
        plant_id: plantId || '',
        equipment_id: '',
        equipment_type: mode,
        manufacturer: '', model: '', rated_power: '', target_efficiency: 98.5,
        imp: '', vmp: '', isc: '', voc: '',
        ac_capacity_kw: '', dc_capacity_kwp: '', rated_efficiency: '', mppt_voltage_min: '', mppt_voltage_max: '', voltage_limit: '', current_set_point: '',
        degradation_loss_pct: '', temp_coefficient_per_deg: '',
        impp: '', vmpp: '', pmax: '', degradation_year1_pct: '', degradation_year2_pct: '', degradation_annual_pct: '', module_efficiency_pct: '',
        alpha_stc: '', beta_stc: '', gamma_stc: '', alpha_noct: '', beta_noct: '', gamma_noct: '',
      });
    }
    setEquipFormFile(null);
    setEquipFormOpen(true);
  };

  const num = (v) => (v === '' || v == null) ? undefined : Number(v);
  const submitEquipForm = async () => {
    const payload = {
      plant_id: equipFormData.plant_id || plantId,
      equipment_id: (equipFormData.equipment_id || '').trim(),
      equipment_type: equipFormMode,
      manufacturer: equipFormData.manufacturer || undefined,
      model: equipFormData.model || undefined,
      rated_power: num(equipFormData.rated_power),
      target_efficiency: num(equipFormData.target_efficiency),
      imp: num(equipFormData.imp), vmp: num(equipFormData.vmp), isc: num(equipFormData.isc), voc: num(equipFormData.voc),
      ac_capacity_kw: num(equipFormData.ac_capacity_kw), dc_capacity_kwp: num(equipFormData.dc_capacity_kwp),
      rated_efficiency: num(equipFormData.rated_efficiency), mppt_voltage_min: num(equipFormData.mppt_voltage_min),
      mppt_voltage_max: num(equipFormData.mppt_voltage_max), voltage_limit: num(equipFormData.voltage_limit),
      current_set_point: num(equipFormData.current_set_point),
      degradation_loss_pct: num(equipFormData.degradation_loss_pct),
      temp_coefficient_per_deg: num(equipFormData.temp_coefficient_per_deg),
      impp: num(equipFormData.impp), vmpp: num(equipFormData.vmpp), pmax: num(equipFormData.pmax),
      degradation_year1_pct: num(equipFormData.degradation_year1_pct), degradation_year2_pct: num(equipFormData.degradation_year2_pct),
      degradation_annual_pct: num(equipFormData.degradation_annual_pct), module_efficiency_pct: num(equipFormData.module_efficiency_pct),
      alpha_stc: num(equipFormData.alpha_stc), beta_stc: num(equipFormData.beta_stc), gamma_stc: num(equipFormData.gamma_stc),
      alpha_noct: num(equipFormData.alpha_noct), beta_noct: num(equipFormData.beta_noct), gamma_noct: num(equipFormData.gamma_noct),
    };
    if (!payload.equipment_id || !payload.plant_id) { setUploadMsg({ type: 'error', text: 'Plant and Equipment ID are required.' }); return; }
    setEquipSaving(true);
    try {
      const saved = await window.SolarAPI.Metadata.addEquipment(payload);
      if (equipFormFile) {
        await window.SolarAPI.Metadata.uploadSpecSheet(saved.id, equipFormFile);
      }
      setUploadMsg({ type: 'success', text: 'Equipment spec saved.' });
      setEquipFormOpen(false);
      loadData();
    } catch (e) {
      setUploadMsg({ type: 'error', text: e.message });
    } finally {
      setEquipSaving(false);
    }
  };

  const Input = (label, key, type = 'text', placeholder = '') =>
    h('div', { key, className: 'form-group', style: { marginBottom: 8 } },
      h('label', { className: 'form-label', style: { fontSize: 11 } }, label),
      h('input', {
        className: 'form-input',
        type,
        placeholder,
        value: equipFormData[key] ?? '',
        onChange: (e) => setEquipFormData(prev => ({ ...prev, [key]: e.target.value })),
        style: { height: 32, fontSize: 12 },
      }),
    );

  const equipmentColumns = [
    {
      key:'equipment_id',
      label:'Equipment ID',
      render:(r)=>h('strong', null, r.equipment_id),
      csvValue:(r)=>r.equipment_id,
    },
    {
      key:'equipment_type',
      label:'Type',
      render:(r)=>h(Badge, { type: r.equipment_type==='inverter'?'blue':r.equipment_type==='module'?'green':'amber' }, r.equipment_type),
      csvValue:(r)=>r.equipment_type,
    },
    { key:'manufacturer', label:'Manufacturer', csvValue:(r)=>fmtNull(r.manufacturer) },
    { key:'model', label:'Model', csvValue:(r)=>fmtNull(r.model) },
    { key:'rated_power', label:'Rated Power', csvValue:(r)=>r.rated_power != null ? `${r.rated_power} kW` : '-' },
    { key:'ac_capacity_kw', label:'AC Cap (kW)', csvValue:(r)=>fmtNull(r.ac_capacity_kw) },
    { key:'dc_capacity_kwp', label:'DC Cap (kWp)', csvValue:(r)=>fmtNull(r.dc_capacity_kwp) },
    { key:'rated_efficiency', label:'Rated Eff %', csvValue:(r)=>fmtNull(r.rated_efficiency) },
    { key:'imp', label:'Imp (A)', csvValue:(r)=>fmtNull(r.imp) },
    { key:'vmp', label:'Vmp (V)', csvValue:(r)=>fmtNull(r.vmp) },
    { key:'impp', label:'Impp (A)', csvValue:(r)=>fmtNull(r.impp) },
    { key:'vmpp', label:'Vmpp (V)', csvValue:(r)=>fmtNull(r.vmpp) },
    { key:'pmax', label:'Pmax (W)', csvValue:(r)=>fmtNull(r.pmax) },
    { key:'isc', label:'Isc (A)', csvValue:(r)=>fmtNull(r.isc) },
    { key:'voc', label:'Voc (V)', csvValue:(r)=>fmtNull(r.voc) },
    {
      key: 'spec_sheet',
      label: 'Spec sheet',
      render: (r) => r.spec_sheet_path
        ? h('button', {
            type: 'button', className: 'btn btn-outline', style: { padding: '2px 6px', fontSize: 11 },
            onClick: async () => {
              try {
                const resp = await fetch(window.SolarAPI.Metadata.specSheetUrl(r.id).split('?')[0], {
                  headers: { Authorization: `Bearer ${window.SolarAPI.getToken()}` },
                });
                if (!resp.ok) throw new Error('Download failed');
                const blob = await resp.blob();
                const a = document.createElement('a');
                a.href = URL.createObjectURL(blob);
                a.download = (r.equipment_id || 'spec') + '_spec.pdf';
                a.click();
                URL.revokeObjectURL(a.href);
              } catch (e) { alert(e.message); }
            },
          }, 'Download')
        : '-',
      csvValue: (r) => r.spec_sheet_path ? 'Yes' : '-',
    },
    {
      key: 'actions',
      label: '',
      render: (r) => h('button', {
        className: 'btn btn-outline',
        style: { padding: '2px 8px', fontSize: 11 },
        onClick: () => openEquipForm(r.equipment_type || 'inverter', r),
      }, 'Edit'),
      csvValue: () => '',
    },
  ];

  const uploadInput = (id) => h('label', {
    htmlFor: id,
    className: 'btn btn-primary',
    style:{ cursor: uploading ? 'wait' : 'pointer', opacity: uploading ? 0.7 : 1 },
  },
    uploading ? h(Spinner) : 'Upload Excel',
    h('input', {
      id, type:'file', accept:'.xlsx,.xls',
      style:{ display:'none' }, onChange: handleUpload, disabled: uploading,
    }),
  );

  return h('div', null,
    // ── Tabs ──────────────────────────────────────────────────────────────
    h('div', { className:'metadata-tabs' },
      [['architecture','Plant Architecture'], ['equipment','Equipment Specs'], ['rawdata', 'Raw Data']].map(([t,label]) =>
        h('div', { key:t, className:`metadata-tab ${tab===t?'active':''}`, onClick:()=>{setTab(t);setUploadMsg(null);} }, label)
      ),
    ),

    // ── Action bar: Download Template + Upload + Refresh ───────────────────
    h('div', { style:{display:'flex', gap:10, marginBottom:16, alignItems:'center', flexWrap:'wrap'} },
      h('button', {
        className:'btn btn-outline',
        onClick:()=>downloadTemplate(
          tab==='architecture' ? '/api/metadata/template/architecture' : 
          tab==='equipment' ? '/api/metadata/template/equipment' : '/api/metadata/template/raw-data',
          tab==='architecture' ? 'architecture_template.xlsx' : 
          tab==='equipment' ? 'equipment_template.xlsx' : 'raw_data_template.xlsx',
        ),
      }, 'Download Template'),
      uploadInput(`upload-${tab}`),
      h('button', { className:'btn btn-outline', onClick: handleMetadataRefresh, disabled:loading }, loading ? h(Spinner) : 'Refresh'),
    ),

    // ── Upload status banner ────────────────────────────────────────────────
    uploadMsg && h('div', {
      style:{
        padding:'12px 16px', borderRadius:8, marginBottom:14, fontSize:13, fontWeight:500,
        background: uploadMsg.type==='success' ? 'rgba(16,185,129,0.10)' : 'rgba(239,68,68,0.10)',
        border: `1px solid ${uploadMsg.type==='success' ? '#10B981' : '#EF4444'}`,
        color: uploadMsg.type==='success' ? '#059669' : '#DC2626',
      },
    }, (uploadMsg.type==='success' ? 'Success: ' : 'Error: ') + uploadMsg.text),

    // ── Help tip ──────────────────────────────────────────────────────────
    h('div', { style:{background:'#F0F9FF',border:'1px solid #BAE6FD',borderRadius:8,padding:'10px 14px',marginBottom:14,fontSize:12,color:'#0369A1'} },
      h('strong', null, 'How to upload: '),
      '1. Click "Download Template" to get the Excel format. ',
      '2. Fill in your data following the column headers. ',
      '3. Click "Upload Excel" to import. ',
      tab === 'architecture' ? 'Required columns for Architecture: plant_id, inverter_id, scb_id, string_id.' :
      tab === 'equipment' ? 'Required columns for Equipment: equipment_id, equipment_type.' :
      'Raw Data Upload is now Dynamic! Upload any SCADA Excel, and you will be prompted to map your columns to system fields (Timestamp, Inverter ID, SCB ID, SCB Current, DC Voltage). Disconnected String diagnostics will run automatically after import.'
    ),

    // ── Architecture view: animated diagram by default; table on demand ──
    tab === 'architecture' && h(Card, {
      title: 'Plant Architecture' + (archTotal ? ` (${archList.length}${archTotal > archList.length ? ' of ' + archTotal : ''} rows)` : ''),
      action: h('div', { style: { display: 'flex', gap: 6, background: 'rgba(255,255,255,0.04)', border: '1px solid var(--line)', borderRadius: 8, padding: 3 } },
        ['diagram', 'table'].map((v) => h('button', {
          key: v,
          type: 'button',
          onClick: () => setArchView(v),
          style: {
            padding: '4px 10px',
            border: 'none',
            borderRadius: 6,
            cursor: 'pointer',
            fontFamily: 'inherit',
            fontSize: 11,
            fontWeight: 700,
            background: archView === v ? 'var(--accent-soft)' : 'transparent',
            color: archView === v ? 'var(--accent)' : 'var(--text-soft)',
          }
        }, v === 'diagram' ? 'Diagram' : 'Table')),
      ),
    },
      archLoading
        ? h('div', { className:'empty-state' }, h(Spinner), ' Loading…')
        : archView === 'diagram' && window.PlantArchitectureViz
          ? h(window.PlantArchitectureViz, { rows: archList, plantId })
          : h(window.DataTable, {
              columns: architectureColumns,
              rows: archList,
              emptyMessage: 'No architecture data loaded. Download the template, fill it in, then upload using the button above.',
              filename: `plant_architecture_${plantId || 'all'}.csv`,
              maxHeight: 420,
              initialSortKey: 'inverter_id',
            }),
    ),

    // ── Equipment Specs: Add buttons + Table + Form modal ────────────────────
    tab === 'equipment' && h('div', null,
      h('div', { style: { display: 'flex', gap: 8, marginBottom: 12, flexWrap: 'wrap' } },
        h('button', { className: 'btn btn-primary', onClick: () => openEquipForm('inverter', null), disabled: !plantId }, 'Add Inverter Spec'),
        h('button', { className: 'btn btn-primary', onClick: () => openEquipForm('module', null), disabled: !plantId }, 'Add Module Spec'),
      ),
      h(Card, { title: `Equipment Specs  (${specList.length} records)` },
        loading
          ? h('div', { className:'empty-state' }, h(Spinner))
          : h(window.DataTable, {
              columns: equipmentColumns,
              rows: specList,
              emptyMessage: 'No equipment specs. Use "Add Inverter Spec" or "Add Module Spec" above, or upload Excel.',
              filename: `equipment_specs_${plantId || 'all'}.csv`,
              maxHeight: 420,
              initialSortKey: 'equipment_id',
            }),
      ),
      h(window.Modal, {
        title: editingSpec ? `Edit ${equipFormMode} – ${editingSpec.equipment_id}` : `Add ${equipFormMode === 'inverter' ? 'Inverter' : 'Module'} Spec`,
        open: equipFormOpen,
        onClose: () => setEquipFormOpen(false),
        footer: h('div', { style: { display: 'flex', gap: 8, justifyContent: 'flex-end' } },
          h('button', { className: 'btn btn-outline', onClick: () => setEquipFormOpen(false) }, 'Cancel'),
          h('button', { className: 'btn btn-primary', onClick: submitEquipForm, disabled: equipSaving }, equipSaving ? h(Spinner) : 'Save'),
        ),
      }, h('div', { style: { maxHeight: '70vh', overflow: 'auto', padding: 4 } },
        Input('Plant ID', 'plant_id', 'text', 'e.g. PLANT-01'),
        Input('Equipment ID', 'equipment_id', 'text', 'e.g. INV-01 or MOD-01'),
        equipFormMode === 'inverter' && h('div', { style: { marginTop: 12, marginBottom: 8, fontWeight: 600, fontSize: 12 } }, 'Inverter parameters (optional)'),
        equipFormMode === 'inverter' && h('div', { style: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px 16px' } },
          Input('AC Capacity (kW)', 'ac_capacity_kw', 'number'),
          Input('DC Capacity (kWp)', 'dc_capacity_kwp', 'number'),
          Input('Rated / Euro efficiency (%)', 'rated_efficiency', 'number'),
          Input('Target efficiency (%)', 'target_efficiency', 'number'),
          Input('MPPT voltage min (V)', 'mppt_voltage_min', 'number'),
          Input('MPPT voltage max (V)', 'mppt_voltage_max', 'number'),
          Input('Voltage limit (V)', 'voltage_limit', 'number'),
          Input('Current set point (A)', 'current_set_point', 'number'),
          Input('Degradation loss (% of expected energy)', 'degradation_loss_pct', 'number'),
          Input('Temp coeff (loss per °C above 25, e.g. 0.004)', 'temp_coefficient_per_deg', 'number'),
        ),
        equipFormMode === 'inverter' && h('div', { style: { marginTop: 8, fontSize: 11, color: 'var(--text-muted)' } }, 'Loss Analysis uses degradation % and optional inverter temp coeff; module gamma STC is used if coeff is blank.'),
        equipFormMode === 'inverter' && h('div', { style: { marginTop: 8 } }, Input('Manufacturer', 'manufacturer'), Input('Model', 'model')),
        equipFormMode === 'module' && h('div', { style: { marginTop: 12, marginBottom: 8, fontWeight: 600, fontSize: 12 } }, 'Module parameters (optional)'),
        equipFormMode === 'module' && h('div', { style: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px 16px' } },
          Input('Impp (A)', 'impp', 'number'),
          Input('Vmpp (V)', 'vmpp', 'number'),
          Input('Pmax (W)', 'pmax', 'number'),
          Input('Isc (A)', 'isc', 'number'),
          Input('Voc (V)', 'voc', 'number'),
          Input('Module efficiency (%)', 'module_efficiency_pct', 'number'),
          Input('1st year degradation (%)', 'degradation_year1_pct', 'number'),
          Input('2nd year degradation (%)', 'degradation_year2_pct', 'number'),
          Input('Annual degradation after Y2 (%)', 'degradation_annual_pct', 'number'),
        ),
        equipFormMode === 'module' && h('div', { style: { marginTop: 8, fontWeight: 600, fontSize: 11 } }, 'STC:'),
        equipFormMode === 'module' && h('div', { style: { display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '8px 16px' } },
          Input('Alpha STC', 'alpha_stc', 'number'),
          Input('Beta STC', 'beta_stc', 'number'),
          Input('Gamma STC', 'gamma_stc', 'number'),
        ),
        equipFormMode === 'module' && h('div', { style: { marginTop: 8, fontWeight: 600, fontSize: 11 } }, 'NOCT:'),
        equipFormMode === 'module' && h('div', { style: { display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '8px 16px' } },
          Input('Alpha NOCT', 'alpha_noct', 'number'),
          Input('Beta NOCT', 'beta_noct', 'number'),
          Input('Gamma NOCT', 'gamma_noct', 'number'),
        ),
        equipFormMode === 'module' && h('div', { style: { marginTop: 8 } }, Input('Manufacturer', 'manufacturer'), Input('Model', 'model')),
        h('div', { style: { marginTop: 12 } },
          h('label', { className: 'form-label', style: { fontSize: 11 } }, 'Spec sheet (PDF/document)'),
          h('input', {
            type: 'file',
            accept: '.pdf,.xlsx,.xls,.doc,.docx,.png,.jpg,.jpeg',
            onChange: (e) => setEquipFormFile(e.target.files?.[0] || null),
            style: { fontSize: 12, marginTop: 4 },
          }),
          editingSpec && editingSpec.spec_sheet_path && h('div', { style: { fontSize: 11, color: 'var(--text-muted)', marginTop: 4 } }, 'Current file attached. Upload a new file to replace.'),
        ),
      )),
    ),

    // ── Raw Data Summary ──────────────────────────────────────────────────
    tab === 'rawdata' && h('div', { style: { display: 'grid', gap: 16 } },
      h(Card, { title: `Raw Data Summary` },
        loading
          ? h('div', { className:'empty-state' }, h(Spinner))
          : rawSummary === null && plantId
            ? h('div', { className:'empty-state' }, h(Spinner), ' Loading summary…')
            : !rawSummary || rawSummary.total_rows === 0
            ? h('div', { className:'empty-state' }, 'No raw data uploaded yet. Download the template and upload your time-series data.')
            : h('div', { style: { padding: '16px', display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '20px' } },
                h('div', null,
                  h('div', { style: { fontSize: 13, color: 'var(--text-muted)', marginBottom: 4 } }, 'Total Rows'),
                  h('div', { style: { fontSize: 24, fontWeight: 700, color: 'var(--text-main)' } }, rawSummary.total_rows.toLocaleString())
                ),
                h('div', null,
                  h('div', { style: { fontSize: 13, color: 'var(--text-muted)', marginBottom: 4 } }, 'Date Range'),
                  h('div', { style: { fontSize: 15, fontWeight: 600, color: 'var(--text-main)' } },
                    rawSummary.date_range ? `${rawSummary.date_range.from.split(' ')[0]} to ${rawSummary.date_range.to.split(' ')[0]}` : '-'
                  )
                ),
                h('div', null,
                  h('div', { style: { fontSize: 13, color: 'var(--text-muted)', marginBottom: 4 } }, 'Equipment Breakdown'),
                  h('div', { style: { display: 'flex', flexDirection: 'column', gap: 4 } },
                    Object.entries(rawSummary.levels || {}).map(([lvl, ct]) =>
                      h('div', { key: lvl, style: { display: 'flex', justifyContent: 'space-between', fontSize: 14 } },
                        h('span', { style: { textTransform: 'capitalize' } }, lvl),
                        h('strong', null, ct)
                      )
                    )
                  )
                )
              )
      ),
      h(Card, { title: 'Stored Raw Data Preview' },
        h('div', { style: { display: 'grid', gap: 12 } },
          h('div', { style: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 12 } },
            h('div', null,
              h('div', { style: { fontSize: 12, color: 'var(--text-muted)', marginBottom: 4 } }, 'Level'),
              h('select', {
                className: 'input',
                value: rawPreviewFilters.equipmentLevel,
                onChange: e => setRawPreviewFilters({ ...rawPreviewFilters, equipmentLevel: e.target.value }),
              },
                h('option', { value: '' }, 'All'),
                h('option', { value: 'wms' }, 'WMS (plant / wms rows)'),
                h('option', { value: 'inverter' }, 'Inverter'),
                h('option', { value: 'scb' }, 'SCB')
              )
            ),
            h('div', null,
              h('div', { style: { fontSize: 12, color: 'var(--text-muted)', marginBottom: 4 } }, 'Equipment ID'),
              h('input', {
                className: 'input',
                value: rawPreviewFilters.equipmentId,
                onChange: e => setRawPreviewFilters({ ...rawPreviewFilters, equipmentId: e.target.value }),
                placeholder: 'e.g. INV-01A / SCB-01A-01'
              })
            ),
            h('div', null,
              h('div', { style: { fontSize: 12, color: 'var(--text-muted)', marginBottom: 4 } }, 'Signal'),
              h('input', {
                className: 'input',
                value: rawPreviewFilters.signal,
                onChange: e => setRawPreviewFilters({ ...rawPreviewFilters, signal: e.target.value }),
                placeholder: 'e.g. dc_current'
              })
            ),
            h('div', null,
              h('div', { style: { fontSize: 12, color: 'var(--text-muted)', marginBottom: 4 } }, 'From'),
              h('input', {
                className: 'input',
                type: 'date',
                value: rawPreviewFilters.dateFrom,
                onChange: e => setRawPreviewFilters({ ...rawPreviewFilters, dateFrom: e.target.value })
              })
            ),
            h('div', null,
              h('div', { style: { fontSize: 12, color: 'var(--text-muted)', marginBottom: 4 } }, 'To'),
              h('input', {
                className: 'input',
                type: 'date',
                value: rawPreviewFilters.dateTo,
                onChange: e => setRawPreviewFilters({ ...rawPreviewFilters, dateTo: e.target.value })
              })
            )
          ),
          rawPreviewLoading
            ? h('div', { className:'empty-state' }, h(Spinner), ' Loading raw rows…')
            : h(window.DataTable, {
                columns: [
                  { key: 'timestamp', label: 'Timestamp' },
                  { key: 'equipment_level', label: 'Level' },
                  { key: 'equipment_id', label: 'Equipment ID' },
                  { key: 'signal', label: 'Signal' },
                  { key: 'value', label: 'Value' },
                  { key: 'source', label: 'Source' },
                ],
                rows: rawPreview,
                emptyMessage: 'No stored raw rows match the selected filters.',
                filename: `raw_data_preview_${plantId || 'plant'}.csv`,
                maxHeight: 420,
                initialSortKey: 'timestamp',
                initialSortDir: 'desc',
              })
        )
      )
    ),

    // ── Column Mapping Modal ──────────────────────────────────────────────────
    mapData && h(Modal, {
      title: 'Map Data Columns',
      onClose: () => { if(!uploading) { document.getElementById('upload-rawdata').value = ''; setMapData(null); } },
    },
      h('div', { style: { display: 'flex', flexDirection: 'column', gap: 15 } },
        h('p', { style: { fontSize: 13, color: 'var(--text-muted)' } },
          'We detected the columns in your Excel file. Please map them to the required system fields.'
        ),
        ['timestamp', 'inverter_id', 'scb_id', 'scb_current', 'dc_voltage'].map(f => h('div', { key: f, style: { display: 'flex', justifyContent: 'space-between', alignItems: 'center' } },
          h('label', { style: { fontWeight: 500, fontSize: 13 } }, f, h('span', { style: { color: 'red' } }, ' *')),
          h('select', { 
            className: 'input', 
            style: { width: 250, padding: 8, borderRadius: 4, border: '1px solid var(--border)' },
            value: mapping[f] || '',
            onChange: e => setMapping({...mapping, [f]: e.target.value})
          },
            h('option', { value: '' }, '-- Select Column --'),
            mapData.columns.map(c => h('option', { key: c, value: c }, c))
          )
        )),
        h('div', { style: { marginTop: 10, marginBottom: 5, fontWeight: 600, fontSize: 12, textTransform: 'uppercase', color: 'var(--text-light)' } }, 'Optional Fields'),
        ['inverter_status', 'string_count'].map(f => h('div', { key: f, style: { display: 'flex', justifyContent: 'space-between', alignItems: 'center' } },
          h('label', { style: { fontWeight: 500, fontSize: 13 } }, f),
          h('select', { 
            className: 'input', 
            style: { width: 250, padding: 8, borderRadius: 4, border: '1px solid var(--border)' },
            value: mapping[f] || '',
            onChange: e => setMapping({...mapping, [f]: e.target.value})
          },
            h('option', { value: '' }, '-- Optional Blank --'),
            mapData.columns.map(c => h('option', { key: c, value: c }, c))
          )
        )),
        h('div', { style: { display: 'flex', justifyContent: 'flex-end', gap: 10, marginTop: 20 } },
          h('button', { className: 'btn btn-outline', onClick: () => { document.getElementById('upload-rawdata').value = ''; setMapData(null); }, disabled: uploading }, 'Cancel'),
          h('button', { 
            className: 'btn btn-primary', 
            onClick: submitMapping,
            disabled: uploading || ['timestamp', 'inverter_id', 'scb_id', 'scb_current', 'dc_voltage'].some(f => !mapping[f])
          }, uploading ? h(Spinner) : 'Import & Run Diagnostics')
        )
      )
    )
  );
};
