console.info('[solar-trace] fault_page.js starting initialization');
const { useState, useEffect, useCallback, useMemo, useRef } = React;
// Recharts explicitly removed during ECharts migration
const { Card, Spinner, Badge, KpiCard, DataTable } = window;

if (!Card || !Spinner || !DataTable) {
  console.error('[solar-error] Critical UI components missing at fault_page.js load time', { Card, Spinner, DataTable });
}

function parseScbSlotInfo(scbId) {
  const match = String(scbId || '').match(/^SCB-([^-]+)-(\d+)$/i);
  if (!match) return null;
  return {
    invKey: `INV-${match[1]}`,
    invSuffix: match[1],
    slot: Number(match[2]),
  };
}

function deriveModuleWp(meta) {
  const dcCapacityKw = Number(meta?.dc_capacity_kw || 0);
  const modulesPerString = Number(meta?.modules_per_string || 0);
  const stringsPerScb = Number(meta?.strings_per_scb || 0);
  if (!dcCapacityKw || !modulesPerString || !stringsPerScb) return null;
  return Math.round((dcCapacityKw * 1000) / (modulesPerString * stringsPerScb));
}

/** Heatmap cell color: high ratio = green (best), low = red (worst), normalized to view min/max. */
function soilingRatioHeatColor(ratio, minR, maxR) {
  if (ratio == null || minR == null || maxR == null || !Number.isFinite(Number(ratio))) return '#1e293b';
  const lo = Number(minR);
  const hi = Number(maxR);
  if (!(hi > lo)) return '#64748b';
  let t = (Number(ratio) - lo) / (hi - lo);
  t = Math.max(0, Math.min(1, t));
  const r1 = 34, g1 = 197, b1 = 94;
  const r2 = 220, g2 = 38, b2 = 38;
  const r = Math.round(r1 + (r2 - r1) * (1 - t));
  const g = Math.round(g1 + (g2 - g1) * (1 - t));
  const b = Math.round(b1 + (b2 - b1) * (1 - t));
  return `rgb(${r},${g},${b})`;
}

/** Within-inverter percentile coloring when ratio is unavailable (same red→green as ratio). */
function soilingPctHeatColor(pct, minP, maxP) {
  if (pct == null || minP == null || maxP == null || !Number.isFinite(Number(pct))) return '#1e293b';
  const lo = Number(minP);
  const hi = Number(maxP);
  if (!(hi > lo)) return '#64748b';
  let t = (Number(pct) - lo) / (hi - lo);
  t = Math.max(0, Math.min(1, t));
  const r1 = 34, g1 = 197, b1 = 94;
  const r2 = 220, g2 = 38, b2 = 38;
  const r = Math.round(r1 + (r2 - r1) * (1 - t));
  const g = Math.round(g1 + (g2 - g1) * (1 - t));
  const b = Math.round(b1 + (b2 - b1) * (1 - t));
  return `rgb(${r},${g},${b})`;
}

/** PL KPIs from `pl-inverter-status` rows only — one legacy API call runs run_power_limitation once. */
function plSummaryFromInverterStatusRows(rows) {
  const data = Array.isArray(rows) ? rows : [];
  const totalLoss = data.reduce((s, r) => s + (Number(r.total_energy_loss_kwh) || 0), 0);
  return {
    active_pl_inverters: data.length,
    total_energy_loss_kwh: Math.round(totalLoss * 100) / 100,
    inverters: data.map((r) => ({
      inverter_id: r.inverter_id,
      energy_loss_kwh: r.total_energy_loss_kwh,
    })),
  };
}

const FAULT_SUB_LABELS = {
  overview: 'Overview',
  ds: 'Disconnected Strings',
  pl: 'Power Limitation',
  is: 'Inverter Shutdown',
  gb: 'Grid Breakdown',
  comm: 'Communication Issue',
  clip: 'Clipping',
  derate: 'Derating',
  scb_perf: 'Soiling',
  inv_eff: 'Inverter Efficiency',
  damage: 'ByPass Diode/Module Damage',
};

const CD_KIND_LABELS = {
  power_clip: 'Power Clipping',
  current_clip: 'Current Clipping',
  static_derate: 'Static Derating',
  dynamic_derate: 'Dynamic Derating',
  normal: 'Normal',
};
const CD_KIND_COLOR = {
  power_clip: '#ef4444',
  current_clip: '#f97316',
  static_derate: '#8b5cf6',
  dynamic_derate: '#3b82f6',
  normal: '#10b981',
};

window.FaultPage = ({ plantId, dateFrom: pFrom, dateTo: pTo, faultSub, onNavigateFaultSub }) => {
  const h = React.createElement;
  /** Single source of truth with URL hash — avoids drift vs sidebar selection. */
  const subView = faultSub || 'overview';
  const goFaultSub = useCallback((sub) => {
    const id = ['overview', 'ds', 'pl', 'is', 'gb', 'comm', 'clip', 'derate', 'scb_perf', 'inv_eff', 'damage'].includes(sub) ? sub : 'overview';
    if (typeof onNavigateFaultSub === 'function') onNavigateFaultSub(id);
    else window.location.hash = `fault-diagnostics/${id}`;
  }, [onNavigateFaultSub]);
  const [loading, setLoading] = useState(false);
  const [scbStatus, setScbStatus] = useState([]);
  const [archList, setArchList] = useState([]);
  const [dsSummary, setDsSummary] = useState(null);
  const [filterSummary, setFilterSummary] = useState(null);
  const [selectedFault, setSelectedFault] = useState(null);
  const [minCurrent, setMinCurrent] = useState('');
  const [maxCurrent, setMaxCurrent] = useState('');
  const [appliedMin, setAppliedMin] = useState('');
  const [appliedMax, setAppliedMax] = useState('');
  const today = new Date().toISOString().slice(0, 10);
  const [dateFrom, setDateFrom] = useState(pFrom || today);
  const [dateTo, setDateTo] = useState(pTo || today);
  const [dataLoading, setDataLoading] = useState(false);
  const [dsLoadError, setDsLoadError] = useState(false);
  const [scbPerfHeatmap, setScbPerfHeatmap] = useState(null);
  const [scbPerfLoading, setScbPerfLoading] = useState(false);
  const [soilingPlant, setSoilingPlant] = useState(null);
  const [soilingRankings, setSoilingRankings] = useState(null);
  const [soilingRankingsLoading, setSoilingRankingsLoading] = useState(false);
  const [soilingBarGroup, setSoilingBarGroup] = useState('inverter');
  const [soilingModalScb, setSoilingModalScb] = useState(null);
  const [soilingTabError, setSoilingTabError] = useState(null);
  const [plSummary, setPlSummary] = useState(null);
  const [plStatus, setPlStatus] = useState([]);
  const [plLoading, setPlLoading] = useState(false);
  const [selectedPlInv, setSelectedPlInv] = useState(null);
  const [isSummary, setIsSummary] = useState(null);
  const [isStatus, setIsStatus] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [selectedIsInv, setSelectedIsInv] = useState(null);
  const [gbSummary, setGbSummary] = useState(null);
  const [gbEvents, setGbEvents] = useState([]);
  const [gbLoading, setGbLoading] = useState(false);
  const [selectedGbEvent, setSelectedGbEvent] = useState(null);
  const [commSummary, setCommSummary] = useState(null);
  const [commEvents, setCommEvents] = useState([]);
  const [commLossByInverter, setCommLossByInverter] = useState([]);
  const [commLoading, setCommLoading] = useState(false);
  const [selectedCommItem, setSelectedCommItem] = useState(null);
  // ── Clipping & Derating (GTI-based virtual-power model) ───────────────────
  const [cdSummary, setCdSummary] = useState(null);
  const [cdRows, setCdRows] = useState([]);
  const [cdLoading, setCdLoading] = useState(false);
  const [selectedCdInverter, setSelectedCdInverter] = useState(null);
  const [cdTimeline, setCdTimeline] = useState([]);
  const [cdTimelineLoading, setCdTimelineLoading] = useState(false);
  // Review sign-off state
  const [reviews, setReviews]       = useState({});      // saved reviews keyed by scb_id
  const [localEdits, setLocalEdits] = useState({});      // unsaved in-progress edits
  const [savingScb, setSavingScb]   = useState(null);    // which scb_id is mid-save
  const dsReqSeqRef = useRef(0);

  const [unifiedFeed, setUnifiedFeed] = useState(null);
  /** Start true so Overview first paint shows loading, not a blank gap before the effect runs. */
  const [unifiedLoading, setUnifiedLoading] = useState(true);
  const [unifiedErr, setUnifiedErr] = useState(null);

  function isLikelyRequestAbort(e) {
    if (!e) return false;
    if (e.name === 'AbortError') return true;
    const m = String(e.message || e);
    if (/aborted|route change/i.test(m)) return true;
    return false;
  }
  const [metricToggle, setMetricToggle] = useState('mwh');
  const [ufEquipQ, setUfEquipQ] = useState('');
  const [ufLossMin, setUfLossMin] = useState('');
  const [ufLossMax, setUfLossMax] = useState('');
  const [ufTextQ, setUfTextQ] = useState('');
  const [ufExcludeCats, setUfExcludeCats] = useState([]);
  const [ufPage, setUfPage] = useState(1);
  const UF_PAGE_SIZE = 50;
  useEffect(() => {
    if (pFrom) setDateFrom(pFrom);
    if (pTo) setDateTo(pTo);
  }, [pFrom, pTo]);

  useEffect(() => {
    if (!plantId || !dateFrom || !dateTo) {
      setUnifiedLoading(false);
      setUnifiedFeed(null);
      setUnifiedErr(null);
      return undefined;
    }
    let cancelled = false;
    setUnifiedLoading(true);
    setUnifiedErr(null);
    window.SolarAPI.Faults.unifiedFeed(plantId, dateFrom, dateTo)
      .then((d) => {
        if (!cancelled) {
          setUnifiedFeed(d);
          setUnifiedErr(null);
        }
      })
      .catch((e) => {
        if (!cancelled) {
          if (isLikelyRequestAbort(e)) {
            setUnifiedErr(null);
            return;
          }
          setUnifiedErr(e.message || String(e));
          setUnifiedFeed(null);
        }
      })
      .finally(() => {
        if (!cancelled) setUnifiedLoading(false);
      });
    return () => { cancelled = true; };
  }, [plantId, dateFrom, dateTo]);

  const loadData = useCallback(() => {
    if (!plantId) return;
    const reqId = ++dsReqSeqRef.current;
    setDataLoading(true);
    setDsLoadError(false);

    window.SolarAPI.Metadata.architectureCompact(plantId)
      .then(arch => { if (dsReqSeqRef.current === reqId) setArchList(arch || []); })
      .catch(() => { if (dsReqSeqRef.current === reqId) setArchList([]); });

    const fetchStatus = async () => {
      try {
        const res = await window.SolarAPI.Faults.dsScbStatus(plantId, dateFrom, dateTo);
        if (dsReqSeqRef.current !== reqId) return;
        setScbStatus(res.data || []);
        setDsLoadError(false);
      } catch (e) {
        // one retry to absorb transient network/backend hiccups
        try {
          await new Promise(r => setTimeout(r, 450));
          const retryRes = await window.SolarAPI.Faults.dsScbStatus(plantId, dateFrom, dateTo);
          if (dsReqSeqRef.current !== reqId) return;
          setScbStatus(retryRes.data || []);
          setDsLoadError(false);
        } catch (e2) {
          if (dsReqSeqRef.current !== reqId) return;
          console.error(e2);
          setScbStatus([]);
          setDsLoadError(true);
        }
      } finally {
        if (dsReqSeqRef.current === reqId) setDataLoading(false);
      }
    };
    fetchStatus();

    window.SolarAPI.Faults.dsSummary(plantId, dateFrom, dateTo)
      .then(data => { if (dsReqSeqRef.current === reqId) setDsSummary(data); })
      .catch(() => { if (dsReqSeqRef.current === reqId) setDsSummary(null); });

    window.SolarAPI.Faults.dsFilterSummary(plantId, dateFrom, dateTo)
      .then(data => { if (dsReqSeqRef.current === reqId) setFilterSummary(data); })
      .catch(() => { if (dsReqSeqRef.current === reqId) setFilterSummary(null); });

    // Load saved reviews for this plant + date range
    window.SolarAPI.Faults.getReviews(plantId, dateFrom, dateTo)
      .then(data => {
        if (dsReqSeqRef.current !== reqId) return;
        setReviews(data || {});
        setLocalEdits({});
      })
      .catch(() => { if (dsReqSeqRef.current === reqId) setReviews({}); });
  }, [plantId, dateFrom, dateTo]);

  useEffect(loadData, [loadData]);

  // Prefetch raw-derived fault tabs (PL / IS / GB) so switching tabs hits warm server + memory cache.
  useEffect(() => {
    if (!plantId || !dateFrom || !dateTo) return;
    window.SolarAPI.Faults.runtimeTabsBundle(plantId, dateFrom, dateTo).catch(() => {});
  }, [plantId, dateFrom, dateTo]);

  useEffect(() => {
    if (subView !== 'pl' || !plantId) return;
    setPlLoading(true);
    const apply = (summary, rows) => {
      setPlSummary(summary || null);
      setPlStatus(rows || []);
      setPlLoading(false);
    };
    const loadLegacy = () =>
      window.SolarAPI.Faults.plInverterStatus(plantId, dateFrom, dateTo)
        .then((status) => {
          const rows = (status && status.data) || [];
          apply(plSummaryFromInverterStatusRows(rows), rows);
        });

    // Prefer /pl-page (one run_power_limitation); fall back to pl-inverter-status only (still one server run).
    window.SolarAPI.Faults.plPage(plantId, dateFrom, dateTo)
      .then((res) => {
        if (res && res.summary != null && res.inverter_status) {
          apply(res.summary, res.inverter_status.data || []);
        } else {
          throw new Error('pl-page response shape');
        }
      })
      .catch(() => loadLegacy())
      .catch(() => apply(null, []));
  }, [subView, plantId, dateFrom, dateTo]);

  useEffect(() => {
    if (subView !== 'is' || !plantId) return;
    setIsLoading(true);
    Promise.all([
      window.SolarAPI.Faults.isSummary(plantId, dateFrom, dateTo),
      window.SolarAPI.Faults.isInverterStatus(plantId, dateFrom, dateTo)
    ]).then(([summary, status]) => {
      setIsSummary(summary || null);
      setIsStatus((status && status.data) || []);
      setIsLoading(false);
    }).catch(() => { setIsSummary(null); setIsStatus([]); setIsLoading(false); });
  }, [subView, plantId, dateFrom, dateTo]);

  useEffect(() => {
    if (subView !== 'gb' || !plantId) return;
    setGbLoading(true);
    Promise.all([
      window.SolarAPI.Faults.gbSummary(plantId, dateFrom, dateTo),
      window.SolarAPI.Faults.gbEvents(plantId, dateFrom, dateTo)
    ]).then(([summary, events]) => {
      setGbSummary(summary || null);
      setGbEvents((events && events.data) || []);
      setGbLoading(false);
    }).catch(() => { setGbSummary(null); setGbEvents([]); setGbLoading(false); });
  }, [subView, plantId, dateFrom, dateTo]);

  useEffect(() => {
    if (subView !== 'comm' || !plantId) return;
    setCommLoading(true);
    Promise.all([
      window.SolarAPI.Faults.commSummary(plantId, dateFrom, dateTo),
      window.SolarAPI.Faults.commEvents(plantId, dateFrom, dateTo),
      window.SolarAPI.Faults.commInverterLoss(plantId, dateFrom, dateTo),
    ]).then(([summary, events, losses]) => {
      setCommSummary(summary || null);
      setCommEvents((events && events.data) || []);
      setCommLossByInverter((losses && losses.data) || []);
      setCommLoading(false);
    }).catch(() => {
      setCommSummary(null);
      setCommEvents([]);
      setCommLossByInverter([]);
      setCommLoading(false);
    });
  }, [subView, plantId, dateFrom, dateTo]);

  // Same backend call powers both the Clipping and the Derating tabs — UI
  // just filters the rows/KPIs it shows based on which sub-tab is active.
  useEffect(() => {
    if ((subView !== 'clip' && subView !== 'derate') || !plantId) return;
    setCdLoading(true);
    window.SolarAPI.Faults.cdPage(plantId, dateFrom, dateTo)
      .then((page) => {
        setCdSummary((page && page.summary) || null);
        setCdRows((page && page.inverter_status) || []);
      })
      .catch(() => { setCdSummary(null); setCdRows([]); })
      .finally(() => setCdLoading(false));
  }, [subView, plantId, dateFrom, dateTo]);

  // Investigate modal — lazy-load timeline for the chosen inverter.
  useEffect(() => {
    if (!selectedCdInverter || !plantId) return;
    setCdTimelineLoading(true);
    setCdTimeline([]);
    window.SolarAPI.Faults.cdTimeline(plantId, selectedCdInverter, dateFrom, dateTo)
      .then((res) => setCdTimeline((res && res.data) || []))
      .catch(() => setCdTimeline([]))
      .finally(() => setCdTimelineLoading(false));
  }, [selectedCdInverter, plantId, dateFrom, dateTo]);

  useEffect(() => {
    if (subView !== 'scb_perf' || !plantId) return;
    setScbPerfLoading(true);
    setSoilingTabError(null);
    Promise.allSettled([
      window.SolarAPI.Faults.scbPerformanceHeatmap(plantId, dateFrom, dateTo),
      window.SolarAPI.Faults.soilingPlantPr(plantId, dateFrom, dateTo),
    ]).then((results) => {
      const errs = [];
      const hm = results[0].status === 'fulfilled' ? results[0].value : null;
      const pl = results[1].status === 'fulfilled' ? results[1].value : null;
      if (results[0].status === 'rejected') {
        const e = results[0].reason;
        console.warn('[Soiling] Heatmap API:', e);
        errs.push(`Heatmap: ${(e && e.message) ? e.message : String(e)}`);
      }
      if (results[1].status === 'rejected') {
        const e = results[1].reason;
        console.warn('[Soiling] Plant PR / KPIs API:', e);
        errs.push(`KPIs: ${(e && e.message) ? e.message : String(e)}`);
      }
      setSoilingTabError(errs.length ? errs.join(' · ') : null);
      setScbPerfHeatmap(hm);
      setSoilingPlant(pl);
    }).finally(() => setScbPerfLoading(false));
  }, [subView, plantId, dateFrom, dateTo]);

  useEffect(() => {
    if (subView !== 'scb_perf' || !plantId) return;
    // Clear stale data immediately so chart doesn't show wrong group's labels while loading
    setSoilingRankings(null);
    setSoilingRankingsLoading(true);
    window.SolarAPI.Faults.soilingRankings(plantId, dateFrom, dateTo, soilingBarGroup)
      .then((rk) => {
        setSoilingRankings(rk);
      })
      .catch((e) => {
        console.warn('[Soiling] Rankings API:', e);
        setSoilingRankings({ group_by: soilingBarGroup, rows: [] });
        setSoilingTabError((prev) => (prev ? `${prev} · Rankings: ${(e && e.message) || e}` : `Rankings: ${(e && e.message) || e}`));
      })
      .finally(() => setSoilingRankingsLoading(false));
  }, [subView, plantId, dateFrom, dateTo, soilingBarGroup]);

  const scbStringsMap = useMemo(() => {
    const map = {};
    archList.forEach(a => {
      if (!map[a.scb_id]) map[a.scb_id] = a.strings_per_scb;
    });
    return map;
  }, [archList]);

  // Computed locally from architecture — does not depend on API cache
  const totalScbs = useMemo(() => archList.filter(a => !a.spare_flag).length, [archList]);
  const communicatingScbs = useMemo(() => {
    const spareSet = new Set(archList.filter(a => a.spare_flag).map(a => a.scb_id));
    return new Set(scbStatus.filter(s => !spareSet.has(s.scb_id)).map(s => s.scb_id)).size;
  }, [archList, scbStatus]);
  const constantBadScbSet = useMemo(() => new Set(filterSummary?.constant_scbs || []), [filterSummary]);

  const allScbs = useMemo(() => {
    return scbStatus
      .sort((a, b) => a.scb_id.localeCompare(b.scb_id))
      .filter(d => {
        if (constantBadScbSet.has(d.scb_id)) return false;
        const strings = scbStringsMap[d.scb_id] || 28;
        const scbCurrent = (d.virtual_string_current || 0) * strings;
        if (appliedMin !== '' && scbCurrent < Number(appliedMin)) return false;
        if (appliedMax !== '' && scbCurrent > Number(appliedMax)) return false;
        return true;
      });
  }, [scbStatus, scbStringsMap, appliedMin, appliedMax, constantBadScbSet]);

  const activeFaults = useMemo(() => allScbs.filter(d => Number(d.missing_strings || 0) > 0), [allScbs]);

  const energyChartData = useMemo(() => {
    if (!dsSummary || !dsSummary.daily_energy_series) return [];
    return dsSummary.daily_energy_series.map(d => ({
      timestamp: d.date,
      total_energy_loss: d.energy_loss_kwh || 0,
    }));
  }, [dsSummary]);

  const scbStatusById = useMemo(() => {
    const m = {};
    (scbStatus || []).forEach(d => { m[d.scb_id] = d; });
    return m;
  }, [scbStatus]);
  const archByInv = useMemo(() => {
    const byInv = {};
    (archList || []).forEach(a => {
      const parsed = parseScbSlotInfo(a.scb_id);
      const inv = a.inverter_id || parsed?.invKey;
      const slot = parsed?.slot;
      if (!inv || !slot) return;
      if (!byInv[inv]) {
        byInv[inv] = {
          invSuffix: parsed?.invSuffix || String(inv).replace(/^INV-/, ''),
          maxSlot: 0,
          slots: {},
        };
      }
      byInv[inv].slots[slot] = { ...a, slot };
      byInv[inv].maxSlot = Math.max(byInv[inv].maxSlot, slot);
    });
    const filled = {};
    Object.keys(byInv).forEach(inv => {
      const invMeta = byInv[inv];
      const invSuffix = invMeta.invSuffix || String(inv).replace(/^INV-/, '');
      filled[inv] = Array.from({ length: invMeta.maxSlot }, (_, idx) => {
        const slot = idx + 1;
        return invMeta.slots[slot] || {
          scb_id: `SCB-${invSuffix}-${String(slot).padStart(2, '0')}`,
          inverter_id: inv,
          slot,
          spare_flag: true,
          inferred_spare: true,
          strings_per_scb: null,
          modules_per_string: null,
          dc_capacity_kw: null,
        };
      });
    });
    return filled;
  }, [archList]);
  const inverters = useMemo(() => {
    const fromArch = Object.keys(archByInv || {}).sort();
    if (fromArch.length) return fromArch;
    const fromData = Array.from(new Set(allScbs.map(d => d.inverter_id || d.scb_id.substring(0, d.scb_id.lastIndexOf('-'))))).sort();
    return fromData;
  }, [allScbs, archByInv]);
  const scbsPerInvDict = useMemo(() => {
    const d = {};
    inverters.forEach(inv => { d[inv] = []; });
    allScbs.forEach(row => {
      const inv = row.inverter_id || row.scb_id.substring(0, row.scb_id.lastIndexOf('-'));
      if (d[inv]) d[inv].push(row);
    });
    inverters.forEach(inv => { if (d[inv]) d[inv].sort((a, b) => a.scb_id.localeCompare(b.scb_id)); });
    return d;
  }, [inverters, allScbs]);
  const maxScbs = useMemo(() => {
    let m = 0;
    if (archByInv && Object.keys(archByInv).length) {
      Object.values(archByInv).forEach(arr => { if (arr.length > m) m = arr.length; });
    }
    return m;
  }, [archByInv]);

  const invMap = {};
  activeFaults.forEach(d => {
    let inv = d.inverter_id || d.scb_id.substring(0, d.scb_id.lastIndexOf('-'));
    if (!invMap[inv]) invMap[inv] = { inverter_id: inv, missing_strings: 0 };
    invMap[inv].missing_strings += d.missing_strings || 0;
  });
  const invChartData = Object.values(invMap).sort((a, b) => a.inverter_id.localeCompare(b.inverter_id));

  const faultColumns = [
    {
      key: 'scb_id',
      label: 'SCB ID',
      render: (row) => h('strong', null, row.scb_id),
      csvValue: (row) => row.scb_id,
    },
    {
      key: 'fault_status',
      label: 'Status',
      render: () => h(Badge, { type: 'red' }, 'DS Fault'),
      csvValue: () => 'DS Fault',
      sortable: false,
    },
    {
      key: 'missing_strings',
      label: 'Disconnected Strings',
      sortValue: (row) => row.missing_strings ?? -Infinity,
      render: (row) => {
        const ms = row.missing_strings ?? 0;
        const total = scbStringsMap[row.scb_id] || 1;
        const pct = total > 0 ? ms / total : 0;
        const showCaution = pct > 0.4;
        return h('span', { style: { display: 'inline-flex', alignItems: 'center', gap: 6 } },
          h('strong', { style: { color: '#EF4444' } }, ms),
          showCaution && h('span', {
            title: 'This fault may be incorrect due to incorrect data. Please improve the data.',
            style: { cursor: 'help', fontSize: 14, color: '#f59e0b', lineHeight: 1 }
          }, '\u26a0')
        );
      },
      csvValue: (row) => row.missing_strings,
    },
    {
      key: 'energy_loss_kwh',
      label: 'Energy Loss (kWh)',
      sortValue: (row) => row.energy_loss_kwh ?? -Infinity,
      render: (row) => (row.energy_loss_kwh || 0).toFixed(2),
      csvValue: (row) => (row.energy_loss_kwh || 0).toFixed(2),
    },
    {
      key: 'active_since',
      label: 'Active Since',
      sortValue: (row) => row.active_since || '',
      render: (row) => row.active_since || '—',
      csvValue: (row) => row.active_since || '',
    },
    {
      key: 'recurring_days',
      label: 'Recurring Days',
      sortValue: (row) => row.recurring_days ?? 0,
      render: (row) => Number(row.recurring_days || 0),
      csvValue: (row) => Number(row.recurring_days || 0),
    },
    {
      key: 'action',
      label: 'Investigate',
      sortable: false,
      render: (row) => h('button', {
        className: 'btn btn-primary',
        style: { padding: '4px 8px', fontSize: 12 },
        onClick: () => setSelectedFault(row.scb_id)
      }, 'Investigate'),
      csvValue: () => 'Investigate',
    },
    {
      key: 'review',
      label: 'Review',
      sortable: false,
      csvValue: (row) => (reviews[row.scb_id] || {}).review_status || '',
      render: (row) => {
        const STATUS_OPTS = [
          { value: '',            label: 'Mark review' },
          { value: 'valid_fault', label: 'Valid fault' },
          { value: 'other_fault', label: 'Other fault' },
          { value: 'no_fault',    label: 'No fault' },
        ];
        const STATUS_COLOR = { valid_fault: '#22c55e', other_fault: '#f59e0b', no_fault: '#ef4444' };
        const LABEL_MAP    = { valid_fault: 'Valid fault', other_fault: 'Other fault', no_fault: 'No fault' };
        const saved   = reviews[row.scb_id]    || {};
        const local   = localEdits[row.scb_id] || {};
        const status  = local.status  !== undefined ? local.status  : (saved.review_status || '');
        const remarks = local.remarks !== undefined ? local.remarks : (saved.remarks || '');
        const isDirty = status !== (saved.review_status || '') || remarks !== (saved.remarks || '');
        const isSaving = savingScb === row.scb_id;

        const setLocal = (patch) =>
          setLocalEdits(prev => ({ ...prev, [row.scb_id]: { ...(prev[row.scb_id] || {}), ...patch } }));

        const handleSave = () => {
          if (!status) return;
          setSavingScb(row.scb_id);
          window.SolarAPI.Faults.saveReview({
            plant_id: plantId, scb_id: row.scb_id,
            date_from: dateFrom, date_to: dateTo,
            review_status: status, remarks,
          }).then(() => {
            setReviews(prev => ({ ...prev, [row.scb_id]: { review_status: status, remarks, reviewed_by: '', reviewed_at: new Date().toISOString() } }));
            setLocalEdits(prev => { const n = {...prev}; delete n[row.scb_id]; return n; });
          }).catch(err => alert('Save failed: ' + (err.message || err)))
            .finally(() => setSavingScb(null));
        };

        return h('div', { style: { display: 'flex', flexDirection: 'column', gap: 2, minWidth: 130 } },
          saved.review_status && !isDirty && h('span', {
            style: { fontSize: 11, fontWeight: 700, color: STATUS_COLOR[saved.review_status], lineHeight: 1 }
          }, LABEL_MAP[saved.review_status] || saved.review_status),
          saved.reviewed_by && !isDirty && h('span', {
            style: { fontSize: 10, color: 'var(--text-muted)', lineHeight: 1 }
          }, 'by ' + saved.reviewed_by),
          h('select', {
            value: status,
            onChange: e => setLocal({ status: e.target.value }),
            style: { fontSize: 11, padding: '2px 4px', borderRadius: 4, border: '1px solid var(--border)', background: 'var(--panel)', color: 'var(--text)', cursor: 'pointer', height: 22 },
          }, STATUS_OPTS.map(o => h('option', { key: o.value, value: o.value }, o.label))),
          h('input', {
            type: 'text', placeholder: 'Remarks...',
            value: remarks,
            onChange: e => setLocal({ remarks: e.target.value }),
            style: { fontSize: 11, padding: '2px 4px', borderRadius: 4, border: '1px solid var(--border)', background: 'var(--panel)', color: 'var(--text)', height: 22 },
          }),
          isDirty && status && h('button', {
            onClick: handleSave,
            disabled: isSaving,
            className: 'btn btn-primary',
            style: { padding: '2px 8px', fontSize: 11, opacity: isSaving ? 0.6 : 1, height: 22 },
          }, isSaving ? 'Saving…' : 'Save Review'),
        );
      },
    },
  ];

  const renderModal = () => {
    if (!selectedFault) return null;
    return ReactDOM.createPortal(
      h(FaultDetailModal, {
        scbId: selectedFault,
        scbStringsMap,
        archList: archList || [],
        plantId,
        dateFrom,
        dateTo,
        summaryEnergyNote: dsSummary?.energy_note || null,
        onClose: () => setSelectedFault(null)
      }),
      document.body
    );
  };

  const openUnifiedInvestigate = useCallback((inv) => {
    if (!inv || !inv.kind) return;
    if (inv.kind === 'ds') setSelectedFault(inv.scb_id);
    else if (inv.kind === 'pl') setSelectedPlInv(inv.inverter_id);
    else if (inv.kind === 'is') setSelectedIsInv(inv.inverter_id);
    else if (inv.kind === 'gb') setSelectedGbEvent(String(inv.event_id));
    else if (inv.kind === 'comm') setSelectedCommItem({
      equipmentLevel: inv.equipment_level,
      equipmentId: inv.equipment_id,
      issueKind: inv.issue_kind || null,
      inverterId: inv.inverter_id || null,
    });
    else if (inv.kind === 'scb_perf') setSoilingModalScb(inv.scb_id);
  }, []);

  useEffect(() => { setUfPage(1); }, [plantId, dateFrom, dateTo, unifiedFeed, ufExcludeCats, ufEquipQ, ufLossMin, ufLossMax, ufTextQ]);

  const unifiedFiltered = useMemo(() => {
    const rows = unifiedFeed?.rows || [];
    let r = rows;
    if (ufExcludeCats.length) r = r.filter((row) => !ufExcludeCats.includes(row.category));
    const eq = ufEquipQ.trim().toLowerCase();
    if (eq) r = r.filter((row) => String(row.equipment_id || '').toLowerCase().includes(eq));
    const lo = ufLossMin === '' ? null : Number(ufLossMin);
    const hi = ufLossMax === '' ? null : Number(ufLossMax);
    if (lo != null && Number.isFinite(lo)) r = r.filter((row) => (Number(row.severity_energy_kwh) || 0) >= lo);
    if (hi != null && Number.isFinite(hi)) r = r.filter((row) => (Number(row.severity_energy_kwh) || 0) <= hi);
    const tq = ufTextQ.trim().toLowerCase();
    if (tq) {
      r = r.filter((row) => {
        const blob = [row.category_label, row.status, row.equipment_id, row.occurred_at, row.duration_note]
          .map((x) => String(x || '').toLowerCase()).join(' ');
        return blob.includes(tq);
      });
    }
    return r;
  }, [unifiedFeed, ufExcludeCats, ufEquipQ, ufLossMin, ufLossMax, ufTextQ]);

  const tileCategories = unifiedFeed?.categories || [];
  const sumTileMwh = useMemo(() => tileCategories.reduce((s, c) => s + (Number(c.loss_mwh) || 0), 0), [tileCategories]);
  const sumTileCnt = useMemo(() => tileCategories.reduce((s, c) => s + (Number(c.fault_count) || 0), 0), [tileCategories]);

  const tileValueForCat = (c) => {
    if (metricToggle === 'mwh') return { val: Number(c.loss_mwh || 0).toFixed(3), unit: 'MWh' };
    if (metricToggle === 'count') return { val: String(c.fault_count ?? 0), unit: 'faults' };
    if (sumTileMwh > 1e-9) return { val: `${(((Number(c.loss_mwh) || 0) / sumTileMwh) * 100).toFixed(1)}`, unit: '% of MWh' };
    if (sumTileCnt > 0) return { val: `${(((Number(c.fault_count) || 0) / sumTileCnt) * 100).toFixed(1)}`, unit: '% of faults' };
    return { val: '—', unit: '%' };
  };

  const totalTileValue = () => {
    if (metricToggle === 'mwh') return { val: sumTileMwh.toFixed(3), unit: 'MWh' };
    if (metricToggle === 'count') return { val: String(sumTileCnt), unit: 'faults' };
    return { val: '100', unit: '%' };
  };

  const ufPageRows = useMemo(() => {
    const start = (ufPage - 1) * UF_PAGE_SIZE;
    return unifiedFiltered.slice(start, start + UF_PAGE_SIZE);
  }, [unifiedFiltered, ufPage, UF_PAGE_SIZE]);

  const ufTotalPages = Math.max(1, Math.ceil(unifiedFiltered.length / UF_PAGE_SIZE));

  const ufTableColumns = useMemo(() => [
    { key: 'occurred_at', label: 'Date / time', sortValue: (row) => String(row.occurred_at || ''), render: (row) => h('span', { style: { fontSize: 12 } }, String(row.occurred_at || '—').replace('T', ' ').slice(0, 19)), csvValue: (row) => row.occurred_at },
    { key: 'category_label', label: 'Category', sortValue: (row) => row.category_label || '', csvValue: (row) => row.category_label },
    { key: 'equipment_id', label: 'Equipment', render: (row) => h('strong', null, row.equipment_id || '—'), csvValue: (row) => row.equipment_id },
    { key: 'severity_energy_kwh', label: 'Loss (kWh)', sortValue: (row) => row.severity_energy_kwh ?? -Infinity, render: (row) => (Number(row.severity_energy_kwh) || 0).toFixed(2), csvValue: (row) => row.severity_energy_kwh },
    { key: 'severity_hours', label: 'Hours', sortValue: (row) => row.severity_hours ?? -Infinity, render: (row) => (row.severity_hours != null ? Number(row.severity_hours).toFixed(2) : '—'), csvValue: (row) => row.severity_hours },
    { key: 'duration_note', label: 'Duration / note', sortValue: (row) => row.duration_note || '', render: (row) => row.duration_note || '—', csvValue: (row) => row.duration_note },
    { key: 'status', label: 'Status', sortValue: (row) => row.status || '', csvValue: (row) => row.status },
    {
      key: 'action',
      label: 'Investigate',
      sortable: false,
      render: (row) => h('button', {
        type: 'button',
        className: 'btn btn-primary',
        style: { padding: '4px 8px', fontSize: 12 },
        onClick: () => openUnifiedInvestigate(row.investigate || {}),
      }, 'Investigate'),
      csvValue: () => 'Investigate',
    },
  ], [openUnifiedInvestigate]);

  const toggleUfCategory = (catId) => {
    setUfExcludeCats((prev) => (prev.includes(catId) ? prev.filter((x) => x !== catId) : [...prev, catId]));
  };

  return h('div', { style: { position: 'relative' } },
    h('div', { className: 'fault-breadcrumb' },
      h('button', {
        type: 'button',
        className: 'crumb-link',
        onClick: () => goFaultSub('overview'),
      }, 'Fault Diagnostics'),
      subView !== 'overview' && h(React.Fragment, null,
        h('span', { 'aria-hidden': true }, '/'),
        h('span', { style: { color: 'var(--text-soft)', fontWeight: 600 } }, FAULT_SUB_LABELS[subView] || subView),
      ),
    ),
    subView !== 'overview' && h('div', { style: { marginBottom: 14 } },
      h('button', {
        type: 'button',
        className: 'btn btn-outline',
        style: { padding: '6px 12px', fontSize: 12 },
        onClick: () => goFaultSub('overview'),
      }, '\u2190 Unified overview'),
    ),

    subView === 'overview' && h('div', { style: { marginBottom: 24 } },
      h('div', { className: 'fault-overview-toolbar' },
        h('div', { className: 'fault-overview-toolbar-text' },
          h('h2', { className: 'fault-overview-title' }, 'Unified fault overview'),
          h('p', { className: 'fault-overview-sub' }, 'Click a KPI card to open that fault category.'),
        ),
        h('div', { className: 'fault-overview-metric-seg', role: 'group', 'aria-label': 'KPI metric' },
          ['mwh', 'count', 'pct'].map((m) => h('button', {
            key: m,
            type: 'button',
            className: `btn fault-metric-btn ${metricToggle === m ? 'btn-primary' : 'btn-outline'}`,
            onClick: () => setMetricToggle(m),
          }, m === 'mwh' ? 'MWh' : m === 'count' ? '# Faults' : '% of total'))
        ),
      ),
      unifiedLoading && h('div', { className: 'fault-overview-loading', role: 'status' },
        h(Spinner),
        h('span', { className: 'fault-overview-loading-text' }, 'Loading unified fault overview…')),
      unifiedErr && !unifiedLoading && h('div', {
        role: 'alert',
        className: 'empty-state',
        style: { padding: 20, marginBottom: 16, borderRadius: 12, border: '1px solid var(--border)' },
      }, h('strong', { style: { display: 'block', marginBottom: 8 } }, 'Could not load unified feed'), unifiedErr),
      !unifiedLoading && !unifiedErr && unifiedFeed && h(React.Fragment, null,
        unifiedFeed._merged_on_client && h('div', {
          role: 'status',
          style: {
            marginBottom: 12,
            padding: '10px 14px',
            borderRadius: 10,
            fontSize: 12,
            background: 'rgba(245,158,11,0.12)',
            border: '1px solid rgba(245,158,11,0.4)',
            color: 'var(--text-soft)',
            lineHeight: 1.45,
          },
        },
          h('strong', { style: { color: '#fbbf24' } }, 'Compatibility mode: '),
          (unifiedFeed._merge_reason || 'Unified feed endpoint missing; data was loaded from individual fault APIs. Restart the backend to enable /api/faults/unified-feed.')),
        h('div', { className: 'fo-grid' },
          (() => {
            const CAT_STYLE = {
              total:    { color: '#3eb7df', tint: 'rgba(62,183,223,0.16)',  abbr: 'ALL' },
              ds:       { color: '#ef4444', tint: 'rgba(239,68,68,0.14)',   abbr: 'DS'  },
              pl:       { color: '#f59e0b', tint: 'rgba(245,158,11,0.14)',  abbr: 'PL'  },
              is:       { color: '#a855f7', tint: 'rgba(168,85,247,0.14)',  abbr: 'IS'  },
              gb:       { color: '#ec4899', tint: 'rgba(236,72,153,0.14)',  abbr: 'GB'  },
              comm:     { color: '#06b6d4', tint: 'rgba(6,182,212,0.14)',   abbr: 'CM'  },
              scb_perf: { color: '#84cc16', tint: 'rgba(132,204,22,0.14)',  abbr: 'SL'  },
              inv_eff:  { color: '#2563eb', tint: 'rgba(37,99,235,0.14)',   abbr: 'IE'  },
              damage:   { color: '#f97316', tint: 'rgba(249,115,22,0.14)',  abbr: 'BP'  },
            };
            const tv = totalTileValue();
            const sharePct = (v) => {
              if (metricToggle === 'mwh') return sumTileMwh > 1e-9 ? Math.min(100, (Number(v) || 0) / sumTileMwh * 100) : 0;
              if (metricToggle === 'count') return sumTileCnt > 0 ? Math.min(100, (Number(v) || 0) / sumTileCnt * 100) : 0;
              return Math.min(100, Number(v) || 0);
            };
            const totalStyle = CAT_STYLE.total;
            return [
              h('button', {
                key: 'total',
                type: 'button',
                className: 'fo-tile is-total',
                style: { '--fo-accent': totalStyle.color, '--fo-tint': totalStyle.tint, '--fo-share': '100%' },
                onClick: () => goFaultSub('overview'),
              },
                h('div', { className: 'fo-head' },
                  h('div', { className: 'fo-badge' }, totalStyle.abbr),
                  h('div', { className: 'fo-label' }, 'Total'),
                ),
                h('div', { className: 'fo-value' }, tv.val),
                h('div', { className: 'fo-unit' }, tv.unit),
                h('div', { className: 'fo-share' }, h('div', { className: 'fo-share-fill' })),
                h('div', { className: 'fo-note' },
                  metricToggle === 'pct'
                    ? 'Across all fault categories (same metric)'
                    : 'Sum of per-category values shown on cards'),
              ),
              ...tileCategories.map((c) => {
                const t = tileValueForCat(c);
                const style = CAT_STYLE[c.id] || { color: 'var(--accent)', tint: 'rgba(62,183,223,0.10)', abbr: '—' };
                const metricRaw = metricToggle === 'mwh' ? (Number(c.loss_mwh) || 0)
                                 : metricToggle === 'count' ? (Number(c.fault_count) || 0)
                                 : (sumTileMwh > 1e-9 ? (Number(c.loss_mwh) || 0) / sumTileMwh * 100 : 0);
                const share = sharePct(metricRaw);
                const isZero = metricRaw <= 1e-9;
                return h('button', {
                  key: c.id,
                  type: 'button',
                  className: `fo-tile ${isZero ? 'is-zero' : ''}`,
                  style: { '--fo-accent': style.color, '--fo-tint': style.tint, '--fo-share': share.toFixed(1) + '%' },
                  onClick: () => goFaultSub(c.id),
                  title: c.label + (c.metric_note ? ' — ' + c.metric_note : ''),
                },
                  h('div', { className: 'fo-head' },
                    h('div', { className: 'fo-badge' }, style.abbr),
                    h('div', { className: 'fo-label' }, c.label),
                  ),
                  h('div', { className: 'fo-value' }, t.val),
                  h('div', { className: 'fo-unit' }, t.unit),
                  h('div', { className: 'fo-share' }, h('div', { className: 'fo-share-fill' })),
                  c.metric_note && h('div', { className: 'fo-note' }, c.metric_note),
                );
              }),
            ];
          })(),
        ),
        h(Card, { title: 'All fault types — unified table' },
          h('div', { style: { display: 'flex', flexWrap: 'wrap', gap: 10, marginBottom: 12, alignItems: 'flex-end' } },
            h('div', { className: 'form-group', style: { marginBottom: 0 } },
              h('label', { className: 'form-label', style: { fontSize: 11 } }, 'Categories (toggle to hide)'),
              h('div', { style: { display: 'flex', flexWrap: 'wrap', gap: 6 } },
                tileCategories.map((c) => h('button', {
                  key: c.id,
                  type: 'button',
                  className: ufExcludeCats.includes(c.id) ? 'btn btn-outline' : 'btn btn-primary',
                  style: { padding: '4px 10px', fontSize: 11 },
                  onClick: () => toggleUfCategory(c.id),
                }, c.label)),
              ),
            ),
            h('div', { className: 'form-group', style: { marginBottom: 0 } },
              h('label', { className: 'form-label', style: { fontSize: 11 } }, 'Equipment contains'),
              h('input', {
                className: 'form-input',
                style: { width: 160 },
                value: ufEquipQ,
                placeholder: 'e.g. INV-1',
                onChange: (e) => setUfEquipQ(e.target.value),
              }),
            ),
            h('div', { className: 'form-group', style: { marginBottom: 0 } },
              h('label', { className: 'form-label', style: { fontSize: 11 } }, 'Min loss kWh'),
              h('input', {
                type: 'number',
                className: 'form-input',
                style: { width: 100 },
                value: ufLossMin,
                onChange: (e) => setUfLossMin(e.target.value),
              }),
            ),
            h('div', { className: 'form-group', style: { marginBottom: 0 } },
              h('label', { className: 'form-label', style: { fontSize: 11 } }, 'Max loss kWh'),
              h('input', {
                type: 'number',
                className: 'form-input',
                style: { width: 100 },
                value: ufLossMax,
                onChange: (e) => setUfLossMax(e.target.value),
              }),
            ),
            h('div', { className: 'form-group', style: { marginBottom: 0, flex: '1 1 200px' } },
              h('label', { className: 'form-label', style: { fontSize: 11 } }, 'Free text'),
              h('input', {
                className: 'form-input',
                style: { width: '100%', minWidth: 180 },
                value: ufTextQ,
                placeholder: 'Search category, status, time…',
                onChange: (e) => setUfTextQ(e.target.value),
              }),
            ),
          ),
          h(DataTable, {
            columns: ufTableColumns,
            rows: ufPageRows,
            emptyMessage: unifiedFiltered.length === 0 ? 'No rows match filters (or no faults in range).' : 'No rows on this page.',
            filename: `unified_faults_${plantId || 'plant'}_${dateFrom}_${dateTo}.csv`,
            maxHeight: 440,
            initialSortKey: 'severity_energy_kwh',
            initialSortDir: 'desc',
            compact: true,
          }),
          h('div', { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginTop: 10, flexWrap: 'wrap', gap: 8 } },
            h('span', { style: { fontSize: 12, color: 'var(--text-muted)' } },
              unifiedFiltered.length === 0 ? '0 rows' : `Rows ${(ufPage - 1) * UF_PAGE_SIZE + 1}–${Math.min(ufPage * UF_PAGE_SIZE, unifiedFiltered.length)} of ${unifiedFiltered.length} (after filters)`),
            h('div', { style: { display: 'flex', gap: 8, alignItems: 'center' } },
              h('button', { type: 'button', className: 'btn btn-outline', disabled: ufPage <= 1, onClick: () => setUfPage((p) => Math.max(1, p - 1)) }, 'Previous'),
              h('span', { style: { fontSize: 12 } }, `Page ${ufPage} / ${ufTotalPages}`),
              h('button', { type: 'button', className: 'btn btn-outline', disabled: ufPage >= ufTotalPages, onClick: () => setUfPage((p) => Math.min(ufTotalPages, p + 1)) }, 'Next'),
            ),
          ),
          (unifiedFeed.row_limit && (unifiedFeed.rows || []).length >= unifiedFeed.row_limit) && h('div', {
            style: { fontSize: 11, color: 'var(--text-muted)', marginTop: 8 },
          }, `Table shows up to ${unifiedFeed.row_limit} highest-severity rows from the server.`),
        ),
      ),
    ),

    subView === 'ds' && dataLoading && h('div', { style: { padding: 22, color: 'var(--text-soft)', display: 'flex', alignItems: 'center', gap: 10 } },
      h(Spinner, { size: 14 }), 'Loading fault diagnostics...'
    ),

    subView === 'ds' && !dataLoading && scbStatus.length === 0 && h('div', { className: 'empty-state', style: { minHeight: 320, background: 'var(--panel)', borderRadius: 12, border: '1px solid var(--line)' } },
      h('div', { style: { fontSize: 18, fontWeight: 700, marginBottom: 10 } }, dsLoadError ? 'Failed to load fault diagnostics' : 'No data for selected date range'),
      h('div', { style: { fontSize: 13, color: 'var(--text-muted)', marginBottom: 8 } }, `Error: ${dsLoadError ? 'DS_FETCH_ERROR' : 'DS_NO_DATA'}`),
      h('div', { style: { fontSize: 13, color: 'var(--text-muted)', marginBottom: 4 } }, `Plant: ${plantId || '-'}`),
      h('div', { style: { fontSize: 13, color: 'var(--text-muted)', marginBottom: 12 } }, `Range: ${dateFrom || '-'} to ${dateTo || '-'}`),
      h('div', { style: { fontSize: 12, color: 'var(--text-muted)', maxWidth: 560, lineHeight: 1.5 } },
        dsLoadError
          ? 'Possible reasons: temporary API/backend timeout, server restart in progress, or network issue. Please retry.'
          : 'Possible reasons: no records in fault_diagnostics for this date range, wrong plant selected, or backend is connected to different database.'
      ),
      h('button', { className: 'btn btn-primary', style: { marginTop: 18 }, onClick: loadData }, 'Try Again')
    ),

    subView === 'ds' && scbStatus.length > 0 && h(Card, { style: { marginBottom: 16 } },
      h('div', { style: { display: 'flex', gap: 16, alignItems: 'center' } },
        h('strong', null, 'Filter by Actual SCB Current Range (A):'),
        h('input', {
          type: 'number', placeholder: 'Min (e.g. 2)', value: minCurrent,
          onChange: e => setMinCurrent(e.target.value),
          className: 'form-input', style: { width: 140 }
        }),
        h('span', null, 'to'),
        h('input', {
          type: 'number', placeholder: 'Max (e.g. 200)', value: maxCurrent,
          onChange: e => setMaxCurrent(e.target.value),
          className: 'form-input', style: { width: 140 }
        }),
        h('button', {
          className: 'btn btn-primary',
          onClick: () => { setAppliedMin(minCurrent); setAppliedMax(maxCurrent); }
        }, 'Apply Filters'),
        h('button', {
          className: 'btn btn-outline',
          onClick: () => { setMinCurrent(''); setMaxCurrent(''); setAppliedMin(''); setAppliedMax(''); }
        }, 'Clear Filters')
      )
    ),

    subView === 'inv_eff' && (window.InverterEfficiencyAnalysis 
      ? h(window.InverterEfficiencyAnalysis, { plantId, dateFrom, dateTo })
      : h('div', { className: 'empty-state', style: { padding: 40 } }, 'Inverter Efficiency module could not be loaded. Please refresh.')),

    subView === 'pl' && h('div', { style: { display: 'flex', flexDirection: 'column', gap: 16 } },
      plLoading && h('div', { style: { padding: 24, textAlign: 'center', color: 'var(--text-soft)' } }, h(Spinner), ' Loading power limitation data…'),
      !plLoading && plSummary && h(Card, { title: 'Power Limitation Insights (10:00–15:00)' },
        h('div', { className: 'kpi-grid', style: { gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))' } },
          h(KpiCard, { label: 'Affected Inverters', value: plSummary.active_pl_inverters ?? 0, unit: '', color: 'var(--solar-orange)' }),
          h(KpiCard, { label: 'Total Energy Loss', value: (plSummary.total_energy_loss_kwh ?? 0).toFixed(1), unit: 'kWh', color: '#EF4444' })
        )
      ),
      !plLoading && h(Card, { title: `Active Power Limitation (${plStatus.length} Inverters Affected)` },
        h(DataTable, {
          columns: [
            { key: 'inverter_id', label: 'Inverter', render: (row) => h('strong', null, row.inverter_id), csvValue: (row) => row.inverter_id },
            { key: 'total_energy_loss_kwh', label: 'Energy Loss (kWh)', sortValue: (row) => row.total_energy_loss_kwh ?? -Infinity, render: (row) => (row.total_energy_loss_kwh ?? 0).toFixed(2), csvValue: (row) => (row.total_energy_loss_kwh ?? 0).toFixed(2) },
            { key: 'last_seen_fault', label: 'Last Seen Fault', csvValue: (row) => row.last_seen_fault || '' },
            { key: 'investigation_window_start', label: 'Investigation Window', render: (row) => {
              const s = row.investigation_window_start || '';
              const e = row.investigation_window_end || '';
              return s && e ? `${String(s).slice(0, 16)} – ${String(e).slice(0, 16)}` : (s || e || '—');
            }, csvValue: (row) => `${row.investigation_window_start || ''} – ${row.investigation_window_end || ''}` },
            { key: 'action', label: 'Action', sortable: false, render: (row) => h('button', { className: 'btn btn-primary', style: { padding: '4px 8px', fontSize: 12 }, onClick: () => setSelectedPlInv(row.inverter_id) }, 'Investigate'), csvValue: () => 'Investigate' },
          ],
          rows: plStatus,
          emptyMessage: 'No power limitation detected in 10:00–15:00 for the selected range.',
          filename: `power_limitation_${plantId || 'plant'}_${dateFrom}_${dateTo}.csv`,
          maxHeight: 420,
          initialSortKey: 'total_energy_loss_kwh',
          initialSortDir: 'desc',
        })
      ),
      !plLoading && plSummary && (plSummary.inverters || []).length > 0 && h(Card, { title: 'Generation Loss by Inverter (Power Limitation)' },
        (()=>{
          const data = plSummary.inverters || [];
          return h(window.EChart, {
            style: { width: '100%', height: 300 },
            option: {
              tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
              grid: { top: 10, right: 10, left: 40, bottom: 24 },
              xAxis: { type: 'category', data: data.map(d=>d.inverter_id), axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, axisLine: { lineStyle: { color: 'var(--line)' } }, axisTick: {show:false} },
              yAxis: { type: 'value', axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, splitLine: { lineStyle: { type: 'dashed', color: 'var(--line)' } } },
              series: [{ type: 'bar', name: 'Energy Loss (kWh)', data: data.map(d=>d.energy_loss_kwh), itemStyle: { color: '#f59e0b', borderRadius: [4,4,0,0] } }]
            }
          })
        })()
      ),
    ),

    subView === 'is' && h('div', { style: { display: 'flex', flexDirection: 'column', gap: 16 } },
      isLoading && h('div', { style: { padding: 24, textAlign: 'center', color: 'var(--text-soft)' } }, h(Spinner), ' Loading inverter shutdown data…'),
      !isLoading && isSummary && h(Card, { title: 'Inverter Shutdown Insights (AC=0 and Irradiance>5 W/m²)' },
        h('div', { className: 'kpi-grid', style: { gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))' } },
          h(KpiCard, { label: 'Affected Inverters', value: isSummary.active_shutdown_inverters ?? 0, unit: '', color: 'var(--solar-orange)' }),
          h(KpiCard, { label: 'Total Shutdown Hours', value: (isSummary.total_shutdown_hours ?? 0).toFixed(2), unit: 'h', color: '#EF4444' })
        )
      ),
      !isLoading && h(Card, { title: `Inverter Shutdown Events (${isStatus.length} Inverters)` },
        h(DataTable, {
          columns: [
            { key: 'inverter_id', label: 'Inverter', render: (row) => h('strong', null, row.inverter_id), csvValue: (row) => row.inverter_id },
            { key: 'shutdown_points', label: 'Shutdown Points', sortValue: (row) => row.shutdown_points ?? -Infinity, render: (row) => Number(row.shutdown_points || 0), csvValue: (row) => Number(row.shutdown_points || 0) },
            { key: 'shutdown_hours', label: 'Shutdown Hours', sortValue: (row) => row.shutdown_hours ?? -Infinity, render: (row) => Number(row.shutdown_hours || 0).toFixed(3), csvValue: (row) => Number(row.shutdown_hours || 0).toFixed(3) },
            { key: 'last_seen_shutdown', label: 'Last Seen', csvValue: (row) => row.last_seen_shutdown || '' },
            { key: 'investigation_window', label: 'Investigation Window', render: (row) => {
              const s = row.investigation_window_start || '';
              const e = row.investigation_window_end || '';
              return s && e ? `${String(s).slice(0, 16)} – ${String(e).slice(0, 16)}` : (s || e || '—');
            }, csvValue: (row) => `${row.investigation_window_start || ''} – ${row.investigation_window_end || ''}` },
            { key: 'action', label: 'Investigate', sortable: false, render: (row) => h('button', {
              className: 'btn btn-primary',
              style: { padding: '4px 8px', fontSize: 12 },
              onClick: () => setSelectedIsInv(row.inverter_id)
            }, 'Investigate'), csvValue: () => 'Investigate' },
          ],
          rows: isStatus,
          emptyMessage: 'No inverter shutdown detected for the selected range.',
          filename: `inverter_shutdown_${plantId || 'plant'}_${dateFrom}_${dateTo}.csv`,
          maxHeight: 420,
          initialSortKey: 'shutdown_hours',
          initialSortDir: 'desc',
        })
      ),
    ),

    subView === 'gb' && h('div', { style: { display: 'flex', flexDirection: 'column', gap: 16 } },
      gbLoading && h('div', { style: { padding: 24, textAlign: 'center', color: 'var(--text-soft)' } }, h(Spinner), ' Loading grid breakdown data...'),
      !gbLoading && gbSummary && h(Card, { title: 'Grid Breakdown Insights (All Inverters AC=0 and Irradiance>5 W/m²)' },
        h('div', { className: 'kpi-grid', style: { gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))' } },
          h(KpiCard, { label: 'Grid Breakdown Events', value: gbSummary.active_grid_events ?? 0, unit: '', color: 'var(--solar-orange)' }),
          h(KpiCard, { label: 'Total Breakdown Hours', value: (gbSummary.total_grid_breakdown_hours ?? 0).toFixed(2), unit: 'h', color: '#EF4444' })
        )
      ),
      !gbLoading && h(Card, { title: `Grid Breakdown Events (${gbEvents.length})` },
        h(DataTable, {
          columns: [
            { key: 'event_id', label: 'Event', render: (row) => h('strong', null, row.event_id), csvValue: (row) => row.event_id },
            { key: 'breakdown_points', label: 'Breakdown Points', sortValue: (row) => row.breakdown_points ?? -Infinity, render: (row) => Number(row.breakdown_points || 0), csvValue: (row) => Number(row.breakdown_points || 0) },
            { key: 'breakdown_hours', label: 'Breakdown Hours', sortValue: (row) => row.breakdown_hours ?? -Infinity, render: (row) => Number(row.breakdown_hours || 0).toFixed(3), csvValue: (row) => Number(row.breakdown_hours || 0).toFixed(3) },
            { key: 'last_seen_breakdown', label: 'Last Seen', csvValue: (row) => row.last_seen_breakdown || '' },
            { key: 'investigation_window', label: 'Investigation Window', render: (row) => {
              const s = row.investigation_window_start || '';
              const e = row.investigation_window_end || '';
              return s && e ? `${String(s).slice(0, 16)} - ${String(e).slice(0, 16)}` : (s || e || '-');
            }, csvValue: (row) => `${row.investigation_window_start || ''} - ${row.investigation_window_end || ''}` },
            { key: 'action', label: 'Investigate', sortable: false, render: (row) => h('button', {
              className: 'btn btn-primary',
              style: { padding: '4px 8px', fontSize: 12 },
              onClick: () => setSelectedGbEvent(row.event_id)
            }, 'Investigate'), csvValue: () => 'Investigate' },
          ],
          rows: gbEvents,
          emptyMessage: 'No grid breakdown detected for the selected range.',
          filename: `grid_breakdown_${plantId || 'plant'}_${dateFrom}_${dateTo}.csv`,
          maxHeight: 420,
          initialSortKey: 'breakdown_hours',
          initialSortDir: 'desc',
        })
      ),
    ),

    subView === 'comm' && h('div', { style: { display: 'flex', flexDirection: 'column', gap: 16 } },
      commLoading && h('div', { style: { padding: 24, textAlign: 'center', color: 'var(--text-soft)' } }, h(Spinner), ' Loading communication issue data...'),
      !commLoading && commSummary && h(Card, { title: 'Communication Issue Insights (WMS available, equipment signal missing)' },
        h('div', { className: 'kpi-grid', style: { gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))' } },
          h(KpiCard, { label: 'Total Communication Issues', value: commSummary.total_communication_issues ?? 0, unit: '', color: 'var(--solar-orange)' }),
          h(KpiCard, { label: 'Total Loss', value: (commSummary.total_loss_kwh ?? 0).toFixed(2), unit: 'kWh', color: '#EF4444' }),
          h(KpiCard, { label: 'Communication Hours', value: (commSummary.total_communication_hours ?? 0).toFixed(2), unit: 'h', color: '#06b6d4' }),
          h(KpiCard, { label: 'Plant Faults', value: commSummary.plant_issue_count ?? 0, unit: '', color: '#8b5cf6' }),
          h(KpiCard, { label: 'Inverter Faults', value: commSummary.inverter_issue_count ?? 0, unit: '', color: '#0ea5e9' }),
          h(KpiCard, { label: 'SCB Gaps', value: commSummary.scb_issue_count ?? 0, unit: '', color: '#14b8a6' }),
          h(KpiCard, { label: 'No-Loss Ingestion Gaps', value: commSummary.ingestion_gap_issue_count ?? 0, unit: '', color: '#64748b' })
        )
      ),
      !commLoading && h(Card, { title: `Communication Issues (${commEvents.length} Events)` },
        h(DataTable, {
          columns: [
            { key: 'equipment_level', label: 'Level', render: (row) => String(row.equipment_level || '').toUpperCase(), csvValue: (row) => row.equipment_level || '' },
            { key: 'equipment_id', label: 'Equipment', render: (row) => h('strong', null, row.equipment_id), csvValue: (row) => row.equipment_id || '' },
            { key: 'inverter_id', label: 'Parent Inverter', csvValue: (row) => row.inverter_id || '' },
            { key: 'status', label: 'Status', csvValue: (row) => row.status || '' },
            { key: 'communication_windows', label: 'Windows', sortValue: (row) => row.communication_windows ?? -Infinity, render: (row) => Number(row.communication_windows || 0), csvValue: (row) => Number(row.communication_windows || 0) },
            { key: 'communication_points', label: 'Points', sortValue: (row) => row.communication_points ?? -Infinity, render: (row) => Number(row.communication_points || 0), csvValue: (row) => Number(row.communication_points || 0) },
            { key: 'communication_hours', label: 'Hours', sortValue: (row) => row.communication_hours ?? -Infinity, render: (row) => Number(row.communication_hours || 0).toFixed(3), csvValue: (row) => Number(row.communication_hours || 0).toFixed(3) },
            { key: 'estimated_loss_kwh', label: 'Loss (kWh)', sortValue: (row) => row.estimated_loss_kwh ?? -Infinity, render: (row) => Number(row.estimated_loss_kwh || 0).toFixed(2), csvValue: (row) => Number(row.estimated_loss_kwh || 0).toFixed(2) },
            { key: 'investigation_window', label: 'Investigation Window', render: (row) => {
              const s = row.investigation_window_start || '';
              const e = row.investigation_window_end || '';
              return s && e ? `${String(s).slice(0, 16)} - ${String(e).slice(0, 16)}` : (s || e || '-');
            }, csvValue: (row) => `${row.investigation_window_start || ''} - ${row.investigation_window_end || ''}` },
            { key: 'action', label: 'Investigate', sortable: false, render: (row) => h('button', {
              className: 'btn btn-primary',
              style: { padding: '4px 8px', fontSize: 12 },
              onClick: () => setSelectedCommItem({ equipmentLevel: row.equipment_level, equipmentId: row.equipment_id, issueKind: row.issue_kind || null, inverterId: row.inverter_id || null })
            }, 'Investigate'), csvValue: () => 'Investigate' },
          ],
          rows: commEvents,
          emptyMessage: 'No communication issue detected for the selected range.',
          filename: `communication_issue_${plantId || 'plant'}_${dateFrom}_${dateTo}.csv`,
          maxHeight: 420,
          initialSortKey: 'estimated_loss_kwh',
          initialSortDir: 'desc',
        })
      ),
      !commLoading && commLossByInverter && commLossByInverter.length > 0 && h(Card, { title: 'Communication Loss by Inverter' },
        (()=>{
          return h(window.EChart, {
            style: { width: '100%', height: 300 },
            option: {
              tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
              grid: { top: 10, right: 10, left: 40, bottom: 24 },
              xAxis: { type: 'category', data: commLossByInverter.map(d=>d.inverter_id), axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, axisLine: { lineStyle: { color: 'var(--line)' } }, axisTick: {show:false} },
              yAxis: { type: 'value', axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, splitLine: { lineStyle: { type: 'dashed', color: 'var(--line)' } } },
              series: [{ type: 'bar', name: 'Loss (kWh)', data: commLossByInverter.map(d=>d.estimated_loss_kwh), itemStyle: { color: '#ef4444', borderRadius: [4,4,0,0] } }]
            }
          })
        })()
      ),
    ),

    // ── Shared Clipping / Derating renderer (tab = 'clip' | 'derate') ──
    (subView === 'clip' || subView === 'derate') && (() => {
      const isClip = subView === 'clip';
      const cat = isClip ? 'clip' : 'derate';
      const rowsForTab = cdRows.filter((r) => r.category === cat);
      const invLossForTab = (cdSummary?.inverter_loss || []).filter((r) => r.category === cat);
      const dq = cdSummary?.data_quality || {};

      // Colour theme per tab
      const accent = isClip ? '#ef4444' : '#8b5cf6';
      const bannerKpis = isClip
        ? [
            { label: 'Active Inverters',  value: cdSummary?.active_clip_inverters ?? 0,  unit: '',    color: accent },
            { label: 'Clipping Loss',     value: (cdSummary?.loss_clipping_total_kwh ?? 0).toFixed(2), unit: 'kWh', color: '#ef4444' },
            { label: 'Power Clipping',    value: (cdSummary?.loss_power_clipping_kwh ?? 0).toFixed(2), unit: 'kWh', color: CD_KIND_COLOR.power_clip },
            { label: 'Current Clipping',  value: (cdSummary?.loss_current_clipping_kwh ?? 0).toFixed(2), unit: 'kWh', color: CD_KIND_COLOR.current_clip },
          ]
        : [
            { label: 'Active Inverters',  value: cdSummary?.active_derate_inverters ?? 0, unit: '',   color: accent },
            { label: 'Derating Loss',     value: (cdSummary?.loss_derating_total_kwh ?? 0).toFixed(2), unit: 'kWh', color: '#8b5cf6' },
            { label: 'Static Derating',   value: (cdSummary?.loss_static_derating_kwh  ?? 0).toFixed(2), unit: 'kWh', color: CD_KIND_COLOR.static_derate },
            { label: 'Dynamic Derating',  value: (cdSummary?.loss_dynamic_derating_kwh ?? 0).toFixed(2), unit: 'kWh', color: CD_KIND_COLOR.dynamic_derate },
          ];

      // Columns depend on which sub-tab we're on
      const clipCols = [
        { key: 'inverter_id', label: 'Inverter', render: (row) => h('strong', null, row.inverter_id), csvValue: (row) => row.inverter_id },
        { key: 'rated_ac_kw', label: 'Rated (kW)', sortValue: (row) => row.rated_ac_kw ?? -Infinity, render: (row) => Number(row.rated_ac_kw || 0).toFixed(1), csvValue: (row) => Number(row.rated_ac_kw || 0).toFixed(2) },
        { key: 'k_factor', label: 'k (kW · W⁻¹·m²)', sortValue: (row) => row.k_factor ?? -Infinity, render: (row) => Number(row.k_factor || 0).toFixed(4), csvValue: (row) => Number(row.k_factor || 0).toFixed(4) },
        { key: 'coverage_pct', label: 'Coverage %', sortValue: (row) => row.coverage_pct ?? -Infinity, render: (row) => Number(row.coverage_pct || 0).toFixed(1), csvValue: (row) => Number(row.coverage_pct || 0).toFixed(1) },
        { key: 'loss_power_clipping_kwh', label: 'Power Clip (kWh)', sortValue: (row) => row.loss_power_clipping_kwh ?? -Infinity, render: (row) => Number(row.loss_power_clipping_kwh || 0).toFixed(2), csvValue: (row) => Number(row.loss_power_clipping_kwh || 0).toFixed(2) },
        { key: 'loss_current_clipping_kwh', label: 'Current Clip (kWh)', sortValue: (row) => row.loss_current_clipping_kwh ?? -Infinity, render: (row) => Number(row.loss_current_clipping_kwh || 0).toFixed(2), csvValue: (row) => Number(row.loss_current_clipping_kwh || 0).toFixed(2) },
        { key: 'total_energy_loss_kwh', label: 'Total Loss (kWh)', sortValue: (row) => row.total_energy_loss_kwh ?? -Infinity, render: (row) => h('strong', null, Number(row.total_energy_loss_kwh || 0).toFixed(2)), csvValue: (row) => Number(row.total_energy_loss_kwh || 0).toFixed(2) },
        { key: 'investigation_window', label: 'Window', render: (row) => {
          const s = row.investigation_window_start || '';
          const e = row.investigation_window_end || '';
          return s && e ? `${String(s).slice(0, 16)} → ${String(e).slice(0, 16)}` : (s || e || '—');
        }, csvValue: (row) => `${row.investigation_window_start || ''} - ${row.investigation_window_end || ''}` },
        { key: 'action', label: 'Investigate', sortable: false, render: (row) => h('button', {
          className: 'btn btn-primary', style: { padding: '4px 8px', fontSize: 12 },
          onClick: () => setSelectedCdInverter(row.inverter_id),
        }, 'Investigate'), csvValue: () => 'Investigate' },
      ];

      const derateCols = [
        { key: 'inverter_id', label: 'Inverter', render: (row) => h('strong', null, row.inverter_id), csvValue: (row) => row.inverter_id },
        { key: 'rated_ac_kw', label: 'Rated (kW)', sortValue: (row) => row.rated_ac_kw ?? -Infinity, render: (row) => Number(row.rated_ac_kw || 0).toFixed(1), csvValue: (row) => Number(row.rated_ac_kw || 0).toFixed(2) },
        { key: 'k_factor', label: 'k (kW · W⁻¹·m²)', sortValue: (row) => row.k_factor ?? -Infinity, render: (row) => Number(row.k_factor || 0).toFixed(4), csvValue: (row) => Number(row.k_factor || 0).toFixed(4) },
        { key: 'coverage_pct', label: 'Coverage %', sortValue: (row) => row.coverage_pct ?? -Infinity, render: (row) => Number(row.coverage_pct || 0).toFixed(1), csvValue: (row) => Number(row.coverage_pct || 0).toFixed(1) },
        { key: 'dominant_kind', label: 'Shape', render: (row) => h('span', { style: { padding: '2px 8px', borderRadius: 12, fontSize: 11, fontWeight: 600, background: CD_KIND_COLOR[row.dominant_kind] || '#334155', color: '#fff' } }, CD_KIND_LABELS[row.dominant_kind] || row.dominant_kind), csvValue: (row) => CD_KIND_LABELS[row.dominant_kind] || row.dominant_kind },
        { key: 'loss_static_derating_kwh', label: 'Static (kWh)', sortValue: (row) => row.loss_static_derating_kwh ?? -Infinity, render: (row) => Number(row.loss_static_derating_kwh || 0).toFixed(2), csvValue: (row) => Number(row.loss_static_derating_kwh || 0).toFixed(2) },
        { key: 'loss_dynamic_derating_kwh', label: 'Dynamic (kWh)', sortValue: (row) => row.loss_dynamic_derating_kwh ?? -Infinity, render: (row) => Number(row.loss_dynamic_derating_kwh || 0).toFixed(2), csvValue: (row) => Number(row.loss_dynamic_derating_kwh || 0).toFixed(2) },
        { key: 'total_energy_loss_kwh', label: 'Total Loss (kWh)', sortValue: (row) => row.total_energy_loss_kwh ?? -Infinity, render: (row) => h('strong', null, Number(row.total_energy_loss_kwh || 0).toFixed(2)), csvValue: (row) => Number(row.total_energy_loss_kwh || 0).toFixed(2) },
        { key: 'investigation_window', label: 'Window', render: (row) => {
          const s = row.investigation_window_start || '';
          const e = row.investigation_window_end || '';
          return s && e ? `${String(s).slice(0, 16)} → ${String(e).slice(0, 16)}` : (s || e || '—');
        }, csvValue: (row) => `${row.investigation_window_start || ''} - ${row.investigation_window_end || ''}` },
        { key: 'action', label: 'Investigate', sortable: false, render: (row) => h('button', {
          className: 'btn btn-primary', style: { padding: '4px 8px', fontSize: 12 },
          onClick: () => setSelectedCdInverter(row.inverter_id),
        }, 'Investigate'), csvValue: () => 'Investigate' },
      ];

      return h('div', { style: { display: 'flex', flexDirection: 'column', gap: 16 } },
        cdLoading && h('div', { style: { padding: 24, textAlign: 'center', color: 'var(--text-soft)' } }, h(Spinner), ' Running GTI virtual-power model…'),

        // Data-quality advisory — only if there are skipped or thin-coverage inverters
        !cdLoading && cdSummary && (dq.skipped_count > 0 || (dq.avg_coverage_pct ?? 100) < 80) && h('div', {
          role: 'alert',
          style: {
            padding: '12px 16px',
            borderRadius: 10,
            fontSize: 13,
            background: 'rgba(245,158,11,0.1)',
            border: '1px solid rgba(245,158,11,0.45)',
            color: '#fde68a',
            display: 'flex',
            flexDirection: 'column',
            gap: 6,
          },
        },
          h('strong', { style: { color: '#fbbf24' } }, '⚠ Data quality in this range is limited.'),
          h('div', null,
            `${dq.skipped_count ?? 0} of ${dq.total_inverters ?? 0} inverters skipped for low coverage / insufficient healthy samples — they do not contribute to the KPIs above. `,
            `Avg valid-sample coverage during 07:00–18:00: ${Number(dq.avg_coverage_pct ?? 0).toFixed(1)} %.`
          ),
          (dq.skipped || []).length > 0 && h('details', { style: { fontSize: 12, opacity: 0.85 } },
            h('summary', { style: { cursor: 'pointer' } }, `Skipped inverters (${(dq.skipped || []).length})`),
            h('div', { style: { marginTop: 6, maxHeight: 120, overflow: 'auto' } },
              (dq.skipped || []).map((s, i) => h('div', { key: i, style: { padding: '2px 0' } },
                h('code', { style: { color: '#fde68a' } }, s.inverter_id), ' — ', s.reason
              ))
            )
          )
        ),

        !cdLoading && cdSummary && h(Card, { title: isClip ? 'Clipping Insights (GTI virtual-power model)' : 'Derating Insights (GTI virtual-power model)' },
          h('div', { className: 'kpi-grid', style: { gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))' } },
            bannerKpis.map((k, i) => h(KpiCard, { key: i, label: k.label, value: k.value, unit: k.unit, color: k.color }))
          )
        ),

        !cdLoading && invLossForTab.length > 0 && h(Card, { title: isClip ? 'Clipping Loss per Inverter' : 'Derating Loss per Inverter' },
          h(window.EChart, {
            style: { width: '100%', height: 320 },
            option: {
              tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, formatter: (params) => `${params[0].name}<br/>${params[0].marker} ${params[0].seriesName}: <b>${Number(params[0].value).toFixed(2)} kWh</b>`, backgroundColor: '#0f172a', borderColor: '#1e293b', textStyle: { color: '#f8fafc', fontSize: 12 } },
              grid: { top: 10, right: 10, left: 45, bottom: 60 },
              xAxis: { type: 'category', data: invLossForTab.map(d=>d.inverter_id), axisLabel: { fontSize: 10, color: '#94a3b8', rotate: 35 }, axisLine: { lineStyle: { color: '#1e293b' } }, axisTick: {show:false} },
              yAxis: { type: 'value', name: 'kWh', nameLocation: 'middle', nameGap: 30, nameTextStyle: { color: '#94a3b8', fontSize: 10 }, axisLabel: { fontSize: 10, color: '#94a3b8' }, splitLine: { lineStyle: { type: 'dashed', color: '#1e293b' } } },
              series: [{ type: 'bar', name: isClip ? 'Clipping Loss (kWh)' : 'Derating Loss (kWh)', data: invLossForTab.map(d=>d.loss_kwh), itemStyle: { color: accent, borderRadius: [4,4,0,0] } }]
            }
          })
        ),

        !cdLoading && h(Card, { title: `Per-Inverter Detail (${rowsForTab.length} inverter${rowsForTab.length === 1 ? '' : 's'})` },
          h(DataTable, {
            columns: isClip ? clipCols : derateCols,
            rows: rowsForTab,
            emptyMessage: isClip ? 'No inverter hit the clipping threshold in this range.' : 'No derating shape detected in this range.',
            filename: `${isClip ? 'clipping' : 'derating'}_${plantId || 'plant'}_${dateFrom}_${dateTo}.csv`,
            maxHeight: 460,
            initialSortKey: 'total_energy_loss_kwh',
            initialSortDir: 'desc',
          })
        ),
      );
    })(),

    // ── Investigate modal (shared between Clipping & Derating tabs) ──
    (subView === 'clip' || subView === 'derate') && selectedCdInverter && ReactDOM.createPortal(
        h('div', {
          style: { position: 'fixed', inset: 0, background: 'rgba(2,6,23,0.72)', zIndex: 1200, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 24 },
          onClick: () => setSelectedCdInverter(null)
        },
          h('div', {
            onClick: (e) => e.stopPropagation(),
            style: { background: 'var(--surface, #0f172a)', border: '1px solid var(--border, #1e293b)', borderRadius: 14, width: 'min(1180px, 96vw)', maxHeight: '92vh', overflow: 'auto', padding: 20, boxShadow: '0 30px 60px rgba(0,0,0,0.5)' }
          },
            h('div', { style: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 14 } },
              h('div', null,
                h('div', { style: { fontSize: 18, fontWeight: 700, color: 'var(--text, #e2e8f0)' } }, `Investigate · ${selectedCdInverter}`),
                h('div', { style: { fontSize: 12, color: 'var(--text-soft, #94a3b8)', marginTop: 2 } }, 'Actual AC power vs GTI-derived virtual curve. Coloured bands mark clipping / derating minutes.')
              ),
              h('button', { className: 'btn', onClick: () => setSelectedCdInverter(null), style: { padding: '6px 12px' } }, '✕ Close')
            ),

            cdTimelineLoading && h('div', { style: { padding: 40, textAlign: 'center', color: 'var(--text-soft)' } }, h(Spinner), ' Loading timeline…'),

            !cdTimelineLoading && cdTimeline.length === 0 && h('div', { style: { padding: 40, textAlign: 'center', color: 'var(--text-soft)' } }, 'No timeline data for this inverter in the selected range.'),

              !cdTimelineLoading && cdTimeline.length > 0 && (() => {
              // ECharts CD timeline — replaces legacy ComposedChart (Recharts removed)
              const runs = [];
              let cur = null;
              for (const p of cdTimeline) {
                if (p.state && p.state !== 'normal') {
                  if (cur && cur.state === p.state) { cur.end = p.timestamp; }
                  else { if (cur) runs.push(cur); cur = { state: p.state, start: p.timestamp, end: p.timestamp }; }
                } else if (cur) { runs.push(cur); cur = null; }
              }
              if (cur) runs.push(cur);

              const xData = cdTimeline.map(d => String(d.timestamp || '').slice(5, 16));
              const markAreas = runs.map(run => ([
                { xAxis: String(run.start || '').slice(5, 16), itemStyle: { color: CD_KIND_COLOR[run.state] || '#64748b', opacity: 0.15 } },
                { xAxis: String(run.end || '').slice(5, 16) }
              ]));

              const cdOption = {
                backgroundColor: 'transparent',
                tooltip: { trigger: 'axis', backgroundColor: '#0f172a', borderColor: '#1e293b', textStyle: { color: '#e2e8f0', fontSize: 12 }, axisPointer: { type: 'cross' } },
                legend: { bottom: 0, textStyle: { color: '#94a3b8', fontSize: 12 } },
                grid: { top: 20, right: 80, left: 60, bottom: 60 },
                xAxis: { type: 'category', data: xData, axisLabel: { color: '#94a3b8', fontSize: 10 }, axisLine: { lineStyle: { color: '#1e293b' } } },
                yAxis: [
                  { name: 'Power (kW)', type: 'value', axisLabel: { color: '#94a3b8', fontSize: 10 }, axisLine: { lineStyle: { color: '#1e293b' } }, splitLine: { lineStyle: { color: '#1e293b', type: 'dashed' } } },
                  { name: 'GTI (W/m²)', type: 'value', position: 'right', axisLabel: { color: '#f59e0b', fontSize: 10 }, axisLine: { lineStyle: { color: '#f59e0b' } }, splitLine: { show: false } }
                ],
                series: [
                  { name: 'Virtual (expected)', type: 'line', yAxisIndex: 0, data: cdTimeline.map(d => d.virtual_ac_kw), lineStyle: { color: '#10b981', width: 2, type: 'dashed' }, symbol: 'none', markArea: { silent: true, data: markAreas } },
                  { name: 'Actual', type: 'line', yAxisIndex: 0, data: cdTimeline.map(d => d.actual_ac_kw), lineStyle: { color: '#3b82f6', width: 2 }, symbol: 'none' },
                  { name: 'GTI (W/m²)', type: 'line', yAxisIndex: 1, data: cdTimeline.map(d => d.gti), lineStyle: { color: '#f59e0b', width: 1.3, opacity: 0.8 }, symbol: 'none' },
                ]
              };
              return h(window.EChart || 'div', { style: { width: '100%', height: 460 }, option: cdOption });
            })(),

            // Legend of kinds
            !cdTimelineLoading && cdTimeline.length > 0 && h('div', { style: { display: 'flex', flexWrap: 'wrap', gap: 10, marginTop: 10, fontSize: 11, color: 'var(--text-soft)' } },
              Object.entries(CD_KIND_COLOR).filter(([k]) => k !== 'normal').map(([k, c]) =>
                h('span', { key: k, style: { display: 'inline-flex', alignItems: 'center', gap: 6 } },
                  h('span', { style: { width: 10, height: 10, borderRadius: 2, background: c, opacity: 0.6 } }),
                  CD_KIND_LABELS[k] || k
                )
              )
            )
          )
        ),
        document.body
      ),

    subView === 'scb_perf' && h('div', { style: { display: 'flex', flexDirection: 'column', gap: 16 } },
      scbPerfLoading && h('div', { style: { padding: 24, textAlign: 'center', color: 'var(--text-soft)' } }, h(Spinner), ' Loading Soiling data…'),

      !scbPerfLoading && soilingTabError && h('div', {
        role: 'alert',
        style: {
          padding: '12px 16px',
          borderRadius: 10,
          fontSize: 13,
          background: 'rgba(239,68,68,0.12)',
          border: '1px solid rgba(239,68,68,0.45)',
          color: '#fecaca',
        },
      }, h('strong', { style: { display: 'block', marginBottom: 6 } }, 'Soiling data could not be loaded completely'),
        soilingTabError),

      !scbPerfLoading && soilingPlant && soilingPlant.data_hints && (() => {
        const hnt = soilingPlant.data_hints;
        const needPr = (hnt.pr_days_with_pr || 0) < 2;
        const noEnergy = (hnt.inverter_energy_rows || 0) === 0;
        const noIrr = (hnt.irradiance_rows || 0) === 0;
        if (!needPr && !noEnergy && !noIrr) return null;
        return h('div', {
          style: {
            padding: '12px 16px',
            borderRadius: 10,
            fontSize: 13,
            background: 'rgba(245,158,11,0.1)',
            border: '1px solid rgba(245,158,11,0.35)',
            color: 'var(--text-soft)',
          },
        },
        h('strong', { style: { color: '#fbbf24' } }, 'Limited or missing inputs for soiling KPIs. '),
        noEnergy && 'No inverter AC power rows in range (equipment_level=inverter, signal=ac_power). ',
        noIrr && 'No plant/WMS GTI or irradiance rows in range. ',
        needPr && 'Need at least two days with computable plant PR (energy + irradiance + DC capacity). ',
        'Heatmap still uses SCB DC current; rankings use peer/PR models when data allows.');
      })(),

      !scbPerfLoading && h('div', { className: 'kpi-grid', style: { gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))' } },
        h(KpiCard, {
          label: 'Soiling rate (PR regression)',
          value: soilingPlant && soilingPlant.soiling_rate_regression_pp_per_day != null ? soilingPlant.soiling_rate_regression_pp_per_day : '—',
          unit: 'pp/day',
          color: '#0ea5e9',
        }),
        h(KpiCard, {
          label: 'Soiling loss',
          value: soilingPlant && soilingPlant.soiling_loss_mwh != null ? soilingPlant.soiling_loss_mwh : '—',
          unit: 'MWh',
          color: '#f97316',
        }),
        h(KpiCard, {
          label: 'Top soiling SCB',
          value: (soilingPlant && soilingPlant.top_soiling_scb_id) ? soilingPlant.top_soiling_scb_id : '—',
          unit: '',
          color: '#dc2626',
        }),
        h(KpiCard, {
          label: 'Revenue loss (soiling × PPA)',
          value: soilingPlant && soilingPlant.revenue_loss_inr != null ? soilingPlant.revenue_loss_inr.toLocaleString('en-IN') : '—',
          unit: soilingPlant && soilingPlant.revenue_loss_inr != null ? '₹' : '',
          color: '#7c3aed',
        }),
      ),

      !scbPerfLoading && soilingPlant && soilingPlant.soiling_rate_median_delta_pp != null && h('div', { style: { fontSize: 12, color: 'var(--text-muted)', marginTop: -4 } },
        'Median day-over-day PR change (3-day smoothed): ',
        h('strong', { style: { color: 'var(--text)' } }, soilingPlant.soiling_rate_median_delta_pp),
        ' pp/day'
      ),

      !scbPerfLoading && soilingPlant && (() => {
        const lo = String(dateFrom || '').slice(0, 10);
        const hi = String(dateTo || '').slice(0, 10);
        const prData = (soilingPlant.series || []).filter((s) => {
          if (s.pr_pct == null) return false;
          const d = String(s.date || '').slice(0, 10);
          if (!lo || !hi) return true;
          return d >= lo && d <= hi;
        });
        if (!prData.length) return null;
        return h(Card, { title: 'Plant performance ratio (daily)' },
          h('div', { style: { height: 300, overflow: 'hidden', marginTop: 4 } },
            h(window.EChart, {
              style: { width: '100%', height: '100%' },
              option: {
                tooltip: { trigger: 'axis', backgroundColor: 'var(--panel)', borderColor: 'var(--line)', textStyle: { color: 'var(--text)' } },
                grid: { top: 28, right: 16, left: 40, bottom: 24 },
                xAxis: { type: 'category', boundaryGap: false, data: prData.map(d=>d.date), axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, axisLine: { lineStyle: { color: 'var(--line)' } } },
                yAxis: { type: 'value', scale: true, axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, splitLine: { lineStyle: { type: 'dashed', color: 'rgba(255,255,255,0.08)' } } },
                series: [{ type: 'line', name: 'PR %', data: prData.map(d=>d.pr_pct), itemStyle: { color: '#14b8a6' }, symbol: 'circle', symbolSize: 6, lineStyle: { width: 2 }, smooth: true }]
              }
            })
          )
        );
      })(),

      !scbPerfLoading && h(Card, { title: 'Soiling loss by equipment (top 15)' },
        h('div', { style: { display: 'flex', gap: 8, marginBottom: 12, alignItems: 'center', flexWrap: 'wrap' } },
          h('span', { style: { fontSize: 13, color: 'var(--text-soft)' } }, 'Group by:'),
          h('button', {
            className: 'btn ' + (soilingBarGroup === 'inverter' ? 'btn-primary' : 'btn-outline'),
            style: { padding: '4px 12px', fontSize: 12 },
            onClick: () => setSoilingBarGroup('inverter'),
          }, 'Inverter'),
          h('button', {
            className: 'btn ' + (soilingBarGroup === 'scb' ? 'btn-primary' : 'btn-outline'),
            style: { padding: '4px 12px', fontSize: 12 },
            onClick: () => setSoilingBarGroup('scb'),
          }, 'SCB'),
        ),
        soilingRankingsLoading && h('div', { style: { minHeight: 120, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-soft)', gap: 8 } },
          h(Spinner), ' Loading rankings…'
        ),
        !soilingRankingsLoading && (!(soilingRankings && (soilingRankings.rows || []).length)) && h('div', { className: 'empty-state', style: { minHeight: 120 } }, 'No ranking data for this range.'),
        !soilingRankingsLoading && (soilingRankings && Array.isArray(soilingRankings.rows) && soilingRankings.rows.length > 0) && (()=>{
          const rankingData = soilingRankings.rows.map(r => ({ name: r.label || r.id, loss: Number(r.loss_mwh) || 0 }));
          return h(window.EChart, {
            style: { width: '100%', height: 360 },
            option: {
              tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, backgroundColor: 'var(--panel)', borderColor: 'var(--line)', textStyle: { color: 'var(--text)' } },
              grid: { top: 20, right: 20, left: 50, bottom: 80 },
              xAxis: { type: 'category', data: rankingData.map(d=>d.name), axisLabel: { fontSize: 9, color: 'var(--text-soft)', rotate: 35 }, axisLine: { lineStyle: { color: 'var(--line)' } }, axisTick: {show:false} },
              yAxis: { type: 'value', name: 'MWh', nameLocation: 'middle', nameGap: 35, nameTextStyle: { color: 'var(--text-soft)', fontSize: 10 }, axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, splitLine: { lineStyle: { type: 'dashed', color: 'rgba(255,255,255,0.08)' } } },
              series: [{ type: 'bar', name: 'Loss (MWh)', data: rankingData.map(d=>d.loss), itemStyle: { color: '#f97316', borderRadius: [2,2,0,0] } }]
            }
          })
        })()
      ),

      !scbPerfLoading && !scbPerfHeatmap && h(Card, { title: 'Soiling heatmap' },
        h('div', { className: 'empty-state', style: { minHeight: 220 } },
          'No SCB current data for the selected plant and date range. Upload raw SCB DC current (equipment_level=scb, signal=dc_current), or try another range.'
        )
      ),
      !scbPerfLoading && scbPerfHeatmap && (scbPerfHeatmap.scbs || []).length > 0 && h(Card, { title: 'SCB heatmap (irradiance-normalized ratio, gradient = best→worst in view)' },
        h('div', { style: { display: 'flex', flexDirection: 'column', gap: 0 } },
          (function () {
            const rangeLo = String(dateFrom || '').slice(0, 10);
            const rangeHi = String(dateTo || '').slice(0, 10);
            const inSelectedRange = (day) => {
              if (!day || !rangeLo || !rangeHi) return true;
              return day >= rangeLo && day <= rangeHi;
            };
            const buckets = scbPerfHeatmap.time_buckets || [];
            const scbs = scbPerfHeatmap.scbs || [];
            const cells = scbPerfHeatmap.cells || [];
            const bucketDateMap = {};
            (buckets || []).forEach(b => {
              const idx = Number(b.index);
              const lbl = String(b.label || '');
              if (!Number.isFinite(idx) || !lbl) return;
              bucketDateMap[idx] = lbl.slice(0, 10);
            });

            // Daily roll-up: one column per date, average metrics across that date's hourly buckets.
            const dailyCellMap = {};
            const dailyOrderSet = new Set();
            cells.forEach(c => {
              const bi = Number(c.bucket_idx);
              if (!Number.isFinite(bi)) return;
              const day = bucketDateMap[bi];
              if (!day || !inSelectedRange(day)) return;
              dailyOrderSet.add(day);
              if (!dailyCellMap[c.scb_id]) dailyCellMap[c.scb_id] = {};
              if (!dailyCellMap[c.scb_id][day]) {
                dailyCellMap[c.scb_id][day] = {
                  curSum: 0, curN: 0,
                  ratioSum: 0, ratioN: 0,
                  pctSum: 0, pctN: 0,
                };
              }
              const slot = dailyCellMap[c.scb_id][day];
              const cur = Number(c.current);
              if (Number.isFinite(cur)) { slot.curSum += cur; slot.curN += 1; }
              const rr = Number(c.ratio);
              if (Number.isFinite(rr)) { slot.ratioSum += rr; slot.ratioN += 1; }
              const pp = Number(c.percentile);
              if (Number.isFinite(pp)) { slot.pctSum += pp; slot.pctN += 1; }
            });

            const dailyBuckets = Array.from(dailyOrderSet).sort().filter(inSelectedRange).map((d, i) => ({ index: i, label: d, date: d }));

            function robustRange(vals) {
              if (!vals || !vals.length) return { lo: null, hi: null };
              const arr = vals.slice().sort((a, b) => a - b);
              const lo = arr[Math.floor((arr.length - 1) * 0.05)];
              const hi = arr[Math.floor((arr.length - 1) * 0.95)];
              return { lo, hi };
            }

            const dailyFlat = [];
            Object.keys(dailyCellMap).forEach((sid) => {
              Object.keys(dailyCellMap[sid]).forEach((d) => {
                const slot = dailyCellMap[sid][d];
                dailyFlat.push({
                  ratio: slot.ratioN ? (slot.ratioSum / slot.ratioN) : null,
                  percentile: slot.pctN ? (slot.pctSum / slot.pctN) : null,
                });
              });
            });
            const ratioList = dailyFlat.map(x => x.ratio).filter(v => v != null && Number.isFinite(v));
            const pctList = dailyFlat.map(x => x.percentile).filter(v => v != null && Number.isFinite(v));
            const rr = robustRange(ratioList);
            const pr = robustRange(pctList);
            const minR = rr.lo;
            const maxR = rr.hi;
            const minP = pr.lo;
            const maxP = pr.hi;
            const ratioRangeOk = minR != null && maxR != null && (maxR - minR) > 0.005;

            // SCB column: fixed % of table; day columns split the rest evenly (fills card width).
            const cellRowH = 30;
            const nDays = dailyBuckets.length || 0;
            const scbColPct = 22;
            const dayColPct = nDays > 0 ? (100 - scbColPct) / nDays : 100 - scbColPct;
            const minTablePx = 168 + Math.max(nDays, 1) * 44;

            const thStickyBase = {
              position: 'sticky',
              top: 0,
              borderBottom: '1px solid var(--line)',
              borderRight: '1px solid var(--line-soft)',
              color: 'var(--text-soft)',
              background: 'var(--panel)',
              fontWeight: 700,
              zIndex: 5,
              boxSizing: 'border-box',
              verticalAlign: 'middle',
            };
            const thCorner = {
              ...thStickyBase,
              left: 0,
              zIndex: 6,
              textAlign: 'left',
              padding: '10px 12px',
              fontSize: 11,
              letterSpacing: '0.02em',
              boxShadow: '4px 0 12px rgba(0,0,0,0.06)',
            };
            const thDay = {
              ...thStickyBase,
              minWidth: 0,
              padding: '8px 4px',
              fontSize: 10,
              lineHeight: 1.15,
              textAlign: 'center',
              fontVariantNumeric: 'tabular-nums',
              whiteSpace: 'nowrap',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
            };
            const tdSticky = (selected) => ({
              padding: '6px 10px',
              borderRight: '1px solid var(--line-soft)',
              borderBottom: '1px solid var(--line-soft)',
              fontWeight: 600,
              fontSize: 11,
              color: 'var(--text)',
              background: selected ? 'rgba(14, 165, 233, 0.12)' : 'var(--panel)',
              position: 'sticky',
              left: 0,
              zIndex: 2,
              cursor: 'pointer',
              whiteSpace: 'nowrap',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              boxShadow: '4px 0 10px rgba(0,0,0,0.04)',
              boxSizing: 'border-box',
              verticalAlign: 'middle',
            });

            return h(React.Fragment, null,
              !ratioRangeOk && ratioList.length > 0 && h('div', {
                style: { margin: '0 0 10px 0', padding: '8px 12px', borderRadius: 8, fontSize: 12, background: 'rgba(245,158,11,0.1)', border: '1px solid rgba(245,158,11,0.35)', color: '#fbbf24' }
              }, '\u26a0 Irradiance data unavailable or uniform — ratio gradient is indistinct. Showing within-inverter percentile coloring. Upload GTI/irradiance (equipment_level=plant or wms) to enable proper soiling gradient.'),
            h('div', {
              style: {
                flexShrink: 0,
                padding: '0 0 10px',
                marginBottom: 8,
                borderBottom: '1px solid var(--line-soft)',
              }
            },
              h('div', { style: { display: 'flex', gap: 15, fontSize: 12, alignItems: 'center', color: 'var(--text-soft)', flexWrap: 'wrap' } },
                h('strong', { style: { color: 'var(--text)' } }, 'Legend: '),
                h('div', { style: { display: 'flex', alignItems: 'center', gap: 5 } }, h('span', { style: { width: 56, height: 12, borderRadius: 3, display: 'inline-block', background: 'linear-gradient(90deg,#dc2626,#eab308,#22c55e)' } }), 'Low → high ratio (within this chart)'),
                h('span', { style: { color: 'var(--text-muted)', marginLeft: 8 } }, '— Click a row to open daily trend.')
              )
            ),
            h('div', {
              className: 'soiling-heatmap-scroll',
              style: {
                width: '100%',
                maxHeight: 480,
                overflow: 'auto',
                WebkitOverflowScrolling: 'touch',
                borderRadius: 10,
                border: '1px solid var(--line)',
                background: 'var(--panel)',
              },
            },
              h('table', {
                className: 'soiling-heatmap-table',
                style: {
                  borderCollapse: 'separate',
                  borderSpacing: 0,
                  tableLayout: 'fixed',
                  width: '100%',
                  minWidth: minTablePx,
                },
              },
                h('colgroup', null,
                  h('col', { style: { width: `${scbColPct}%` } }),
                  dailyBuckets.map((b) => h('col', { key: `col-${b.date}`, style: { width: `${dayColPct}%` } })),
                ),
                h('thead', null,
                  h('tr', null,
                    h('th', { style: thCorner, scope: 'col' }, 'SCB'),
                    dailyBuckets.map(b => h('th', { key: b.date, style: thDay, scope: 'col', title: b.date }, b.date.slice(5)))
                  )
                ),
                h('tbody', null,
                scbs.map(s => {
                  const scbId = s.scb_id;
                  const isSelected = soilingModalScb === scbId;
                  return h('tr', {
                    key: scbId,
                    style: { background: isSelected ? 'rgba(14, 165, 233, 0.08)' : 'transparent' },
                    onClick: () => setSoilingModalScb(scbId)
                  },
                    h('td', { style: tdSticky(isSelected), title: scbId }, scbId),
                    dailyBuckets.map(b => {
                      const slot = dailyCellMap[scbId] && dailyCellMap[scbId][b.date];
                      const pct = slot && slot.pctN ? (slot.pctSum / slot.pctN) : null;
                      const cur = slot && slot.curN ? (slot.curSum / slot.curN) : null;
                      const ratio = slot && slot.ratioN ? (slot.ratioSum / slot.ratioN) : null;
                      const useRatio = ratio != null && Number.isFinite(Number(ratio)) && minR != null && maxR != null && (maxR - minR) > 0.005;
                      const bg = useRatio
                        ? soilingRatioHeatColor(ratio, minR, maxR)
                        : (pct != null && minP != null && maxP != null && maxP > minP
                          ? soilingPctHeatColor(pct, minP, maxP)
                          : '#1e293b');
                      const rt = ratio != null ? Number(ratio).toFixed(3) : '—';
                      return h('td', {
                        key: b.date,
                        style: {
                          minWidth: 0,
                          height: cellRowH,
                          padding: 0,
                          background: bg,
                          borderRight: '1px solid var(--line-soft)',
                          borderBottom: '1px solid var(--line-soft)',
                          cursor: 'pointer',
                          position: 'relative',
                          zIndex: 0,
                          boxSizing: 'border-box',
                          verticalAlign: 'middle',
                        },
                        title: cur != null ? `${scbId} @ ${b.date}: ${cur.toFixed(2)} A | ratio ${rt} | inv %ile ${pct != null ? pct.toFixed(0) : '—'}` : `${scbId} @ ${b.date}`,
                      }, '');
                    })
                  );
                })
                )
              )
            ));
          })()
        )
      ),
      !scbPerfLoading && scbPerfHeatmap && (!scbPerfHeatmap.scbs || scbPerfHeatmap.scbs.length === 0) && h(Card, { title: 'Soiling heatmap' }, h('div', { className: 'empty-state', style: { minHeight: 200 } }, 'No SCB current data for the selected date range.')),
    ),

    subView === 'damage' && h('div', { className: 'empty-state', style: { minHeight: 300, background: 'var(--panel)', borderRadius: 12, border: '1px solid var(--line)' } },
      h('div', { style: { fontSize: 16, fontWeight: 600, marginBottom: 8 } }, 'Algorithm Under Development'),
      h('p', { style: { color: 'var(--text-muted)' } }, 'This diagnostic feature will be available in a future update.')
    ),

    scbStatus.length > 0 && subView === 'ds' && h('div', { style: { display: 'grid', gap: 16, gridTemplateColumns: '1fr' } },
      filterSummary && (filterSummary.total_filtered > 0) && h('div', {
        style: {
          padding: '10px 16px', borderRadius: 10, fontSize: 13,
          background: 'rgba(245,158,11,0.12)', border: '1px solid rgba(245,158,11,0.4)',
          color: '#92400e', display: 'flex', flexWrap: 'wrap', gap: 12, alignItems: 'flex-start'
        }
      },
        h('strong', { style: { color: '#78350f' } }, '\u26a0\ufe0f Data Quality Notice:'),
        h('span', null,
          `${filterSummary.total_filtered} SCB${filterSummary.total_filtered !== 1 ? 's' : ''} were excluded from DS analysis for ${dateFrom} \u2192 ${dateTo} — `,
          filterSummary.outlier_count > 0  && h('span', null, `${filterSummary.outlier_count} due to outlier data (current > Isc\u00d7strings or < 0)`),
          filterSummary.outlier_count > 0 && (filterSummary.constant_count > 0 || filterSummary.leakage_count > 0) && ', ',
          filterSummary.constant_count > 0 && h('span', null, `${filterSummary.constant_count} due to constant/frozen data (>10 consecutive equal readings, including flat signals >120 timestamps/day)`),
          filterSummary.constant_count > 0 && filterSummary.leakage_count > 0 && ', ',
          filterSummary.leakage_count > 0  && h('span', null, `${filterSummary.leakage_count} due to leakage data (very low current all day with irradiance > 50 W/m\u00b2)`)
        )
      ),
      dsSummary && h(Card, { title: `Disconnected String (DS) Insights — ${dsSummary.latest_date || dateTo || 'Latest'}` },
        h('div', null,
          h('div', { className: 'kpi-grid', style: { gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))' } },
            h(KpiCard, { label: 'Total SCBs', value: totalScbs || (dsSummary.total_scbs != null ? dsSummary.total_scbs : '—'), unit: '', color: 'var(--text-soft)' }),
            h(KpiCard, { label: 'Communicating SCBs', value: communicatingScbs || (dsSummary.communicating_scbs != null ? dsSummary.communicating_scbs : '—'), unit: '', color: 'var(--accent)' }),
            h(KpiCard, { label: 'Active DS Faults', value: dsSummary.active_ds_faults, unit: 'SCBs', color: 'var(--solar-orange)' }),
            h(KpiCard, { label: 'Total Disconnected Strings', value: dsSummary.total_disconnected_strings, unit: 'strings', color: '#EF4444' }),
            h(KpiCard, {
              label: 'Daily Energy Loss',
              value: dsSummary.energy_available
                ? (dsSummary.daily_energy_loss_mwh != null ? dsSummary.daily_energy_loss_mwh
                   : dsSummary.daily_energy_loss_kwh != null ? (dsSummary.daily_energy_loss_kwh / 1000).toFixed(2)
                   : null)
                : 'N/A',
              unit: dsSummary.energy_available ? 'MWh' : '', color: '#8B5CF6'
            }),
            h(KpiCard, {
              label: 'Top Affected SCB',
              value: dsSummary.energy_available ? (((dsSummary.top_affected_scbs || [])[0] || {}).scb_id || 'None') : 'N/A',
              unit: (() => {
                const top = (dsSummary.top_affected_scbs || [])[0];
                if (!dsSummary.energy_available || !top) return '';
                const mwh = top.loss_mwh != null ? top.loss_mwh : (top.loss_kwh != null ? (Number(top.loss_kwh) / 1000).toFixed(2) : null);
                return mwh != null ? `${mwh} MWh loss` : '';
              })(),
              color: 'var(--text-main)'
            }),
          ),
          dsSummary.energy_note && h('div', {
            style: {
              marginTop: 12, padding: 10, borderRadius: 10,
              background: '#fff7ed', color: '#9a3412', fontSize: 13,
              border: '1px solid #fdba74'
            }
          }, dsSummary.energy_note)
        )
      ),

      h(Card, { title: `Active Disconnected Strings (${activeFaults.length} SCBs Affected)` },
        h(DataTable, {
          columns: faultColumns,
          rows: activeFaults,
          emptyMessage: 'No active DS faults detected matching filters.',
          filename: `ds_faults_${plantId || 'plant'}_${dateFrom}_${dateTo}.csv`,
          maxHeight: 420,
          initialSortKey: 'missing_strings',
          initialSortDir: 'desc',
          compact: true,
        })
      ),

      h(Card, { title: 'Plant Heatmap: Disconnected Strings per SCB' },
        h('div', { style: { display: 'flex', flexDirection: 'column', overflowX: 'auto', paddingBottom: 10, maxHeight: 520, overflowY: 'auto' } },
          h('div', { style: { display: 'flex', gap: 15, marginBottom: 15, fontSize: 12, alignItems: 'center', color: 'var(--text-soft)', flexWrap: 'wrap', flexShrink: 0 } },
            h('strong', { style: { color: 'var(--text)' } }, 'Legend: '),
            h('div', { style: { display: 'flex', alignItems: 'center', gap: 5 } },
              h('span', { style: { width: 12, height: 12, background: '#24b36b', borderRadius: 3, display: 'inline-block', boxShadow: 'inset 0 0 0 1px rgba(255,255,255,0.14)' } }), 'No Fault'),
            h('div', { style: { display: 'flex', alignItems: 'center', gap: 5 } },
              h('span', { style: { width: 12, height: 12, background: '#d99a33', borderRadius: 3, display: 'inline-block', boxShadow: 'inset 0 0 0 1px rgba(255,255,255,0.14)' } }), '1–2 Disconnected Strings'),
            h('div', { style: { display: 'flex', alignItems: 'center', gap: 5 } },
              h('span', { style: { width: 12, height: 12, background: '#de5a5a', borderRadius: 3, display: 'inline-block', boxShadow: 'inset 0 0 0 1px rgba(255,255,255,0.14)' } }), '>2 Disconnected Strings'),
            h('div', { style: { display: 'flex', alignItems: 'center', gap: 5 } },
              h('span', { style: { width: 12, height: 12, background: '#b91c1c', borderRadius: 3, display: 'inline-block', boxShadow: 'inset 0 0 0 1px rgba(255,255,255,0.14)' } }), 'Bad data'),
            h('div', { style: { display: 'flex', alignItems: 'center', gap: 5 } },
              h('span', { style: { width: 12, height: 12, background: '#172433', borderRadius: 3, display: 'inline-block', boxShadow: 'inset 0 0 0 1px rgba(255,255,255,0.14)' } }), 'Normal / Night'),
            h('div', { style: { display: 'flex', alignItems: 'center', gap: 5 } },
              h('span', { style: { width: 12, height: 12, background: '#0f172a', borderRadius: 3, display: 'inline-block', boxShadow: 'inset 0 0 0 1px rgba(255,255,255,0.2)' } }), 'No communication'),
            h('div', { style: { display: 'flex', alignItems: 'center', gap: 5 } },
              h('span', { style: { width: 12, height: 12, background: '#64748b', borderRadius: 3, display: 'inline-block', boxShadow: 'inset 0 0 0 1px rgba(255,255,255,0.14)' } }), 'Spare')
          ),
          h('table', { style: { borderCollapse: 'separate', borderSpacing: 0, fontSize: 11, minWidth: 'max-content' } },
            h('thead', { style: { position: 'sticky', top: 0, zIndex: 4, background: '#0d1520' } }, h('tr', null,
              h('th', { style: { padding: '6px 10px', background: '#132131', color: 'var(--text-soft)', borderRight: '1px solid rgba(255,255,255,0.08)', borderBottom: '1px solid rgba(255,255,255,0.08)', position: 'sticky', left: 0, zIndex: 5, textAlign: 'left' } }, 'SCBs'),
              inverters.map(inv => h('th', { key: inv, style: { padding: '6px 8px', borderRight: '1px solid rgba(255,255,255,0.06)', borderBottom: '1px solid rgba(255,255,255,0.08)', minWidth: 42, color: 'var(--text-soft)', background: '#0f1b29', fontWeight: 700 } }, inv.replace('INV-', '')))
            )),
            h('tbody', null,
              Array.from({ length: maxScbs }).map((_, i) =>
                h('tr', { key: i },
                  h('td', { style: { padding: '4px 10px', borderRight: '1px solid rgba(255,255,255,0.08)', borderBottom: '1px solid rgba(255,255,255,0.06)', fontWeight: 700, color: 'var(--text-soft)', background: i % 2 === 0 ? '#132131' : '#101d2b', position: 'sticky', left: 0, zIndex: 2, textAlign: 'left', whiteSpace: 'nowrap' } }, `SCB ${String(i + 1).padStart(2, '0')}`),
                  inverters.map(inv => {
                    const archCell = archByInv && archByInv[inv] && archByInv[inv][i];
                    if (!archCell) {
                      return h('td', {
                        key: inv,
                        style: { background: '#0d1520', borderRight: '1px solid rgba(255,255,255,0.06)', borderBottom: '1px solid rgba(255,255,255,0.06)', minWidth: 24, height: 28 }
                      }, '');
                    }
                    const data = scbStatusById[archCell.scb_id];
                    const isConstantBad = constantBadScbSet.has(archCell.scb_id);
                    const stringCount = archCell?.strings_per_scb != null ? archCell.strings_per_scb : '—';
                    const moduleWp = deriveModuleWp(archCell);
                    // Min(missing_strings) over range = same value everywhere (table, heatmap, summary)
                    const disconnectedStrings = Number(data?.missing_strings ?? data?.range_min_missing_strings ?? 0);
                    const hoverLines = [
                      `SCB: ${archCell.scb_id}`,
                      `Inverter: ${archCell.inverter_id || inv}`,
                      `Disconnected Strings (Min): ${disconnectedStrings}`,
                      `Strings Connected: ${stringCount}`,
                      `Module Wp: ${moduleWp != null && Number.isFinite(moduleWp) ? moduleWp : '—'}`,
                    ];
                    if (archCell.spare_flag || archCell.inferred_spare) {
                      return h('td', {
                        key: inv,
                        style: { background: '#64748b', borderRight: '1px solid rgba(255,255,255,0.06)', borderBottom: '1px solid rgba(255,255,255,0.06)', minWidth: 24, height: 28, cursor: 'default' },
                        title: [...hoverLines, archCell.inferred_spare ? 'Spare / inferred from architecture gap' : 'Spare'].join('\n')
                      }, '');
                    }
                    if (isConstantBad) {
                      return h('td', {
                        key: inv,
                        style: { background: '#b91c1c', borderRight: '1px solid rgba(255,255,255,0.06)', borderBottom: '1px solid rgba(255,255,255,0.06)', minWidth: 24, height: 28, cursor: 'default' },
                        title: [...hoverLines, 'Bad data: flat/constant signal detected (>120 equal timestamps in a day). DS logic excluded.'].join('\n')
                      }, '');
                    }
                    if (!data) {
                      return h('td', {
                        key: inv,
                        style: { background: '#0f172a', borderRight: '1px solid rgba(255,255,255,0.06)', borderBottom: '1px solid rgba(255,255,255,0.06)', minWidth: 24, height: 28 },
                        title: [...hoverLines, 'No communication'].join('\n')
                      }, '');
                    }
                    const isFault = disconnectedStrings > 0;
                    let bg = '#24b36b';
                    let textColor = '#f8fafc';
                    if (!isFault) bg = (data.expected_current === 0) ? '#172433' : '#24b36b';
                    else if (disconnectedStrings > 2) bg = '#de5a5a';
                    else bg = '#d99a33';
                    return h('td', {
                      key: inv,
                      style: { background: bg, borderRight: '1px solid rgba(255,255,255,0.06)', borderBottom: '1px solid rgba(255,255,255,0.06)', textAlign: 'center', color: textColor, fontWeight: 800, cursor: 'pointer', minWidth: 24, height: 28 },
                      title: hoverLines.join('\n'),
                      onClick: () => setSelectedFault(data.scb_id)
                    }, isFault ? disconnectedStrings : '');
                  })
                )
              )
            )
          )
        )
      ),

      h(Card, { title: 'Active Disconnected Strings by Inverter' },
        invChartData.length > 0
          ? h(window.EChart, {
              style: { width: '100%', height: 300 },
              option: {
                tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
                grid: { top: 10, right: 10, left: 40, bottom: 24 },
                xAxis: { type: 'category', data: invChartData.map(d=>d.inverter_id), axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, axisLine: { lineStyle: { color: 'var(--line)' } }, axisTick: {show:false} },
                yAxis: { type: 'value', axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, splitLine: { lineStyle: { type: 'dashed', color: 'var(--line)' } } },
                series: [{ type: 'bar', name: 'Disconnected Strings (Mode)', data: invChartData.map(d=>d.missing_strings), itemStyle: { color: '#f59e0b', borderRadius: [4,4,0,0] } }]
              }
            })
          : h('div', { style: { padding: 40, textAlign: 'center', color: 'var(--text-muted)' } }, 'No active DS faults to display.')
      ),

      h(Card, { title: 'Daily Energy Loss due to Disconnected Strings (kWh)' },
        !dsSummary?.energy_available
          ? h('div', { style: { padding: 40, textAlign: 'center', color: 'var(--text-muted)' } }, dsSummary?.energy_note || 'Energy cannot be calculated.')
          : energyChartData.length > 0
          ? h(window.EChart, {
              style: { width: '100%', height: 300 },
              option: {
                tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
                legend: { bottom: 0, textStyle: { color: 'var(--text-soft)' } },
                grid: { top: 10, right: 10, left: 40, bottom: 30 },
                xAxis: { type: 'category', data: energyChartData.map(d=>d.timestamp), axisLabel: { fontSize: 10, color: 'var(--text-soft)', formatter: v => v.slice(5) }, axisLine: { lineStyle: { color: 'var(--line)' } }, axisTick: {show:false} },
                yAxis: { type: 'value', axisLabel: { fontSize: 10, color: 'var(--text-soft)' }, splitLine: { lineStyle: { type: 'dashed', color: 'var(--line)' } } },
                series: [{ type: 'bar', name: 'Energy Loss (kWh)', data: energyChartData.map(d=>d.total_energy_loss), itemStyle: { color: '#ef4444', borderRadius: [2,2,0,0] } }]
              }
            })
          : h('div', { style: { padding: 40, textAlign: 'center', color: 'var(--text-muted)' } }, 'No energy loss data for this period.')
      )
    ),
    renderModal(),
    selectedPlInv && ReactDOM.createPortal(h(PLInvestigateModal, { inverterId: selectedPlInv, archList, plantId, dateFrom, dateTo, onClose: () => setSelectedPlInv(null) }), document.body),
    selectedIsInv && ReactDOM.createPortal(h(ISInvestigateModal, {
      inverterId: selectedIsInv,
      plantId,
      dateFrom,
      dateTo,
      onClose: () => setSelectedIsInv(null),
    }), document.body),
    selectedGbEvent && ReactDOM.createPortal(h(GBInvestigateModal, {
      eventId: selectedGbEvent,
      plantId,
      dateFrom,
      dateTo,
      onClose: () => setSelectedGbEvent(null),
    }), document.body),
    selectedCommItem && ReactDOM.createPortal(h(CommunicationIssueModal, {
      equipmentLevel: selectedCommItem.equipmentLevel,
      equipmentId: selectedCommItem.equipmentId,
      issueKind: selectedCommItem.issueKind,
      inverterId: selectedCommItem.inverterId,
      plantId,
      dateFrom,
      dateTo,
      onClose: () => setSelectedCommItem(null),
    }), document.body),
    soilingModalScb && ReactDOM.createPortal(h(SoilingScbModal, {
      scbId: soilingModalScb,
      plantId,
      dateFrom,
      dateTo,
      onClose: () => setSoilingModalScb(null),
    }), document.body),
  );
};

const ECHARTS_UNIFIED = {
  tooltip: { trigger: 'axis' },
  // Plotly-like interactivity: wheel over plot = X zoom/pan; Shift+wheel = Y zoom (stretch).
  dataZoom: [
    { type: 'inside', xAxisIndex: 0, start: 0, end: 100, zoomOnMouseWheel: true, moveOnMouseMove: true },
    { type: 'inside', yAxisIndex: 0, zoomOnMouseWheel: 'shift', moveOnMouseMove: 'shift', filterMode: 'none' },
  ],
  toolbox: {
    feature: {
      dataZoom: {},
      restore: {},
      saveAsImage: {}
    }
  },
  animation: true
};

function axisMaxFromValues(values, padRatio = 0.08, fallback = 1) {
  const nums = (values || []).map(v => Number(v)).filter(v => Number.isFinite(v));
  if (!nums.length) return fallback;
  const vmax = nums.reduce((a, b) => a > b ? a : b, -Infinity);
  if (!(vmax > 0)) return fallback;
  const raw = vmax * (1 + padRatio);
  // Round to a "nice" step so ticks remain clean (no long decimals).
  const mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const norm = raw / mag;
  let niceNorm = 1;
  if (norm > 1) niceNorm = 2;
  if (norm > 2) niceNorm = 5;
  if (norm > 5) niceNorm = 10;
  return niceNorm * mag;
}

/** ECharts canvas cannot reliably stroke with CSS variables; hover/emphasis may drop the line. Resolve theme vars to real colors. */
function themeCssColor(varName, fallback) {
  if (typeof document === 'undefined') return fallback;
  const el = document.body || document.documentElement;
  const v = getComputedStyle(el).getPropertyValue(varName).trim();
  return v || fallback;
}

/** X-axis category for PL charts: must include calendar day so multi-day ranges do not repeat HH:MM (markArea would shade the wrong day). */
function plXAxisLabel(ts) {
  const s = String(ts).replace('T', ' ');
  if (s.length >= 16) return s.slice(5, 16);
  return s;
}

/** Keeps fault-modality ECharts responsive on long timelines (smooth spline + animation + every tick is expensive). */
function echartsFaultModalPerf(pointCount) {
  const n = pointCount || 0;
  const heavy = n > 360;
  return {
    animation: !heavy,
    lineSmooth: n <= 800,
    xAxis: {
      boundaryGap: true,
      axisLabel: {
        hideOverlap: true,
        interval: 'auto',
        rotate: n > 220 ? 28 : 0,
        fontSize: 11,
      },
    },
  };
}

function SoilingScbModal({ scbId, plantId, dateFrom, dateTo, onClose }) {
  const h = React.createElement;
  const chartRef = React.useRef(null);
  const echartsRef = React.useRef(null);
  const [loading, setLoading] = useState(true);
  const [payload, setPayload] = useState(null);

  useEffect(() => {
    if (!plantId || !scbId) return;
    setLoading(true);
    setPayload(null);
    window.SolarAPI.Faults.scbSoilingTrend(plantId, scbId, dateFrom, dateTo)
      .then(setPayload)
      .catch(() => setPayload(null))
      .finally(() => setLoading(false));
  }, [plantId, scbId, dateFrom, dateTo]);

  useEffect(() => {
    if (!window.echarts || !chartRef.current || !payload || !payload.series || payload.series.length === 0) return;
    const echarts = window.echarts;
    if (!echartsRef.current) echartsRef.current = echarts.init(chartRef.current);
    const series = payload.series;
    const xData = series.map(d => d.date);
    const yData = series.map(d => d.ratio);
    const perf = echartsFaultModalPerf(xData.length);
    const sub = [];
    if (payload.median_daily_slope_ratio != null) sub.push(`Median daily Δ ratio: ${payload.median_daily_slope_ratio}`);
    if (payload.regression_slope_ratio_per_day != null) sub.push(`Regression slope: ${payload.regression_slope_ratio_per_day} /day`);
    const option = {
      ...ECHARTS_UNIFIED,
      animation: perf.animation,
      title: {
        text: 'Daily ratio (I ÷ (kWdc × irr/1000))',
        subtext: sub.join('  |  '),
        left: 'center',
        textStyle: { fontSize: 14 },
        subtextStyle: { color: '#64748b', fontSize: 11 },
      },
      grid: { left: 55, right: 20, top: 72, bottom: 48 },
      dataZoom: [
        { type: 'inside', start: 0, end: 100 },
      ],
      xAxis: { type: 'category', data: xData, ...perf.xAxis },
      yAxis: { type: 'value', name: 'Ratio', nameTextStyle: { color: '#64748b' } },
      series: [{
        name: 'Daily ratio',
        type: 'line',
        data: yData,
        smooth: perf.lineSmooth,
        symbol: 'none',
        lineStyle: { color: '#14b8a6', width: 2 },
        areaStyle: { color: 'rgba(20,184,166,0.08)' },
      }],
      tooltip: { trigger: 'axis' },
    };
    echartsRef.current.setOption(option, { notMerge: true, lazyUpdate: true });
    const onResize = () => echartsRef.current && echartsRef.current.resize();
    window.addEventListener('resize', onResize);
    return () => {
      window.removeEventListener('resize', onResize);
      if (echartsRef.current) {
        try { echartsRef.current.dispose(); } catch (e) { /* noop */ }
        echartsRef.current = null;
      }
    };
  }, [payload]);

  return h('div', {
    className: 'modal-overlay',
    onClick: (e) => { if (e.target === e.currentTarget) onClose(); },
    style: { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 99999 },
  },
    h('div', { className: 'modal-content', style: { background: 'var(--panel)', borderRadius: 12, padding: 24, maxWidth: 920, width: '95%', maxHeight: '90vh', overflow: 'auto' } },
      h('div', { style: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 12 } },
        h('h3', { style: { margin: 0 } }, `Soiling: ${scbId}`),
        h('button', { className: 'btn btn-outline', onClick: onClose }, 'Close'),
      ),
      loading && h('div', { style: { padding: 40, textAlign: 'center' } }, h(Spinner), ' Loading…'),
      !loading && (!payload || !payload.series || payload.series.length === 0) && h('div', { className: 'empty-state', style: { minHeight: 160 } }, 'No daily ratio data for this SCB in the selected range.'),
      !loading && payload && payload.series && payload.series.length > 0 && h('div', { style: { position: 'relative', height: 400, marginTop: 8 } },
        h('div', { ref: chartRef, style: { width: '100%', height: '100%' } }),
      ),
    ),
  );
}

const ISInvestigateModal = ({ inverterId, plantId, dateFrom, dateTo, onClose }) => {
  const h = React.createElement;
  const chartRef = React.useRef(null);
  const echartsRef = React.useRef(null);
  const [loading, setLoading] = useState(true);
  const [timelineData, setTimelineData] = useState([]);

  useEffect(() => {
    if (!plantId || !inverterId) return;
    setLoading(true);
    window.SolarAPI.Faults.isTimeline(plantId, inverterId, dateFrom, dateTo)
      .then(res => {
        setTimelineData((res && res.data) || []);
        setLoading(false);
      })
      .catch(() => { setTimelineData([]); setLoading(false); });
  }, [plantId, inverterId, dateFrom, dateTo]);

  const chartData = useMemo(() => {
    return [...timelineData]
      .sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp)))
      .map(d => ({
        timestamp: d.timestamp,
        formatted_time: String(d.timestamp).slice(5, 16),
        ac_power_kw: d.ac_power_kw || 0,
        irradiance: d.irradiance != null ? d.irradiance : null,
        shutdown: !!d.shutdown,
      }));
  }, [timelineData]);

  const shutdownAreas = [];
  let start = null;
  chartData.forEach(d => {
    if (d.shutdown && !start) start = d.formatted_time;
    else if (!d.shutdown && start) {
      shutdownAreas.push({ start, end: d.formatted_time });
      start = null;
    }
  });
  if (start && chartData.length) shutdownAreas.push({ start, end: chartData[chartData.length - 1].formatted_time });

  useEffect(() => {
    if (!window.echarts || !chartRef.current || !chartData.length) return;
    const echarts = window.echarts;
    if (!echartsRef.current) echartsRef.current = echarts.init(chartRef.current);
    const xData = chartData.map(d => d.formatted_time);
    const perf = echartsFaultModalPerf(xData.length);
    const sm = perf.lineSmooth;
    const acMax = axisMaxFromValues(chartData.map(d => d.ac_power_kw), 0.10, 10);
    const irrMax = axisMaxFromValues(chartData.map(d => d.irradiance), 0.18, 10);
    const option = {
      ...ECHARTS_UNIFIED,
      animation: perf.animation,
      grid: { left: 55, right: 70, top: 40, bottom: 90 },
      legend: { show: true, bottom: 48, textStyle: { color: '#1e293b' } },
      dataZoom: [
        { type: 'inside', start: 0, end: 100 },
      ],
      xAxis: { type: 'category', data: xData, ...perf.xAxis },
      yAxis: [
        { type: 'value', min: 0, max: acMax, name: 'AC Power (kW)', position: 'left', nameTextStyle: { color: '#64748b' }, axisLabel: { formatter: (v) => Number(v).toLocaleString() } },
        { type: 'value', min: 0, max: irrMax, name: 'Irradiance (W/m²)', position: 'right', nameTextStyle: { color: '#f59e0b' }, axisLabel: { color: '#f59e0b', formatter: (v) => Number(v).toLocaleString() }, splitLine: { show: false }, alignTicks: true }
      ],
      series: [
        {
          name: 'AC Power',
          type: 'line',
          data: chartData.map(d => d.ac_power_kw),
          smooth: sm,
          symbol: 'none',
          lineStyle: { color: '#06b6d4', width: 2 },
          itemStyle: { color: '#06b6d4' },
          markArea: shutdownAreas.length > 0 ? {
            itemStyle: { color: 'rgba(239,68,68,0.12)' },
            data: shutdownAreas.map(a => [{ xAxis: a.start }, { xAxis: a.end }]),
          } : undefined
        },
        {
          name: 'Irradiance',
          type: 'line',
          yAxisIndex: 1,
          data: chartData.map(d => d.irradiance),
          smooth: sm,
          symbol: 'none',
          lineStyle: { color: '#f59e0b', width: 1.5 },
          areaStyle: { color: 'rgba(245,158,11,0.10)' },
          itemStyle: { color: '#f59e0b' },
        }
      ],
      tooltip: {
        trigger: 'axis',
        backgroundColor: 'var(--panel)',
        borderColor: 'var(--line)',
        textStyle: { color: 'var(--text)', fontSize: 12 },
        formatter: (params) => {
          let str = `<div><strong>${params[0].axisValue}</strong></div>`;
          params.forEach(p => { str += `<div>${p.marker} ${p.seriesName}: <strong>${p.value != null ? p.value : 'N/A'}</strong></div>`; });
          const idx = params[0].dataIndex;
          if (chartData[idx]?.shutdown) str += `<div style="color:#ef4444;margin-top:4px;font-weight:bold;">Shutdown Condition Met</div>`;
          return str;
        }
      }
    };
    echartsRef.current.setOption(option, { notMerge: true, lazyUpdate: true });
    const onResize = () => echartsRef.current && echartsRef.current.resize();
    window.addEventListener('resize', onResize);
    return () => {
      window.removeEventListener('resize', onResize);
      if (echartsRef.current) {
        try { echartsRef.current.dispose(); } catch (e) { /* noop */ }
        echartsRef.current = null;
      }
    };
  }, [chartData]);

  return h('div', {
    className: 'modal-overlay',
    onClick: (e) => { if (e.target === e.currentTarget) onClose(); },
    style: { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 99999 }
  },
    h('div', { className: 'modal-content', style: { background: 'var(--panel)', borderRadius: 12, padding: 24, maxWidth: 1000, width: '95%', maxHeight: '90vh', overflow: 'auto' } },
      h('div', { style: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 } },
        h('h3', { style: { margin: 0 } }, `Inverter Shutdown: ${inverterId}`),
        h('button', { className: 'btn btn-outline', onClick: onClose }, 'Close')
      ),
      h('div', { style: { color: 'var(--text-muted)', fontSize: 13, marginBottom: 12 } },
        'Rule: AC Power = 0 and Irradiance > 5 W/m². Red shaded regions are detected shutdown windows.'
      ),
      loading && h('div', { style: { padding: 40, textAlign: 'center' } }, h(Spinner), ' Loading…'),
      !loading && chartData.length === 0 && h('div', { className: 'empty-state', style: { minHeight: 180 } }, 'No timeline data for selected inverter/range.'),
      !loading && chartData.length > 0 && h('div', { style: { position: 'relative', height: 420 } },
        h('div', { ref: chartRef, style: { width: '100%', height: '100%' } })
      )
    )
  );
};

const GBInvestigateModal = ({ eventId, plantId, dateFrom, dateTo, onClose }) => {
  const h = React.createElement;
  const chartRef = React.useRef(null);
  const echartsRef = React.useRef(null);
  const [loading, setLoading] = useState(true);
  const [timelineData, setTimelineData] = useState([]);

  useEffect(() => {
    if (!plantId) return;
    setLoading(true);
    window.SolarAPI.Faults.gbTimeline(plantId, dateFrom, dateTo)
      .then(res => {
        setTimelineData((res && res.data) || []);
        setLoading(false);
      })
      .catch(() => { setTimelineData([]); setLoading(false); });
  }, [plantId, dateFrom, dateTo]);

  const chartData = useMemo(() => {
    return [...timelineData]
      .sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp)))
      .map(d => ({
        timestamp: d.timestamp,
        formatted_time: String(d.timestamp).slice(5, 16),
        inverter_count: Number(d.inverter_count || 0),
        zero_power_inverter_count: Number(d.zero_power_inverter_count || 0),
        irradiance: d.irradiance != null ? Number(d.irradiance) : null,
        grid_breakdown: !!d.grid_breakdown,
      }));
  }, [timelineData]);

  const breakdownAreas = [];
  let start = null;
  chartData.forEach(d => {
    if (d.grid_breakdown && !start) start = d.formatted_time;
    else if (!d.grid_breakdown && start) {
      breakdownAreas.push({ start, end: d.formatted_time });
      start = null;
    }
  });
  if (start && chartData.length) breakdownAreas.push({ start, end: chartData[chartData.length - 1].formatted_time });

  useEffect(() => {
    if (!window.echarts || !chartRef.current || !chartData.length) return;
    const echarts = window.echarts;
    if (!echartsRef.current) echartsRef.current = echarts.init(chartRef.current);
    const xData = chartData.map(d => d.formatted_time);
    const perf = echartsFaultModalPerf(xData.length);
    const sm = perf.lineSmooth;
    const invMax = axisMaxFromValues(chartData.flatMap(d => [d.inverter_count, d.zero_power_inverter_count]), 0.15, 10);
    const irrMax = axisMaxFromValues(chartData.map(d => d.irradiance), 0.18, 10);
    const option = {
      ...ECHARTS_UNIFIED,
      animation: perf.animation,
      grid: { left: 55, right: 70, top: 40, bottom: 90 },
      legend: { show: true, bottom: 48, textStyle: { color: '#1e293b' } },
      dataZoom: [
        { type: 'inside', start: 0, end: 100 },
      ],
      xAxis: { type: 'category', data: xData, ...perf.xAxis },
      yAxis: [
        { type: 'value', min: 0, max: invMax, name: 'Inverters', position: 'left', nameTextStyle: { color: '#64748b' }, axisLabel: { formatter: (v) => Number(v).toLocaleString() } },
        { type: 'value', min: 0, max: irrMax, name: 'Irradiance (W/m²)', position: 'right', nameTextStyle: { color: '#f59e0b' }, axisLabel: { color: '#f59e0b', formatter: (v) => Number(v).toLocaleString() }, splitLine: { show: false }, alignTicks: true }
      ],
      series: [
        {
          name: 'Zero-Power Inverters',
          type: 'line',
          data: chartData.map(d => d.zero_power_inverter_count),
          smooth: sm,
          symbol: 'none',
          lineStyle: { color: '#ef4444', width: 2.2 },
          itemStyle: { color: '#ef4444' },
          markArea: breakdownAreas.length > 0 ? {
            itemStyle: { color: 'rgba(239,68,68,0.12)' },
            data: breakdownAreas.map(a => [{ xAxis: a.start }, { xAxis: a.end }]),
          } : undefined
        },
        {
          name: 'Total Inverters',
          type: 'line',
          data: chartData.map(d => d.inverter_count),
          smooth: sm,
          symbol: 'none',
          lineStyle: { color: '#06b6d4', width: 1.8 },
          itemStyle: { color: '#06b6d4' },
        },
        {
          name: 'Irradiance',
          type: 'line',
          yAxisIndex: 1,
          data: chartData.map(d => d.irradiance),
          smooth: sm,
          symbol: 'none',
          lineStyle: { color: '#f59e0b', width: 1.5 },
          areaStyle: { color: 'rgba(245,158,11,0.10)' },
          itemStyle: { color: '#f59e0b' },
        }
      ],
      tooltip: {
        trigger: 'axis',
        backgroundColor: 'var(--panel)',
        borderColor: 'var(--line)',
        textStyle: { color: 'var(--text)', fontSize: 12 },
        formatter: (params) => {
          let str = `<div><strong>${params[0].axisValue}</strong></div>`;
          params.forEach(p => { str += `<div>${p.marker} ${p.seriesName}: <strong>${p.value != null ? p.value : 'N/A'}</strong></div>`; });
          const idx = params[0].dataIndex;
          if (chartData[idx]?.grid_breakdown) str += `<div style="color:#ef4444;margin-top:4px;font-weight:bold;">Grid Breakdown Condition Met</div>`;
          return str;
        }
      }
    };
    echartsRef.current.setOption(option, { notMerge: true, lazyUpdate: true });
    const onResize = () => echartsRef.current && echartsRef.current.resize();
    window.addEventListener('resize', onResize);
    return () => {
      window.removeEventListener('resize', onResize);
      if (echartsRef.current) {
        try { echartsRef.current.dispose(); } catch (e) { /* noop */ }
        echartsRef.current = null;
      }
    };
  }, [chartData]);

  return h('div', {
    className: 'modal-overlay',
    onClick: (e) => { if (e.target === e.currentTarget) onClose(); },
    style: { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 99999 }
  },
    h('div', { className: 'modal-content', style: { background: 'var(--panel)', borderRadius: 12, padding: 24, maxWidth: 1050, width: '95%', maxHeight: '90vh', overflow: 'auto' } },
      h('div', { style: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 } },
        h('h3', { style: { margin: 0 } }, `Grid Breakdown: ${eventId}`),
        h('button', { className: 'btn btn-outline', onClick: onClose }, 'Close')
      ),
      h('div', { style: { color: 'var(--text-muted)', fontSize: 13, marginBottom: 12 } },
        'Rule: all inverters AC Power = 0 and Irradiance > 5 W/m². Red shaded regions are detected grid breakdown windows.'
      ),
      loading && h('div', { style: { padding: 40, textAlign: 'center' } }, h(Spinner), ' Loading...'),
      !loading && chartData.length === 0 && h('div', { className: 'empty-state', style: { minHeight: 180 } }, 'No timeline data for selected range.'),
      !loading && chartData.length > 0 && h('div', { style: { position: 'relative', height: 440 } },
        h('div', { ref: chartRef, style: { width: '100%', height: '100%' } })
      )
    )
  );
};

const CommunicationIssueModal = ({ equipmentLevel, equipmentId, issueKind, inverterId, plantId, dateFrom, dateTo, onClose }) => {
  const h = React.createElement;
  const chartRef = React.useRef(null);
  const echartsRef = React.useRef(null);
  const [loading, setLoading] = useState(true);
  const [timelineData, setTimelineData] = useState([]);

  useEffect(() => {
    if (!plantId || !equipmentLevel || !equipmentId) return;
    setLoading(true);
    window.SolarAPI.Faults.commTimeline(plantId, equipmentLevel, equipmentId, issueKind, dateFrom, dateTo)
      .then((res) => {
        setTimelineData((res && res.data) || []);
        setLoading(false);
      })
      .catch(() => {
        setTimelineData([]);
        setLoading(false);
      });
  }, [plantId, equipmentLevel, equipmentId, issueKind, dateFrom, dateTo]);

  const level = String(equipmentLevel || '').toLowerCase();
  const metricLabel = level === 'scb' ? 'DC Current' : 'Active Power';
  const metricUnit = level === 'scb' ? 'A' : 'kW';

  const chartData = useMemo(() => {
    const metricKey = level === 'scb' ? 'dc_current_a' : 'active_power_kw';
    return [...timelineData]
      .sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp)))
      .map((d) => ({
        timestamp: d.timestamp,
        formatted_time: String(d.timestamp || '').replace('T', ' ').slice(0, 16),
        metric: d[metricKey] != null ? Number(d[metricKey]) : null,
        irradiance: d.irradiance != null ? Number(d.irradiance) : null,
        communication_issue: !!d.communication_issue,
      }));
  }, [timelineData, level]);

  const commAreas = [];
  let start = null;
  chartData.forEach((d) => {
    if (d.communication_issue && !start) start = d.timestamp;
    else if (!d.communication_issue && start) {
      commAreas.push({ start, end: d.timestamp });
      start = null;
    }
  });
  if (start && chartData.length) commAreas.push({ start, end: chartData[chartData.length - 1].timestamp });

  useEffect(() => {
    if (!window.echarts || !chartRef.current || !chartData.length) return;
    const echarts = window.echarts;
    if (!echartsRef.current) echartsRef.current = echarts.init(chartRef.current);
    const xData = chartData.map((d) => d.timestamp);
    const perf = echartsFaultModalPerf(xData.length);
    const sm = perf.lineSmooth;
    const metricMax = axisMaxFromValues(chartData.map((d) => d.metric), 0.18, 10);
    const irrMax = axisMaxFromValues(chartData.map((d) => d.irradiance), 0.18, 10);
    const option = {
      ...ECHARTS_UNIFIED,
      animation: perf.animation,
      grid: { left: 55, right: 70, top: 40, bottom: 90 },
      legend: { show: true, bottom: 48, textStyle: { color: '#1e293b' } },
      dataZoom: [
        { type: 'inside', start: 0, end: 100 },
      ],
      xAxis: { type: 'category', data: xData, ...perf.xAxis, axisLabel: { ...(perf.xAxis && perf.xAxis.axisLabel ? perf.xAxis.axisLabel : {}), formatter: (value) => String(value || '').replace('T', '\n').slice(0, 16) } },
      yAxis: [
        { type: 'value', min: 0, max: metricMax, name: `${metricLabel} (${metricUnit})`, position: 'left', nameTextStyle: { color: '#64748b' }, axisLabel: { formatter: (v) => Number(v).toLocaleString() } },
        { type: 'value', min: 0, max: irrMax, name: 'WMS (W/m²)', position: 'right', nameTextStyle: { color: '#f59e0b' }, axisLabel: { color: '#f59e0b', formatter: (v) => Number(v).toLocaleString() }, splitLine: { show: false }, alignTicks: true }
      ],
      series: [
        {
          name: metricLabel,
          type: 'line',
          data: chartData.map((d) => d.metric),
          smooth: sm,
          symbol: 'none',
          lineStyle: { color: '#06b6d4', width: 2.2 },
          itemStyle: { color: '#06b6d4' },
          connectNulls: false,
          markArea: commAreas.length > 0 ? {
            itemStyle: { color: 'rgba(239,68,68,0.12)' },
            data: commAreas.map((a) => [{ xAxis: a.start }, { xAxis: a.end }]),
          } : undefined
        },
        {
          name: 'WMS',
          type: 'line',
          yAxisIndex: 1,
          data: chartData.map((d) => d.irradiance),
          smooth: sm,
          symbol: 'none',
          lineStyle: { color: '#f59e0b', width: 1.5 },
          areaStyle: { color: 'rgba(245,158,11,0.10)' },
          itemStyle: { color: '#f59e0b' },
        }
      ],
      tooltip: {
        trigger: 'axis',
        backgroundColor: 'var(--panel)',
        borderColor: 'var(--line)',
        textStyle: { color: 'var(--text)', fontSize: 12 },
        formatter: (params) => {
          const axisTs = params[0] && params[0].axisValue ? String(params[0].axisValue).replace('T', ' ').slice(0, 16) : '';
          let str = `<div><strong>${axisTs}</strong></div>`;
          params.forEach((p) => { str += `<div>${p.marker} ${p.seriesName}: <strong>${p.value != null ? p.value : 'N/A'}</strong></div>`; });
          const idx = params[0].dataIndex;
          if (chartData[idx]?.communication_issue) str += `<div style="color:#ef4444;margin-top:4px;font-weight:bold;">Communication issue window</div>`;
          return str;
        }
      }
    };
    echartsRef.current.setOption(option, { notMerge: true, lazyUpdate: true });
    const onResize = () => echartsRef.current && echartsRef.current.resize();
    window.addEventListener('resize', onResize);
    return () => {
      window.removeEventListener('resize', onResize);
      if (echartsRef.current) {
        try { echartsRef.current.dispose(); } catch (e) { /* noop */ }
        echartsRef.current = null;
      }
    };
  }, [chartData, metricLabel, metricUnit]);

  const titlePrefix = level === 'plant' ? 'Plant Communication Issue' : (level === 'scb' ? 'SCB Communication Issue' : 'Inverter Communication Issue');
  const ruleText = (() => {
    if (issueKind === 'plant_communication') return 'Rule: all inverters are missing while WMS irradiance is available. Red shaded regions are plant-owned communication windows.';
    if (issueKind === 'inverter_communication') return 'Rule: inverter active power is missing while WMS irradiance is available. Red shaded regions are inverter-owned communication windows.';
    if (issueKind === 'all_scbs_missing_inverter_present') return 'Rule: all SCBs under this inverter are missing while inverter active power is still present. This is treated as a no-loss ingestion gap.';
    if (issueKind === 'scb_data_missing') return 'Rule: SCB current is missing while inverter telemetry is present. This is treated as a no-loss ingestion gap.';
    return `Rule: WMS irradiance is available but ${metricLabel.toLowerCase()} data is missing. Red shaded regions are detected communication windows.`;
  })();

  return h('div', {
    className: 'modal-overlay',
    onClick: (e) => { if (e.target === e.currentTarget) onClose(); },
    style: { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 99999 }
  },
    h('div', { className: 'modal-content', style: { background: 'var(--panel)', borderRadius: 12, padding: 24, maxWidth: 1050, width: '95%', maxHeight: '90vh', overflow: 'auto' } },
      h('div', { style: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 } },
        h('h3', { style: { margin: 0 } }, `${titlePrefix}: ${equipmentId}`),
        h('button', { className: 'btn btn-outline', onClick: onClose }, 'Close')
      ),
      h('div', { style: { color: 'var(--text-muted)', fontSize: 13, marginBottom: 12 } },
        ruleText,
        inverterId ? ` Parent inverter: ${inverterId}.` : ''
      ),
      loading && h('div', { style: { padding: 40, textAlign: 'center' } }, h(Spinner), ' Loading...'),
      !loading && chartData.length === 0 && h('div', { className: 'empty-state', style: { minHeight: 180 } }, 'No timeline data for selected equipment/range.'),
      !loading && chartData.length > 0 && h('div', { style: { position: 'relative', height: 440 } },
        h('div', { ref: chartRef, style: { width: '100%', height: '100%' } })
      )
    )
  );
};

const PLInvestigateModal = ({ inverterId, archList = [], plantId, dateFrom, dateTo, onClose }) => {
  const h = React.createElement;
  const chartRef = React.useRef(null);
  const echartsRef = React.useRef(null);
  const [timelineData, setTimelineData] = useState([]);
  const [timelineCompare, setTimelineCompare] = useState([]);
  const [compareInvId, setCompareInvId] = useState('');
  const [irradianceAvailable, setIrradianceAvailable] = useState(false);
  const [loading, setLoading] = useState(true);

  // Extract unique inverters from archList
  const compareInvOptions = Array.from(new Set((archList || []).map(a => a.inverter_id).filter(Boolean))).filter(id => id !== inverterId);

  useEffect(() => {
    if (!plantId || !inverterId) return;
    setLoading(true);
    setCompareInvId('');
    setTimelineCompare([]);
    setIrradianceAvailable(false);
    window.SolarAPI.Faults.plTimeline(plantId, inverterId, dateFrom, dateTo)
      .then(res => { 
        setTimelineData(res.data || []); 
        // Notice if irradiance exists in ANY row, not just the very first point
        if (res.data && res.data.some(d => d.irradiance !== undefined && d.irradiance !== null)) {
          setIrradianceAvailable(true);
        }
        setLoading(false); 
      })
      .catch(() => { setTimelineData([]); setLoading(false); });
  }, [plantId, inverterId, dateFrom, dateTo]);

  useEffect(() => {
    if (!compareInvId || !plantId) { setTimelineCompare([]); return; }
    window.SolarAPI.Faults.plTimeline(plantId, compareInvId, dateFrom, dateTo)
      .then(res => setTimelineCompare(res.data || []))
      .catch(() => setTimelineCompare([]));
  }, [compareInvId, plantId, dateFrom, dateTo]);

  const chartData = useMemo(() => {
    const sorted = [...timelineData].sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp)));
    const primary = sorted.map(d => ({
      ts: d.timestamp,
      exp: d.expected_ac_kw || 0,
      act: d.actual_ac_kw || 0,
      isLimited: !!d.limited,
      irradiance: d.irradiance != null ? d.irradiance : null
    }));

    if (!compareInvId || !timelineCompare.length) {
      return primary.map(d => ({
        timestamp: d.ts,
        formatted_time: plXAxisLabel(d.ts),
        expected: d.exp,
        actual: d.act,
        isLimited: d.isLimited,
        irradiance: d.irradiance
      }));
    }

    const compareMap = {};
    timelineCompare.forEach(d => {
      compareMap[d.timestamp] = { exp: d.expected_ac_kw || 0, act: d.actual_ac_kw || 0 };
    });

    const times = new Set(primary.map(p => p.ts));
    timelineCompare.forEach(d => times.add(d.timestamp));

    return Array.from(times).sort().map(ts => {
      const p = primary.find(x => x.ts === ts);
      const c = compareMap[ts];
      const row = { timestamp: ts, formatted_time: plXAxisLabel(ts) };
      if (p) {
        row.expected = p.exp; row.actual = p.act; row.isLimited = p.isLimited; row.irradiance = p.irradiance;
      } else {
        row.expected = null; row.actual = null; row.isLimited = false; row.irradiance = null;
      }
      if (c) {
        row.expectedCompare = c.exp; row.actualCompare = c.act;
      }
      return row;
    });
  }, [timelineData, timelineCompare, compareInvId]);

  // Find fault areas for background shading
  const faultAreas = [];
  let start = null;
  chartData.forEach(d => {
    if (d.isLimited && !start) start = d.formatted_time;
    else if (!d.isLimited && start) {
      faultAreas.push({ start, end: d.formatted_time });
      start = null;
    }
  });
  if (start && chartData.length) faultAreas.push({ start, end: chartData[chartData.length - 1].formatted_time });

  useEffect(() => {
    if (!window.echarts || !chartRef.current || !chartData.length) return;
    const echarts = window.echarts;
    if (!echartsRef.current) echartsRef.current = echarts.init(chartRef.current);
    const xData = chartData.map(d => d.formatted_time);
    const perf = echartsFaultModalPerf(xData.length);
    const sm = perf.lineSmooth;

    // Determine if any irradiance data exists
    const hasIrr = irradianceAvailable && chartData.some(d => d.irradiance != null);
    const powerVals = [];
    chartData.forEach(d => {
      powerVals.push(d.expected, d.actual, d.expectedCompare, d.actualCompare);
    });
    const acMax = axisMaxFromValues(powerVals, 0.08, 10);
    const irrMax = axisMaxFromValues(chartData.map(d => d.irradiance), 0.08, 10);
    const accentColor = themeCssColor('--accent', '#3eb7df');

    const series = [
      { name: (compareInvId ? inverterId + ' Ref' : 'Reference (Median)'), type: 'line', data: chartData.map(d => d.expected), smooth: sm, symbol: 'none', lineStyle: { color: '#22c55e', width: 2 }, itemStyle: { color: '#22c55e' }, emphasis: { focus: 'none' } },
      { name: (compareInvId ? inverterId + ' Actual' : 'Actual'), type: 'line', data: chartData.map(d => d.actual), smooth: sm, symbol: 'none', lineStyle: { color: accentColor, width: 2 }, itemStyle: { color: accentColor },
        emphasis: { focus: 'none', lineStyle: { color: accentColor, width: 2 }, itemStyle: { color: accentColor } },
        markArea: faultAreas.length > 0 ? {
          itemStyle: { color: 'rgba(239, 68, 68, 0.1)' },
          data: faultAreas.map(area => [{ xAxis: area.start }, { xAxis: area.end }])
        } : undefined
      }
    ];

    if (compareInvId && chartData.some(d => d.expectedCompare != null)) {
      series.push({ name: compareInvId + ' Ref', type: 'line', data: chartData.map(d => d.expectedCompare), smooth: sm, symbol: 'none', lineStyle: { color: '#86efac', type: 'dashed' }, itemStyle: { color: '#86efac' }, emphasis: { focus: 'none' } });
      series.push({ name: compareInvId + ' Actual', type: 'line', data: chartData.map(d => d.actualCompare), smooth: sm, symbol: 'none', lineStyle: { color: '#38bdf8', type: 'dashed' }, itemStyle: { color: '#38bdf8' }, emphasis: { focus: 'none' } });
    }

    if (hasIrr) {
      series.push({
        name: 'Irradiance (W/m²)',
        type: 'line',
        yAxisIndex: 1,
        data: chartData.map(d => d.irradiance),
        smooth: sm,
        symbol: 'none',
        lineStyle: { color: '#f59e0b', width: 1.5, type: 'solid' },
        areaStyle: { color: 'rgba(245,158,11,0.10)' },
        itemStyle: { color: '#f59e0b' },
        emphasis: { focus: 'none' },
      });
    }

    const yAxes = [
      { type: 'value', min: 0, max: acMax, splitNumber: 5, alignTicks: true, name: 'AC Power (kW)', position: 'left', nameTextStyle: { color: '#64748b' } }
    ];
    if (hasIrr) {
      yAxes.push({
        type: 'value',
        min: 0,
        max: irrMax,
        splitNumber: 5,
        scale: false,
        name: 'Irradiance (W/m²)',
        position: 'right',
        nameTextStyle: { color: '#f59e0b' },
        axisLabel: { color: '#f59e0b' },
        splitLine: { show: false },
        alignTicks: true,
      });
    }

    const option = {
      ...ECHARTS_UNIFIED,
      animation: perf.animation,
      grid: { left: 55, right: hasIrr ? 70 : 55, top: 40, bottom: 90 },
      legend: { show: true, bottom: 48, textStyle: { color: '#1e293b' } },
      dataZoom: [
        { type: 'inside', xAxisIndex: 0, start: 0, end: 100, zoomOnMouseWheel: true, moveOnMouseMove: true },
        { type: 'inside', yAxisIndex: 0, zoomOnMouseWheel: 'shift', moveOnMouseMove: 'shift', filterMode: 'none' },
        ...(hasIrr ? [{ type: 'inside', yAxisIndex: 1, zoomOnMouseWheel: 'shift', moveOnMouseMove: 'shift', filterMode: 'none' }] : []),
      ],
      xAxis: { type: 'category', data: xData, ...perf.xAxis },
      yAxis: yAxes,
      series,
      tooltip: {
        trigger: 'axis',
        backgroundColor: 'var(--panel)',
        borderColor: 'var(--line)',
        textStyle: { color: 'var(--text)', fontSize: 12 },
        formatter: (params) => {
          let str = `<div><strong>${params[0].axisValue}</strong></div>`;
          params.forEach(p => {
             str += `<div>${p.marker} ${p.seriesName}: <strong>${p.value != null ? p.value : 'N/A'}</strong></div>`;
          });
          const dataIndex = params[0].dataIndex;
          if (chartData[dataIndex]?.isLimited) {
             str += `<div style="color: #ef4444; margin-top: 4px; font-weight: bold;">Power Limited</div>`;
          }
          return str;
        }
      }
    };
    echartsRef.current.setOption(option, { notMerge: true, lazyUpdate: true });
    const onResize = () => echartsRef.current && echartsRef.current.resize();
    window.addEventListener('resize', onResize);
    return () => {
      window.removeEventListener('resize', onResize);
      if (echartsRef.current) {
        try { echartsRef.current.dispose(); } catch (e) { /* noop */ }
        echartsRef.current = null;
      }
    };
  }, [chartData, inverterId, compareInvId, irradianceAvailable]);

  return h('div', {
    className: 'modal-overlay',
    onClick: (e) => { if (e.target === e.currentTarget) onClose(); },
    style: { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 99999 }
  },
    h('div', { className: 'modal-content', style: { background: 'var(--panel)', borderRadius: 12, padding: 24, maxWidth: 1000, width: '95%', maxHeight: '90vh', overflow: 'auto' } },
      
      h('div', { style: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 16 } },
        h('div', null,
          h('h3', { style: { margin: '0 0 8px 0' } }, `Power Limitation: ${inverterId}`),
          h('div', { style: { display: 'flex', gap: 16, alignItems: 'center' } },
            h('label', { style: { fontSize: 13, color: 'var(--text-soft)', display: 'flex', alignItems: 'center', gap: 8 } },
              'Compare with Inverter:',
              h('select', { className: 'form-input', style: { width: 180, padding: '4px 8px' }, value: compareInvId, onChange: e => setCompareInvId(e.target.value) },
                h('option', { value: '' }, 'None'),
                compareInvOptions.map(id => h('option', { key: id, value: id }, id))
              )
            )
          )
        ),
        h('button', { className: 'btn btn-outline', onClick: onClose }, 'Close')
      ),

      loading && h('div', { style: { padding: 40, textAlign: 'center' } }, h(Spinner), ' Loading…'),
      !loading && chartData.length === 0 && h('div', { className: 'empty-state', style: { minHeight: 200 } }, 'No timeline data for this inverter in 10:00–15:00.'),
      
      !loading && chartData.length > 0 && h('div', { style: { position: 'relative', height: 400, marginTop: 16 } },
        h('div', { ref: chartRef, style: { width: '100%', height: '100%' } })
      )
    )
  );
};


const FaultDetailModal = ({ scbId, scbStringsMap, archList = [], plantId, dateFrom, dateTo, summaryEnergyNote, onClose }) => {
  const h = React.createElement;
  const chartRef = React.useRef(null);
  const echartsRef = React.useRef(null);
  const [normalized, setNormalized] = useState(false);
  const [hiddenLines, setHiddenLines] = useState([]);
  const [showMetadata, setShowMetadata] = useState(false);
  const [timelineData, setTimelineData] = useState([]);
  const [tlLoading, setTlLoading] = useState(true);
  const [energyInfo, setEnergyInfo] = useState({ available: true, note: summaryEnergyNote || null });
  const [compareScbId, setCompareScbId] = useState('');
  const [timelineCompare, setTimelineCompare] = useState([]);
  const [metaPrimary, setMetaPrimary] = useState(null);
  const [metaCompare, setMetaCompare] = useState(null);
  const [irradianceAvailable, setIrradianceAvailable] = useState(false);
  const compareScbOptions = (archList || []).filter(a => a.scb_id && !a.spare_flag && a.scb_id !== scbId).map(a => a.scb_id);

  useEffect(() => {
    setTlLoading(true);
    setCompareScbId('');
    setTimelineCompare([]);
    setMetaPrimary(null);
    setMetaCompare(null);
    setIrradianceAvailable(false);
    window.SolarAPI.Faults.dsTimeline(plantId, scbId, dateFrom, dateTo)
      .then(res => {
        setTimelineData(res.data || []);
        setIrradianceAvailable(res.irradiance_available === true);
        setEnergyInfo({ available: res.energy_available !== false, note: res.energy_note || summaryEnergyNote || null });
        setTlLoading(false);
      })
      .catch(() => { setTimelineData([]); setTlLoading(false); });
    if (plantId && scbId) {
      window.SolarAPI.Metadata.scbMetadata(plantId, scbId).then(setMetaPrimary).catch(() => setMetaPrimary(null));
    }
  }, [scbId, plantId, dateFrom, dateTo, summaryEnergyNote]);

  useEffect(() => {
    if (!compareScbId || !plantId) { setTimelineCompare([]); setMetaCompare(null); return; }
    window.SolarAPI.Faults.dsTimeline(plantId, compareScbId, dateFrom, dateTo)
      .then(res => setTimelineCompare(res.data || []))
      .catch(() => setTimelineCompare([]));
    window.SolarAPI.Metadata.scbMetadata(plantId, compareScbId).then(setMetaCompare).catch(() => setMetaCompare(null));
  }, [compareScbId, plantId, dateFrom, dateTo]);

  const toggleLine = (e) => {
    setHiddenLines(p => p.includes(e.dataKey) ? p.filter(k => k !== e.dataKey) : [...p, e.dataKey]);
  };

  const stringCount = scbStringsMap[scbId] || 28;
  const stringCountCompare = compareScbId ? (scbStringsMap[compareScbId] || 28) : 0;

  const chartData = useMemo(() => {
    const sorted = [...timelineData].sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp)));
    const primary = sorted.map(d => {
      const mult = normalized ? 1 : stringCount;
      return { ts: d.timestamp, exp: (d.expected_current || 0) * mult, act: (d.virtual_string_current || 0) * mult, missing: d.missing_strings || 0, isFaulty: d.fault_status === 'CONFIRMED_DS', irradiance: d.irradiance != null ? d.irradiance : null };
    });
    if (!compareScbId || !timelineCompare.length) {
      return primary.map(d => ({
        timestamp: d.ts,
        formatted_time: String(d.ts).slice(5, 16),
        expected: Number(d.exp.toFixed(2)),
        actual: Number(d.act.toFixed(2)),
        missing: d.missing,
        isFaulty: d.isFaulty,
        irradiance: d.irradiance,
      }));
    }
    const compareMap = {};
    timelineCompare.forEach(d => {
      const mult = normalized ? 1 : stringCountCompare;
      compareMap[d.timestamp] = { act: (d.virtual_string_current || 0) * mult };
    });
    const times = new Set(primary.map(p => p.ts));
    timelineCompare.forEach(d => times.add(d.timestamp));
    return Array.from(times).sort().map(ts => {
      const p = primary.find(x => x.ts === ts);
      const c = compareMap[ts];
      const row = { timestamp: ts, formatted_time: String(ts).slice(5, 16) };
      if (p) { row.expected = Number(p.exp.toFixed(2)); row.actual = Number(p.act.toFixed(2)); row.missing = p.missing; row.isFaulty = p.isFaulty; row.irradiance = p.irradiance; } else { row.expected = null; row.actual = null; row.missing = 0; row.isFaulty = false; row.irradiance = null; }
      if (c) { row.actualCompare = Number(c.act.toFixed(2)); }
      return row;
    });
  }, [timelineData, timelineCompare, normalized, stringCount, stringCountCompare, compareScbId]);

  const faultAreas = [];
  let start = null;
  chartData.forEach(d => {
    if (d.isFaulty && !start) start = d.timestamp;
    else if (!d.isFaulty && start) {
      faultAreas.push({ start, end: d.timestamp });
      start = null;
    }
  });
  if (start && chartData.length) faultAreas.push({ start, end: chartData[chartData.length - 1].timestamp });

  useEffect(() => {
    if (!window.echarts || !chartRef.current || !chartData.length) return;
    const echarts = window.echarts;
    if (!echartsRef.current) echartsRef.current = echarts.init(chartRef.current);
    const xData = chartData.map(d => d.formatted_time);
    const perf = echartsFaultModalPerf(xData.length);
    const sm = perf.lineSmooth;

    // Determine if any irradiance data exists in this chartData
    const hasIrr = irradianceAvailable && chartData.some(d => d.irradiance != null);
    const currentVals = [];
    chartData.forEach(d => {
      currentVals.push(d.expected, d.actual, d.actualCompare);
    });
    const currMax = axisMaxFromValues(currentVals, 0.08, 10);
    const dsMax = axisMaxFromValues(chartData.map(d => d.missing), 0.12, 1);
    const irrMax = axisMaxFromValues(chartData.map(d => d.irradiance), 0.08, 10);

    const series = [
      { name: (compareScbId ? scbId + ' Ref' : 'Reference'), type: 'line', data: chartData.map(d => d.expected), smooth: sm, symbol: 'none' },
      { name: (compareScbId ? scbId + ' Actual' : 'Actual'), type: 'line', data: chartData.map(d => d.actual), smooth: sm, symbol: 'none' }
    ];
    if (compareScbId && chartData.some(d => d.actualCompare != null)) {
      series.push({ name: compareScbId + ' Actual', type: 'line', data: chartData.map(d => d.actualCompare), smooth: sm, symbol: 'none', lineStyle: { type: 'dashed' } });
    }
    series.push({ name: 'Disconnected Strings', type: 'bar', yAxisIndex: 1, data: chartData.map(d => d.missing), itemStyle: { color: '#ef4444', opacity: 0.6 } });

    // Irradiance series — third Y-axis (right side, offset so it doesn't overlap DS axis)
    if (hasIrr) {
      series.push({
        name: 'Irradiance (W/m²)',
        type: 'line',
        yAxisIndex: 2,
        data: chartData.map(d => d.irradiance),
        smooth: sm,
        symbol: 'none',
        lineStyle: { color: '#f59e0b', width: 1.5, type: 'solid' },
        areaStyle: { color: 'rgba(245,158,11,0.10)' },
        itemStyle: { color: '#f59e0b' },
      });
    }

    const yAxes = [
      { type: 'value', min: 0, max: currMax, splitNumber: 5, scale: false, alignTicks: true, name: 'Current (A)', position: 'left', nameTextStyle: { color: '#64748b' } },
      { type: 'value', min: 0, max: dsMax, splitNumber: 5, scale: false, name: 'Disc. Strings', position: 'right', offset: 0, allowDecimals: false, nameTextStyle: { color: '#ef4444' }, axisLabel: { color: '#ef4444' }, alignTicks: true },
    ];
    if (hasIrr) {
      yAxes.push({
        type: 'value',
        min: 0,
        max: irrMax,
        splitNumber: 5,
        scale: false,
        name: 'Irradiance (W/m²)',
        position: 'right',
        offset: 85,
        nameTextStyle: { color: '#f59e0b', padding: [0, 0, 0, 30] },
        axisLabel: { color: '#f59e0b' },
        splitLine: { show: false },
        alignTicks: true,
      });
    }

    const option = {
      ...ECHARTS_UNIFIED,
      animation: perf.animation,
      // Increased top margin from 30 to 55 to prevent labels from overlapping the toolbox
      // Increased right margin from 130 to 160 to accommodate the 85px offset
      grid: { left: 55, right: hasIrr ? 160 : 70, top: 55, bottom: 100 },
      legend: { show: true, bottom: 48, textStyle: { color: '#1e293b' } },
      dataZoom: [
        { type: 'inside', xAxisIndex: 0, start: 0, end: 100, zoomOnMouseWheel: true, moveOnMouseMove: true },
        { type: 'inside', yAxisIndex: 0, zoomOnMouseWheel: 'shift', moveOnMouseMove: 'shift', filterMode: 'none' },
        { type: 'inside', yAxisIndex: 1, zoomOnMouseWheel: 'shift', moveOnMouseMove: 'shift', filterMode: 'none' },
        ...(hasIrr ? [{ type: 'inside', yAxisIndex: 2, zoomOnMouseWheel: 'shift', moveOnMouseMove: 'shift', filterMode: 'none' }] : []),
      ],
      xAxis: { type: 'category', data: xData, ...perf.xAxis },
      yAxis: yAxes,
      series
    };
    echartsRef.current.setOption(option, { notMerge: true, lazyUpdate: true });
    const onResize = () => echartsRef.current && echartsRef.current.resize();
    window.addEventListener('resize', onResize);
    return () => {
      window.removeEventListener('resize', onResize);
      if (echartsRef.current) {
        try { echartsRef.current.dispose(); } catch (e) { /* noop */ }
        echartsRef.current = null;
      }
    };
  }, [chartData, scbId, compareScbId, irradianceAvailable]);

  const totalLoss = useMemo(() => {
    return timelineData.reduce((s, d) => s + (d.energy_loss_kwh || 0), 0);
  }, [timelineData]);

  return h('div', {
    onClick: (e) => { if (e.target === e.currentTarget) onClose(); },
    className: 'modal-backdrop',
    style: { position: 'fixed', top: 0, left: 0, right: 0, bottom: 0, background: 'rgba(0,0,0,0.55)', zIndex: 99999, display: 'flex', alignItems: 'center', justifyContent: 'center' }
  },
    h('div', { style: { background: 'var(--panel, white)', padding: 16, borderRadius: 10, width: '92%', maxWidth: 900, maxHeight: '88vh', overflowY: 'auto', position: 'relative', color: 'var(--text, #1e293b)', border: '1px solid var(--line, #e2e8f0)' } },
      h('button', {
        onClick: onClose,
        style: { position: 'absolute', top: 20, right: 20, border: 'none', background: 'transparent', fontSize: 24, cursor: 'pointer', color: '#64748b' }
      }, '\u00d7'),

      h('h3', { style: { marginBottom: 4 } }, `Detailed Analysis: ${scbId}`),
      h('div', { style: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 12, marginBottom: 12 } },
        h('p', { style: { color: 'var(--text-muted)', margin: 0, fontSize: 13 } }, `String Capacity: ${stringCount} strings`),
        compareScbOptions.length > 0 && h('div', { style: { display: 'flex', alignItems: 'center', gap: 8 } },
          h('label', { style: { fontSize: 13, color: '#64748b' } }, 'Compare with SCB:'),
          h('select', {
            value: compareScbId,
            onChange: e => setCompareScbId(e.target.value || ''),
            style: { minWidth: 160, padding: '6px 10px', border: '1px solid #cbd5e1', borderRadius: 6, fontSize: 13, color: '#1e293b', background: '#f8fafc', cursor: 'pointer' }
          }, h('option', { value: '' }, '— None —'), ...compareScbOptions.map(id => h('option', { key: id, value: id, style: { color: '#1e293b', background: '#fff' } }, id)))
        )
      ),
      h('div', { style: { marginBottom: 16 } },
        h('button', {
          type: 'button',
          className: showMetadata ? 'btn btn-primary' : 'btn btn-outline',
          onClick: () => setShowMetadata(v => !v),
          style: { marginBottom: showMetadata ? 12 : 0 }
        }, showMetadata ? 'Hide Parameters' : 'Show Parameters'),
        showMetadata && h('div', { style: { overflowX: 'auto', background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: 10, padding: 12 } },
          h('table', { style: { width: '100%', fontSize: 12, borderCollapse: 'collapse' } },
            h('thead', null, h('tr', null,
              h('th', { style: { textAlign: 'left', padding: '6px 8px', borderBottom: '1px solid #e2e8f0', color: '#64748b' } }, 'Parameter'),
              h('th', { style: { textAlign: 'left', padding: '6px 8px', borderBottom: '1px solid #e2e8f0', color: '#1e293b' } }, scbId),
              (compareScbId || metaCompare) && h('th', { style: { textAlign: 'left', padding: '6px 8px', borderBottom: '1px solid #e2e8f0', color: '#1e293b' } }, compareScbId)
            )),
            h('tbody', null,
              [
                ['number_of_strings', 'Number of Strings'],
                ['modules_per_string', 'Modules per String'],
                ['dc_capacity_kw', 'DC Capacity (kW)'],
                ['module_wp', 'Module Wp'],
                ['impp', 'Impp (A)'],
                ['vmpp', 'Vmpp (V)'],
              ].map(([param, label]) => {
                let v1 = metaPrimary && metaPrimary[param] != null ? metaPrimary[param] : (param === 'number_of_strings' ? (scbStringsMap[scbId] || '—') : '—');
                const v2 = metaCompare && metaCompare[param] != null ? metaCompare[param] : (param === 'number_of_strings' ? (scbStringsMap[compareScbId] || '—') : '—');
                return h('tr', { key: param },
                  h('td', { style: { padding: '6px 8px', color: '#64748b' } }, label),
                  h('td', { style: { padding: '6px 8px', color: '#1e293b', fontWeight: 500 } }, String(v1)),
                  (compareScbId || metaCompare) && h('td', { style: { padding: '6px 8px', color: '#1e293b', fontWeight: 500 } }, String(v2))
                );
              })
            )
          )
        )
      ),
      h('p', { style: { color: '#8B5CF6', marginBottom: 8, fontSize: 13, fontWeight: 600 } },
        energyInfo.available
          ? `Total Energy Loss: ${totalLoss.toFixed(2)} kWh (${(totalLoss / 1000).toFixed(3)} MWh)`
          : 'Total Energy Loss: N/A'
      ),
      energyInfo.note && h('div', {
        style: {
          marginBottom: 16, padding: 10, borderRadius: 10,
          background: '#fff7ed', color: '#9a3412', fontSize: 13,
          border: '1px solid #fdba74'
        }
      }, energyInfo.note),

      tlLoading
        ? h('div', { style: { height: 320, display: 'flex', alignItems: 'center', justifyContent: 'center' } }, h(Spinner), ' Loading timeline…')
        : chartData.length === 0
          ? h('div', { style: { height: 320, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-muted)' } }, 'No timeline data available for this SCB.')
          : h(React.Fragment, null,
              h('div', { style: { display: 'flex', gap: 10, marginBottom: 16, flexWrap: 'wrap' } },
                h('button', {
                  className: normalized ? 'btn btn-outline' : 'btn btn-primary',
                  onClick: () => setNormalized(false)
                }, 'Actual Currents (SCB Level)'),
                h('button', {
                  className: normalized ? 'btn btn-primary' : 'btn btn-outline',
                  onClick: () => setNormalized(true)
                }, 'Normalized (Per-String)')
              ),
              // ECharts chart is initialized via useEffect on chartRef — no Recharts components needed here
              h('div', { ref: chartRef, style: { height: 320, width: '100%' } })
            )
    )
  );
};

console.info('[solar-trace] fault_page.js initialization complete (window.FaultPage set)');




