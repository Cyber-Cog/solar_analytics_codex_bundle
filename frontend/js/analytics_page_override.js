// frontend/js/analytics_page_override.js
// Analytics Lab: multi-level hierarchy (e.g. WMS + SCB on one chart), WMS from raw_data_generic.

(() => {
  const { useState, useEffect, useRef, useMemo } = React;
  const { Card, Spinner, Toggle, EquipmentPicker, EChart } = window;

  /** Picker row id encodes level + equipment_id for API (equipment_id only is sent to timeseries). */
  function makePickerKey(level, equipmentId) {
    return `${level}:${encodeURIComponent(String(equipmentId))}`;
  }
  function parsePickerKey(key) {
    const i = String(key).indexOf(':');
    if (i < 0) return { level: null, equipmentId: key };
    return {
      level: key.slice(0, i),
      equipmentId: decodeURIComponent(key.slice(i + 1)),
    };
  }

  window.AnalyticsPage = ({ plantId, dateFrom, dateTo, onNavigate }) => {
    const h = React.createElement;
    const LEVELS = [
      { id: 'inverter', label: 'Inverter' },
      { id: 'scb', label: 'SCB' },
      { id: 'string', label: 'String' },
      { id: 'wms', label: 'WMS' },
    ];
    const LEVEL_ORDER = LEVELS.map(x => x.id);
    const levelLabel = (id) => (LEVELS.find(x => x.id === id) || {}).label || id;

    const PARAM_LABELS = {
      ac_power: 'AC Power (kW)',
      ambient_temp: 'Ambient Temperature (°C)',
      dc_current: 'DC Current (A)',
      dc_power: 'DC Power (kW)',
      dc_voltage: 'DC Voltage (V)',
      energy_export_kwh: 'Energy Export (kWh)',
      energy_generation_kwh: 'Energy Generation (kWh)',
      ghi: 'GHI (W/m²)',
      gti: 'GTI (W/m²)',
      irradiance: 'Irradiance (W/m²)',
      module_temp: 'Module Temperature (°C)',
      status: 'Status',
      string_count: 'String Count',
      temperature: 'Temperature (°C)',
      wind_speed: 'Wind Speed (m/s)',
    };
    const SIGNAL_ORDER = [
      'ac_power', 'dc_power', 'dc_current', 'dc_voltage',
      'energy_export_kwh', 'energy_generation_kwh',
      'irradiance', 'gti', 'ghi', 'temperature',
      'ambient_temp', 'module_temp', 'wind_speed',
      'string_count', 'status',
    ];
    const COLORS = ['#0EA5E9', '#F59E0B', '#10B981', '#8B5CF6', '#EC4899', '#EF4444', '#14B8A6', '#F97316'];

    const signalLabel = (sig) => PARAM_LABELS[sig] || sig.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
    // Legend-only label — drops the trailing unit like "(kW)" since axes already carry units.
    // Keeps the legend readable when many series are selected.
    const signalLabelShort = (sig) => String(signalLabel(sig)).replace(/\s*\([^)]*\)\s*$/, '').trim();
    const sortSignals = (signals = []) => [...signals].sort((a, b) => {
      const ai = SIGNAL_ORDER.indexOf(a);
      const bi = SIGNAL_ORDER.indexOf(b);
      if (ai === -1 && bi === -1) return a.localeCompare(b);
      if (ai === -1) return 1;
      if (bi === -1) return -1;
      return ai - bi;
    });

    const defaultSignalsMerged = (levels, signals = []) => {
      const preferred = ['gti', 'irradiance', 'ghi', 'dc_current', 'ac_power', 'energy_export_kwh', 'energy_generation_kwh', 'dc_voltage', 'dc_power', 'temperature', 'wind_speed'];
      const picked = preferred.filter(sig => signals.includes(sig));
      return (picked.length ? picked : signals).slice(0, 4);
    };

    const WMS_SIGNAL_FALLBACK = ['irradiance', 'ghi', 'gti', 'temperature', 'ambient_temp', 'module_temp', 'wind_speed'];

    /** Meteo / irradiance — use right Y-axis when mixed with electrical signals */
    const METEO_SIGNALS = new Set(['gti', 'ghi', 'irradiance', 'ambient_temp', 'module_temp', 'wind_speed', 'temperature']);

    /** Only plot signals that belong on that hierarchy row (no GTI under Inverter, no DC current under WMS). */
    function signalAllowedForHierarchy(sig, level) {
      if (!sig || !level) return false;
      if (level === 'wms') {
        return METEO_SIGNALS.has(sig);
      }
      if (level === 'inverter') {
        return new Set(['ac_power', 'dc_power', 'dc_current', 'dc_voltage', 'energy_export_kwh', 'energy_generation_kwh', 'status']).has(sig);
      }
      if (level === 'scb') {
        return new Set(['dc_current', 'dc_voltage', 'dc_power', 'string_count']).has(sig);
      }
      if (level === 'string') {
        const s = String(sig).toLowerCase();
        if (METEO_SIGNALS.has(sig) || METEO_SIGNALS.has(s)) return false;
        if (new Set(['dc_current', 'dc_voltage', 'dc_power', 'current', 'voltage']).has(s)) return true;
        if (s.includes('current') || (s.includes('voltage') && !s.includes('ac_'))) return true;
        return false;
      }
      return false;
    }

    function isMeteoAxisSignal(sig) {
      return METEO_SIGNALS.has(sig);
    }

    function formatTimestampLabel(ts, includeDate = true) {
      if (!ts) return '';
      const dt = new Date(ts);
      if (Number.isNaN(dt.getTime())) return String(ts);
      const yy = dt.getFullYear();
      const mm = String(dt.getMonth() + 1).padStart(2, '0');
      const dd = String(dt.getDate()).padStart(2, '0');
      const hh = String(dt.getHours()).padStart(2, '0');
      const mi = String(dt.getMinutes()).padStart(2, '0');
      return includeDate ? `${yy}-${mm}-${dd}\n${hh}:${mi}` : `${hh}:${mi}`;
    }

    function formatTooltipTs(ts) {
      if (!ts) return '';
      const dt = new Date(ts);
      if (Number.isNaN(dt.getTime())) return String(ts);
      const yy = dt.getFullYear();
      const mm = String(dt.getMonth() + 1).padStart(2, '0');
      const dd = String(dt.getDate()).padStart(2, '0');
      const hh = String(dt.getHours()).padStart(2, '0');
      const mi = String(dt.getMinutes()).padStart(2, '0');
      const ss = String(dt.getSeconds()).padStart(2, '0');
      return `${yy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
    }

    function buildTooltipFormatter() {
      return (items) => {
        const rows = Array.isArray(items) ? items : [items];
        if (!rows.length) return '';
        const title = formatTooltipTs(rows[0].axisValue);
        const body = rows.map(item => {
          const value = Array.isArray(item.value) ? item.value[1] : item.value;
          const shown = value == null || value === '' ? '—' : value;
          return `${item.marker}${item.seriesName}: ${shown}`;
        });
        return [title, ...body].join('<br/>');
      };
    }

    function buildToolbox() {
      return {
        show: true,
        right: 10,
        top: 6,
        itemSize: 14,
        feature: {
          dataZoom: { yAxisIndex: 'none' },
          dataView: { readOnly: true },
          magicType: { type: ['line', 'bar'] },
          restore: {},
          saveAsImage: {},
        },
      };
    }

    const [selectedLevels, setSelectedLevels] = useState(['inverter']);
    const [pickerItems, setPickerItems] = useState([]);
    const [availableSignals, setAvailableSignals] = useState([]);
    const [selected, setSelected] = useState([]);
    const [search, setSearch] = useState('');
    const [params, setParams] = useState([]);
    const [normalize, setNormalize] = useState(false);
    /** When true, inverter dc_current/dc_power prefer SUM(SCB) roll-up (can be ~100k A); default false = inverter telemetry */
    const [preferScbDcAggregate, setPreferScbDcAggregate] = useState(false);
    const [tsData, setTsData] = useState([]);
    const [avail, setAvail] = useState(0);
    const [loading, setLoading] = useState(false);
    const [equipLoading, setEquipLoading] = useState(false);
    const [signalLoading, setSignalLoading] = useState(false);
    const [hiddenLines, setHiddenLines] = useState([]);
    const [scbMetadata, setScbMetadata] = useState(null);

    useEffect(() => {
      if (!selected.length) return;
      const levels = selected.map(k => parsePickerKey(k).level).filter(Boolean);
      setParams(prev => prev.filter(sig => levels.some(lvl => signalAllowedForHierarchy(sig, lvl))));
    }, [selected]);

    const toggleLevel = (levelId) => {
      setSelectedLevels(prev => {
        if (prev.includes(levelId)) {
          if (prev.length <= 1) return prev;
          return prev.filter(x => x !== levelId);
        }
        const next = [...prev, levelId];
        return next.sort((a, b) => LEVEL_ORDER.indexOf(a) - LEVEL_ORDER.indexOf(b));
      });
    };

    // SCB metadata when exactly one SCB row is selected
    useEffect(() => {
      const scbKeys = selected.filter(k => parsePickerKey(k).level === 'scb');
      if (scbKeys.length !== 1 || !plantId) {
        setScbMetadata(null);
        return;
      }
      const scbId = parsePickerKey(scbKeys[0]).equipmentId;
      window.SolarAPI.Metadata.scbMetadata(plantId, scbId)
        .then(setScbMetadata)
        .catch(() => setScbMetadata(null));
    }, [plantId, selected]);

    useEffect(() => {
      if (!plantId || !selectedLevels.length) {
        setPickerItems([]);
        setAvailableSignals([]);
        setSelected([]);
        setParams([]);
        return;
      }

      setEquipLoading(true);
      setSignalLoading(true);
      setTsData([]);
      setAvail(0);
      setHiddenLines([]);

      // Per-level: don't let one failed /signals call wipe the whole Lab (Promise.all would reject).
      const fetches = selectedLevels.map(lvl =>
        Promise.allSettled([
          window.SolarAPI.Analytics.equipment(lvl, plantId),
          window.SolarAPI.Analytics.signals(lvl, plantId),
        ]).then(([equipSettled, signalSettled]) => ({
          level: lvl,
          equipResp: equipSettled.status === 'fulfilled' ? equipSettled.value : { equipment_ids: [] },
          signalResp: signalSettled.status === 'fulfilled' ? signalSettled.value : { signals: [] },
          _equipErr: equipSettled.status === 'rejected' ? equipSettled.reason : null,
          _sigErr: signalSettled.status === 'rejected' ? signalSettled.reason : null,
        }))
      );

      Promise.all(fetches)
        .then(results => {
          results.forEach(r => {
            if (r._equipErr) console.warn('Analytics equipment failed', r.level, r._equipErr);
            if (r._sigErr) console.warn('Analytics signals failed', r.level, r._sigErr);
          });
          const items = [];
          const signalSet = new Set();
          const seenKeys = new Set();

          results.forEach(({ level, equipResp, signalResp }) => {
            let nextEquip = equipResp.equipment_ids || [];
            let nextSignals = sortSignals(signalResp.signals || []);

            if (level === 'wms') {
              const looksLikeStringTags = nextEquip.length > 0 && nextEquip.every(id => String(id).startsWith('STR-'));
              if (looksLikeStringTags || nextEquip.length > 5) {
                nextEquip = [plantId];
              }
              if (plantId && !nextEquip.includes(plantId)) {
                nextEquip = [plantId, ...nextEquip];
              }
              if (nextSignals.length === 0) {
                nextSignals = WMS_SIGNAL_FALLBACK.slice();
              }
            }

            nextSignals.forEach(s => signalSet.add(s));

            nextEquip.forEach(eq => {
              const key = makePickerKey(level, eq);
              if (seenKeys.has(key)) return;
              seenKeys.add(key);
              items.push({
                id: key,
                label: `${eq} (${levelLabel(level)})`,
              });
            });
          });

          const mergedSignals = sortSignals([...signalSet]);
          setPickerItems(items);
          setAvailableSignals(mergedSignals);
          setSelected(prev => prev.filter(k => seenKeys.has(k)));
          setSearch('');
          setParams(prev => {
            const kept = prev.filter(sig => mergedSignals.includes(sig));
            return kept.length ? kept : defaultSignalsMerged(selectedLevels, mergedSignals);
          });
          if (!mergedSignals.includes('dc_current')) setNormalize(false);
        })
        .catch(() => {
          setPickerItems([]);
          setAvailableSignals([]);
          setSelected([]);
          setParams([]);
          setNormalize(false);
        })
        .finally(() => {
          setEquipLoading(false);
          setSignalLoading(false);
        });
    }, [selectedLevels, plantId]);

    const toggleParam = (param) => {
      setParams(prev => prev.includes(param) ? prev.filter(x => x !== param) : [...prev, param]);
    };

    const fetchData = async () => {
      if (!selected.length) return alert('Please select at least one equipment row from the list.');
      if (!params.length) return alert('Please select at least one parameter to plot.');
      const ids = [...new Set(selected.map(k => parsePickerKey(k).equipmentId))];
      setLoading(true);
      try {
        const resp = await window.SolarAPI.Analytics.timeseries(
          ids.join(','),
          params.join(','),
          plantId,
          dateFrom,
          dateTo,
          normalize,
          preferScbDcAggregate ? 'scb_aggregate' : 'raw',
          (selectedLevels.length === 1 ? selectedLevels[0] : 'inverter').toLowerCase()
        );
        setTsData(resp.data || []);
        setAvail(resp.availability_pct || 0);
      } catch (e) {
        alert('Failed to fetch data: ' + e.message);
      } finally {
        setLoading(false);
      }
    };

    const normalizeRef = useRef(normalize);
    useEffect(() => {
      if (normalizeRef.current === normalize) return;
      normalizeRef.current = normalize;
      if (selected.length && params.length && plantId) fetchData();
    }, [normalize]);

    const scbDcRef = useRef(preferScbDcAggregate);
    useEffect(() => {
      if (scbDcRef.current === preferScbDcAggregate) return;
      scbDcRef.current = preferScbDcAggregate;
      if (selected.length && params.length && plantId) fetchData();
    }, [preferScbDcAggregate]);

    // Rebuilding this on every render used to be the single biggest cost on
    // the Analytics Lab page — 10k-row tsData meant 10k Object.values() spread
    // + a fresh sort on every keystroke. Memoise on the tsData identity.
    const chartRows = useMemo(() => {
      const chartData = {};
      tsData.forEach(d => {
        const ts = d.timestamp != null ? String(d.timestamp) : '';
        if (!ts) return;
        if (!chartData[ts]) chartData[ts] = { timestamp: ts };
        chartData[ts][`${d.equipment_id}|${d.signal}`] = d.value;
      });
      return Object.values(chartData).sort((a, b) => {
        const ta = a.timestamp ? new Date(a.timestamp).getTime() : 0;
        const tb = b.timestamp ? new Date(b.timestamp).getTime() : 0;
        return ta - tb;
      });
    }, [tsData]);
    const topSigs = params.filter(sig => sig !== 'dc_voltage');
    const botSigs = params.includes('dc_voltage') ? ['dc_voltage'] : [];

    const noEquipmentMsg = selectedLevels.includes('wms')
      ? `Check the top date range covers days that exist in raw_data (see Metadata → Raw Data summary). WMS uses equipment_level wms or plant in PostgreSQL. After backend updates, restart the API. Plant id: ${plantId || '(none)'}.`
      : 'Upload raw time-series data and plant architecture to use the Analytics Lab.';

    // Short level hint added only when the ID would be ambiguous (so INV-/SCB-/STR- IDs don't get a redundant "(Inverter)" etc).
    const levelHint = (level, id) => {
      const s = String(id || '').toUpperCase();
      if (level === 'inverter' && /^INV/.test(s)) return '';
      if (level === 'scb' && /SCB/.test(s)) return '';
      if (level === 'string' && /STR/.test(s)) return '';
      if (level === 'wms') return '';
      const l = levelLabel(level);
      return l ? ` (${l})` : '';
    };
    const seriesLabels = selected.map(k => {
      const { level, equipmentId } = parsePickerKey(k);
      return {
        key: k,
        level,
        equipmentId,
        legendPrefix: `${equipmentId}${levelHint(level, equipmentId)}`,
      };
    });

    /** Parameters checkboxes: only signals that apply to at least one selected equipment row */
    const selectedPickerLevels = selected.map(k => parsePickerKey(k).level).filter(Boolean);
    const visibleSignalsForPicker = availableSignals.filter(sig =>
      selected.length === 0
        ? true
        : selectedPickerLevels.some(lvl => signalAllowedForHierarchy(sig, lvl))
    );

    // Memoise topOption so EChart does not receive a fresh object identity
    // on every keystroke. Rebuilt only when the data or selection changes.
    const topOption = useMemo(() => {
      if (!tsData.length || !topSigs.length) return null;
      const categories = chartRows.map(r => r.timestamp);
      const series = [];
      let idx = 0;
      let leftAxisNeedsMinZero = false;
      seriesLabels.forEach(({ equipmentId, legendPrefix, level }) => {
        topSigs.forEach(sig => {
          if (!signalAllowedForHierarchy(sig, level)) return;
          const dataKey = `${equipmentId}|${sig}`;
          const color = COLORS[idx % COLORS.length];
          idx += 1;
          const yAxisIndex = isMeteoAxisSignal(sig) ? 1 : 0;
          if (yAxisIndex === 0 && /current|power|status/i.test(sig)) leftAxisNeedsMinZero = true;
          const isIrrLike = /irradiance|gti|ghi/i.test(sig);
          series.push({
            name: `${legendPrefix} · ${signalLabelShort(sig)}`,
            type: 'line',
            smooth: true,
            showSymbol: false,
            data: chartRows.map(r => {
              const v = r[dataKey];
              if (v == null) return null;
              if (yAxisIndex === 1 && isIrrLike && Number(v) < 0) return 0;
              return v;
            }),
            connectNulls: false,
            emphasis: { focus: 'series' },
            lineStyle: { width: 1.8 },
            itemStyle: { color },
            yAxisIndex,
          });
        });
      });
      if (!series.length) return null;

      const hasLeftSeries = series.some(s => s.yAxisIndex === 0);
      let hasRightAxis = series.some(s => s.yAxisIndex === 1);
      if (!hasLeftSeries && hasRightAxis) {
        series.forEach(s => { s.yAxisIndex = 0; });
        hasRightAxis = false;
      }

      // Use a shared split count on both Y-axes so the gridlines line up horizontally
      // (ECharts' alignTicks alone can disagree when the two value ranges round to different "nice" steps).
      const SPLIT_N = 5;
      const leftAxis = {
        type: 'value',
        name: hasRightAxis ? 'Power / current / count' : (leftAxisNeedsMinZero ? 'Power / current / count' : 'Weather / irradiance'),
        nameLocation: 'middle',
        nameGap: 44,
        min: 0,
        alignTicks: true,
        splitNumber: SPLIT_N,
        axisLabel: { fontSize: 10 },
        splitLine: { lineStyle: { type: 'dashed', color: 'rgba(148,163,184,0.25)' } },
      };
      const yAxis = [leftAxis];
      if (hasRightAxis) {
        yAxis.push({
          type: 'value',
          name: 'Irradiance & weather (W/m², °C, m/s)',
          nameLocation: 'middle',
          nameGap: 52,
          position: 'right',
          min: 0,
          alignTicks: true,
          splitNumber: SPLIT_N,
          axisLabel: { fontSize: 10 },
          splitLine: { show: false },
          axisLine: { show: true, lineStyle: { color: '#94a3b8' } },
        });
      }

      // Dynamic top padding: the more series we have, the more rows the legend wraps into.
      // Rough estimate: ~170px per item, so ((items × 170) / chartWidth) rows; use a safe upper bound.
      const legendRows = Math.min(4, Math.max(1, Math.ceil(series.length / 4)));
      const chartTop = 30 + legendRows * 22;

      return {
        animationDuration: 600,
        toolbox: buildToolbox(),
        tooltip: { trigger: 'axis', axisPointer: { type: 'cross' }, formatter: buildTooltipFormatter() },
        legend: {
          type: 'scroll',
          top: 4,
          left: 'center',
          width: '78%',
          icon: 'roundRect',
          itemWidth: 14,
          itemHeight: 8,
          itemGap: 14,
          textStyle: { fontSize: 11, color: '#a8b8c8' },
          pageTextStyle: { fontSize: 10, color: '#71849a' },
        },
        grid: { left: 56, right: hasRightAxis ? 72 : 16, top: chartTop, bottom: 72 },
        xAxis: {
          type: 'category',
          data: categories,
          axisLabel: { fontSize: 10, formatter: (value) => formatTimestampLabel(value, true) },
        },
        yAxis,
        // Plotly-like independent axis stretching:
        //   • scroll over the plot area  → pans/zooms the X axis
        //   • scroll over the left Y axis area → stretches Power / current
        //   • scroll over the right Y axis area → stretches Irradiance / weather
        //   • Shift + scroll anywhere → stretches whatever Y axis the pointer is over
        // Each yAxis gets its own `inside` dataZoom so the two Y scales are decoupled
        // (exactly like dragging an axis in Plotly).
        dataZoom: [
          { type: 'inside', xAxisIndex: 0, zoomOnMouseWheel: true, moveOnMouseMove: true },
          { type: 'inside', yAxisIndex: 0, zoomOnMouseWheel: 'shift', moveOnMouseMove: 'shift', filterMode: 'none' },
          ...(hasRightAxis ? [{ type: 'inside', yAxisIndex: 1, zoomOnMouseWheel: 'shift', moveOnMouseMove: 'shift', filterMode: 'none' }] : []),
        ],
        series,
      };
    }, [chartRows, tsData.length, topSigs, seriesLabels]);

    const botOption = useMemo(() => {
      if (!tsData.length || !botSigs.length) return null;
      const categories = chartRows.map(r => r.timestamp);
      const series = [];
      let eqIndex = 0;
      seriesLabels.forEach(({ equipmentId, legendPrefix, level }) => {
        if (!signalAllowedForHierarchy('dc_voltage', level)) return;
        const key = `${equipmentId}|dc_voltage`;
        const color = COLORS[eqIndex % COLORS.length];
        eqIndex += 1;
        series.push({
          name: `${legendPrefix} · ${signalLabelShort('dc_voltage')}`,
          type: 'line',
          smooth: true,
          showSymbol: false,
          data: chartRows.map(r => r[key] ?? null),
          connectNulls: false,
          emphasis: { focus: 'series' },
          lineStyle: { width: 1.6 },
          itemStyle: { color },
        });
      });
      if (!series.length) return null;
      const legendRowsBot = Math.min(3, Math.max(1, Math.ceil(series.length / 4)));
      const chartTopBot = 30 + legendRowsBot * 22;
      return {
        animationDuration: 600,
        toolbox: buildToolbox(),
        tooltip: { trigger: 'axis', formatter: buildTooltipFormatter() },
        legend: {
          type: 'scroll',
          top: 4,
          left: 'center',
          width: '78%',
          icon: 'roundRect',
          itemWidth: 14,
          itemHeight: 8,
          itemGap: 14,
          textStyle: { fontSize: 11, color: '#a8b8c8' },
          pageTextStyle: { fontSize: 10, color: '#71849a' },
        },
        grid: { left: 48, right: 16, top: chartTopBot, bottom: 72 },
        xAxis: {
          type: 'category',
          data: categories,
          axisLabel: { fontSize: 10, formatter: (value) => formatTimestampLabel(value, true) },
        },
        yAxis: {
          type: 'value',
          splitNumber: 5,
          axisLabel: { fontSize: 10 },
          splitLine: { lineStyle: { type: 'dashed', color: 'rgba(148,163,184,0.25)' } },
        },
        dataZoom: [
          { type: 'inside', xAxisIndex: 0, zoomOnMouseWheel: true, moveOnMouseMove: true },
          { type: 'inside', yAxisIndex: 0, zoomOnMouseWheel: 'shift', moveOnMouseMove: 'shift', filterMode: 'none' },
        ],
        series,
      };
    }, [chartRows, tsData.length, botSigs, seriesLabels]);

    const singleScbId = (() => {
      const scbKeys = selected.filter(k => parsePickerKey(k).level === 'scb');
      return scbKeys.length === 1 ? parsePickerKey(scbKeys[0]).equipmentId : null;
    })();

    return h('div', null,
      !equipLoading && pickerItems.length === 0 && h('div', {
        style: {
          background: '#FFF7ED',
          border: '1px solid #FED7AA',
          borderRadius: 10,
          padding: '14px 18px',
          marginBottom: 16,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
        },
      },
        h('div', null,
          h('div', { style: { fontWeight: 700, fontSize: 13, color: '#92400E', marginBottom: 4 } }, 'No equipment data found in database'),
          h('div', { style: { fontSize: 12, color: '#B45309' } }, noEquipmentMsg),
        ),
        onNavigate && h('button', {
          className: 'btn btn-outline',
          onClick: () => onNavigate('Metadata'),
          style: { borderColor: '#F59E0B', color: '#B45309' },
        }, 'Go to Metadata'),
      ),

      h('div', { style: { display: 'grid', gridTemplateColumns: '168px 1fr 1fr', gap: 16, marginBottom: 16 } },
        h(Card, { title: 'Hierarchy levels' },
          h('div', { style: { fontSize: 11, color: 'var(--text-muted)', marginBottom: 8, lineHeight: 1.4 } },
            'Multiple Select'),
          h('div', { style: { display: 'flex', flexDirection: 'column', gap: 8 } },
            LEVELS.map(item => h('label', {
              key: item.id,
              style: {
                display: 'flex',
                gap: 8,
                alignItems: 'center',
                cursor: 'pointer',
                fontSize: 13,
                padding: '6px 8px',
                borderRadius: 6,
                background: selectedLevels.includes(item.id) ? 'rgba(14,165,233,0.08)' : 'transparent',
              },
            },
              h('input', {
                type: 'checkbox',
                checked: selectedLevels.includes(item.id),
                onChange: () => toggleLevel(item.id),
              }),
              h('span', {
                style: {
                  fontWeight: selectedLevels.includes(item.id) ? 700 : 400,
                  color: selectedLevels.includes(item.id) ? 'var(--accent)' : 'var(--text-primary)',
                },
              }, item.label.toUpperCase()),
            )),
          ),
        ),
        h(Card, { title: equipLoading ? 'Equipment - Loading...' : `Equipment (${selected.length} of ${pickerItems.length} selected)` },
          equipLoading
            ? h('div', { className: 'empty-state', style: { minHeight: 80 } }, h(Spinner), 'Loading...')
            : h(EquipmentPicker, {
                items: pickerItems,
                selected,
                search,
                onSearch: setSearch,
                onToggle: id => setSelected(prev => prev.includes(id) ? prev.filter(x => x !== id) : [...prev, id]),
                onSelectAll: (filteredIds, shouldSelect) => setSelected(prev => shouldSelect
                  ? [...new Set([...prev, ...filteredIds])]
                  : prev.filter(id => !filteredIds.includes(id))),
              }),
        ),
        h(Card, { title: 'Parameters to Plot', action: (visibleSignalsForPicker.includes('dc_current') || visibleSignalsForPicker.includes('dc_power') || visibleSignalsForPicker.includes('ac_power')) ? h(Toggle, { label: 'Normalize', title: 'dc_current as A/kWp | dc/ac power as % of DC capacity', value: normalize, onChange: setNormalize }) : null },
          h('div', { style: { display: 'flex', flexDirection: 'column', gap: 8, marginBottom: 14 } },
            signalLoading
              ? h('div', { className: 'empty-state', style: { minHeight: 80 } }, h(Spinner), 'Loading parameters...')
              : availableSignals.length === 0
                ? h('div', { className: 'empty-state', style: { minHeight: 80 } }, 'No parameters available for this hierarchy yet.')
                : visibleSignalsForPicker.length === 0
                  ? h('div', { style: { fontSize: 12, color: 'var(--text-muted)' } }, 'Select equipment rows first; parameters are filtered by hierarchy (WMS = weather only, Inverter = power/current, etc.).')
                  : visibleSignalsForPicker.map(sig => h('label', { key: sig, style: { display: 'flex', gap: 8, alignItems: 'center', cursor: 'pointer', fontSize: 13 } },
                      h('input', { type: 'checkbox', checked: params.includes(sig), onChange: () => toggleParam(sig) }),
                      signalLabel(sig),
                    )),
          ),
          selectedPickerLevels.includes('inverter') && (params.includes('dc_current') || params.includes('dc_power')) && h('div', { style: { marginBottom: 12, paddingTop: 10, borderTop: '1px solid var(--line-soft)' } },
            h(Toggle, { label: 'Inverter DC = sum of all SCB currents (roll-up)', value: preferScbDcAggregate, onChange: setPreferScbDcAggregate }),
            h('div', { style: { fontSize: 10, color: 'var(--text-muted)', marginTop: 6, lineHeight: 1.4 } },
              'Default: use inverter-level rows when present (single bus reading). Enabling this prefers Σ(SCB) per timestamp — often 10⁴–10⁵ A and not comparable to DC power on the same axis.'),
          ),
          h('button', { className: 'btn btn-primary', onClick: fetchData, style: { width: '100%', justifyContent: 'center' }, disabled: loading },
            loading ? h(Spinner) : 'Plot Charts',
          ),
        ),
      ),

      singleScbId && scbMetadata && h(Card, { title: 'Plant Architecture — ' + singleScbId, style: { marginBottom: 16 } },
        h('div', { style: { display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))', gap: 12, fontSize: 13 } },
          [
            ['number_of_strings', 'Number of Strings'],
            ['dc_capacity_kw', 'Total DC Capacity (kW)'],
            ['module_wp', 'Module Wp'],
            ['impp', 'Impp'],
            ['vmpp', 'Vmpp'],
            ['modules_per_string', 'Modules per String'],
          ].map(([param, label]) => {
            const v = scbMetadata[param];
            return h('div', { key: param }, h('span', { style: { color: 'var(--text-muted)', display: 'block', fontSize: 11 } }, label), h('span', { style: { fontWeight: 600 } }, v != null ? String(v) : '—'));
          })
        )
      ),

      tsData.length > 0 && h('div', { style: { display: 'flex', alignItems: 'center', gap: 12, marginBottom: 12, background: 'white', padding: '10px 16px', borderRadius: 10, border: '1px solid var(--border)' } },
        h('span', { style: { fontSize: 12, fontWeight: 700, color: 'var(--text-secondary)', whiteSpace: 'nowrap' } }, `Data Availability: ${avail}%`),
        h('div', { className: 'avail-bar-track', style: { flex: 1 } }, h('div', { className: 'avail-bar-fill', style: { width: `${avail}%` } })),
        h('span', { style: { fontSize: 11, color: 'var(--text-muted)', whiteSpace: 'nowrap' } }, `${tsData.length} data points`),
      ),

      tsData.length > 0 && topOption && topOption.series && topOption.series.length > 0 && h(Card, { title: 'Selected Signals', style: { marginBottom: 12 } },
        h(EChart, { option: topOption, style: { height: 360 } })
      ),

      tsData.length > 0 && botSigs.length > 0 && botOption && botOption.series && botOption.series.length > 0 && h(Card, { title: 'DC Voltage (V)' },
        h(EChart, { option: botOption, style: { height: 220 } })
      ),

      tsData.length === 0 && pickerItems.length > 0 && h('div', { className: 'empty-state', style: { minHeight: 260, background: 'white', borderRadius: 12, border: '1px solid var(--border)' } },
        h('span', null, 'Select equipment and parameters, then click "Plot Charts" (try WMS + SCB together for GTI vs current).'),
      ),
    );
  };
})();


