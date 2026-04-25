/**
 * js/perf_admin.js
 * =================
 * Admin Performance Dashboard — loaded on demand via the Admin page's Performance tab.
 * Shows real-time endpoint timings, slow queries, cache stats, DB health, and precompute controls.
 */

window.PerfAdminPanel = () => {
  const { useState, useEffect, useRef } = React;
  const h = React.createElement;
  const { Card, Badge, DataTable } = window;

  const [overview, setOverview] = useState(null);
  const [dbHealth, setDbHealth] = useState(null);
  const [endpointStats, setEndpointStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [precomputeStatus, setPrecomputeStatus] = useState(null);
  const [precomputeRunning, setPrecomputeRunning] = useState(false);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [pollFast, setPollFast] = useState(false);
  const intervalRef = useRef(null);

  const loadData = async () => {
    try {
      const [ov, db, ep] = await Promise.all([
        window.SolarAPI.Admin.perfOverview(),
        window.SolarAPI.Admin.perfDbHealth(),
        window.SolarAPI.Admin.perfEndpointStats(60),
      ]);
      setOverview(ov);
      setDbHealth(db);
      setEndpointStats(ep);
      setPrecomputeStatus(ov.precompute);
      const r = ov.precompute?.running || false;
      setPrecomputeRunning(r);
      setPollFast(r);
      setError(null);
    } catch (e) {
      setError(e.message || 'Failed to load performance data');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadData();
    return () => { if (intervalRef.current) clearInterval(intervalRef.current); };
  }, []);

  useEffect(() => {
    if (intervalRef.current) clearInterval(intervalRef.current);
    const ms = pollFast ? 1200 : (autoRefresh ? 15000 : 0);
    if (ms > 0) {
      intervalRef.current = setInterval(loadData, ms);
    }
    return () => { if (intervalRef.current) clearInterval(intervalRef.current); };
  }, [autoRefresh, pollFast]);

  const formatEta = (sec) => {
    if (sec == null || sec === '' || Number.isNaN(Number(sec))) return '—';
    const s = Math.max(0, Math.floor(Number(sec)));
    if (s < 60) return `${s}s`;
    const m = Math.floor(s / 60);
    const r = s % 60;
    return `${m}m ${r}s`;
  };

  const handlePrecompute = async () => {
    if (precomputeRunning) return;
    if (!confirm(
      'Run the FULL FAULT & ANALYTICS pipeline for all plants?\n\n' +
      'This computes module snapshots (DS summary, unified fault feed, loss bridge, category KPI rows) ' +
      'and runs all fault tab engines (power limitation, inverter shutdown, grid breakdown, communication, clipping/derating) ' +
      'for each plant’s raw-data date range. It may take several minutes.\n\n' +
      'It does NOT re-run disconnected-string detection on raw SCB time series (that runs on data ingest).'
    )) return;
    try {
      const r = await window.SolarAPI.Admin.runPrecompute();
      setPrecomputeRunning(true);
      setPollFast(true);
      setTimeout(loadData, 500);
    } catch (e) {
      alert('Failed: ' + (e.message || e));
    }
  };

  // ── KPI Card ──────────────────────────────────────────────────────────────
  const KPICard = ({ label, value, sub, color }) => {
    return h('div', {
      style: {
        background: 'var(--panel)', border: '1px solid var(--line)', borderRadius: 14,
        padding: '18px 20px', minWidth: 160, flex: '1 1 160px',
      }
    },
      h('div', { style: { fontSize: 11, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.3px', marginBottom: 6 } }, label),
      h('div', { style: { fontSize: 28, fontWeight: 800, color: color || 'var(--text)', lineHeight: 1.1 } }, value),
      sub && h('div', { style: { fontSize: 11, color: 'var(--text-muted)', marginTop: 4 } }, sub)
    );
  };

  // ── Cache gauge ───────────────────────────────────────────────────────────
  const CacheGauge = ({ hits, misses }) => {
    const total = (hits || 0) + (misses || 0);
    const rate = total > 0 ? Math.round((hits / total) * 100) : 0;
    const color = rate >= 70 ? '#22c55e' : rate >= 40 ? '#f59e0b' : '#ef4444';
    return h('div', { style: { textAlign: 'center' } },
      h('div', { style: { position: 'relative', width: 100, height: 100, margin: '0 auto 8px' } },
        h('svg', { viewBox: '0 0 36 36', style: { width: 100, height: 100, transform: 'rotate(-90deg)' } },
          h('circle', { cx: 18, cy: 18, r: 15.9, fill: 'none', stroke: 'var(--line)', strokeWidth: 2.5 }),
          h('circle', {
            cx: 18, cy: 18, r: 15.9, fill: 'none', stroke: color, strokeWidth: 2.5,
            strokeDasharray: `${rate} ${100 - rate}`, strokeLinecap: 'round'
          })
        ),
        h('div', { style: { position: 'absolute', top: '50%', left: '50%', transform: 'translate(-50%,-50%)', fontSize: 20, fontWeight: 800, color } }, rate + '%')
      ),
      h('div', { style: { fontSize: 11, color: 'var(--text-muted)' } }, `${hits || 0} hits / ${misses || 0} misses`)
    );
  };

  if (loading) {
    return h('div', { style: { padding: 40, textAlign: 'center', color: 'var(--text-muted)' } },
      h('div', { className: 'loading-spinner', style: { margin: '0 auto 12px', width: 24, height: 24 } }),
      'Loading performance data…'
    );
  }

  if (error) {
    return h(Card, { title: 'Performance Monitor' },
      h('div', { style: { color: '#ef4444', padding: 20 } }, '⚠ ' + error),
      h('button', { className: 'btn btn-outline', onClick: () => { setLoading(true); loadData(); } }, 'Retry')
    );
  }

  const cache = overview?.cache || {};
  const precomp = precomputeStatus || {};

  // Endpoint stats table columns
  const epColumns = [
    { key: 'path', label: 'Endpoint', render: r => h('code', { style: { fontSize: 11 } }, r.path), csvValue: r => r.path },
    { key: 'count', label: 'Calls', render: r => r.count, sortValue: r => r.count, csvValue: r => r.count },
    { key: 'avg_ms', label: 'Avg (ms)', render: r => {
      const c = r.avg_ms > 5000 ? '#ef4444' : r.avg_ms > 2000 ? '#f59e0b' : '#22c55e';
      return h('span', { style: { fontWeight: 700, color: c } }, Math.round(r.avg_ms));
    }, sortValue: r => r.avg_ms, csvValue: r => Math.round(r.avg_ms) },
    { key: 'p95_ms', label: 'P95 (ms)', render: r => Math.round(r.p95_ms), sortValue: r => r.p95_ms, csvValue: r => Math.round(r.p95_ms) },
    { key: 'max_ms', label: 'Max (ms)', render: r => {
      const c = r.max_ms > 10000 ? '#ef4444' : r.max_ms > 5000 ? '#f59e0b' : 'var(--text)';
      return h('span', { style: { color: c, fontWeight: 600 } }, Math.round(r.max_ms));
    }, sortValue: r => r.max_ms, csvValue: r => Math.round(r.max_ms) },
  ];

  // DB tables columns
  const dbTableCols = [
    { key: 'name', label: 'Table', render: r => h('code', { style: { fontSize: 11 } }, r.name), csvValue: r => r.name },
    { key: 'size', label: 'Size', render: r => r.size, csvValue: r => r.size },
    { key: 'rows', label: 'Est. Rows', render: r => (r.rows || 0).toLocaleString(), sortValue: r => r.rows || 0, csvValue: r => r.rows },
  ];

  // DB index columns
  const dbIdxCols = [
    { key: 'table', label: 'Table', render: r => r.table, csvValue: r => r.table },
    { key: 'index', label: 'Index Name', render: r => h('code', { style: { fontSize: 10 } }, r.index), csvValue: r => r.index },
    { key: 'scans', label: 'Scans', render: r => (r.scans || 0).toLocaleString(), sortValue: r => r.scans || 0, csvValue: r => r.scans },
    { key: 'size', label: 'Size', render: r => r.size, csvValue: r => r.size },
  ];

  return h('div', { style: { display: 'flex', flexDirection: 'column', gap: 16 } },
    // Header
    h('div', { style: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 10 } },
      h('div', null,
        h('h3', { style: { margin: 0 } }, '⚡ Performance Monitor'),
        h('p', { style: { fontSize: 12, color: 'var(--text-muted)', margin: '4px 0 0' } },
          'Real-time API performance, database health, and cache efficiency')
      ),
      h('div', { style: { display: 'flex', gap: 8, alignItems: 'center' } },
        h('label', { style: { fontSize: 11, display: 'flex', alignItems: 'center', gap: 4, color: 'var(--text-muted)' } },
          h('input', { type: 'checkbox', checked: autoRefresh, onChange: e => setAutoRefresh(e.target.checked) }),
          'Auto-refresh 15s'
        ),
        h('button', { className: 'btn btn-outline', style: { padding: '4px 10px', fontSize: 11 }, onClick: () => { setLoading(true); loadData(); } }, '↻ Refresh')
      )
    ),

    // KPI row
    h('div', { style: { display: 'flex', gap: 12, flexWrap: 'wrap' } },
      h(KPICard, {
        label: 'Requests Tracked',
        value: overview?.total_requests_tracked || 0,
        sub: 'Recent in-memory buffer'
      }),
      h(KPICard, {
        label: 'Slow Requests (>3s)',
        value: overview?.slow_requests_gt3s || 0,
        color: (overview?.slow_requests_gt3s || 0) > 5 ? '#ef4444' : '#22c55e',
        sub: 'Target: 0'
      }),
      h(KPICard, {
        label: 'DB Connections',
        value: dbHealth?.connections?.active || '?',
        sub: `${dbHealth?.connections?.idle || 0} idle / ${dbHealth?.connections?.total || 0} total`,
        color: (dbHealth?.connections?.active || 0) > 10 ? '#f59e0b' : 'var(--text)'
      }),
      h(KPICard, {
        label: 'Cache Store Size',
        value: cache.store_size || 0,
        sub: 'In-memory entries'
      })
    ),

    // Cache gauge + Precompute
    h('div', { style: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: 12 } },
      h(Card, { title: 'Cache Hit Rate' },
        h(CacheGauge, { hits: cache.hits, misses: cache.misses })
      ),
      h(Card, { title: 'Full fault & snapshot pipeline' },
        h('div', { style: { display: 'flex', flexDirection: 'column', gap: 10 } },
          h('div', { style: { display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' } },
            h(Badge, { type: precomp.running ? 'amber' : 'green' }, precomp.running ? 'Running' : 'Idle'),
            precomp.mode && precomp.mode !== 'idle' && h('span', { style: { fontSize: 11, color: 'var(--text-muted)' } },
              'Mode: ' + String(precomp.mode)),
          ),
          h('div', { style: { width: '100%', height: 10, background: 'var(--line)', borderRadius: 5, overflow: 'hidden' } },
            h('div', {
              style: {
                width: (Math.min(100, precomp.percent != null ? Number(precomp.percent) : 0)) + '%',
                height: '100%',
                background: precomp.running ? '#0ea5e9' : '#22c55e',
                borderRadius: 5,
                transition: 'width 0.3s ease',
              },
            }),
          ),
          h('div', { style: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(120px, 1fr))', gap: 6, fontSize: 11, color: 'var(--text-muted)' } },
            h('div', null, h('strong', { style: { color: 'var(--text)' } }, 'Progress: '), (precomp.percent != null ? precomp.percent : '0') + ' %'),
            h('div', null, h('strong', { style: { color: 'var(--text)' } }, 'Plants: '), (precomp.plants_done != null ? precomp.plants_done : 0) + ' / ' + (precomp.plants_total != null ? precomp.plants_total : 0)),
            h('div', { style: { gridColumn: 'span 2' } }, h('strong', { style: { color: 'var(--text)' } }, 'Current: '), (precomp.current_plant || '—') + (precomp.step ? ' · ' + precomp.step : '')),
            h('div', null, h('strong', { style: { color: 'var(--text)' } }, 'Elapsed: '), (precomp.elapsed_seconds != null ? precomp.elapsed_seconds + ' s' : '—')),
            h('div', null, h('strong', { style: { color: 'var(--text)' } }, 'Est. remaining: '), formatEta(precomp.eta_seconds)),
            precomp.started_at && h('div', { style: { gridColumn: '1 / -1' } },
              h('strong', { style: { color: 'var(--text)' } }, 'Started: '),
              new Date(precomp.started_at).toLocaleString()
            ),
          ),
          (precomp.event_log && precomp.event_log.length > 0) && h('div', {
            style: {
              maxHeight: 200,
              overflow: 'auto',
              fontSize: 10,
              fontFamily: 'ui-monospace, Consolas, monospace',
              background: 'rgba(0,0,0,0.15)',
              border: '1px solid var(--line)',
              borderRadius: 8,
              padding: 8,
            },
          },
            precomp.event_log.slice(-80).map((e, idx) => h('div', { key: idx, style: { marginBottom: 4, color: 'var(--text)' } },
              h('span', { style: { color: 'var(--text-muted)' } }, (e.t || '').replace('T', ' ').slice(0, 19), '  '),
              e.message
            ))
          ),
          precomp.last_run && h('div', { style: { fontSize: 11, color: 'var(--text-muted)' } },
            `Last run: ${new Date(precomp.last_run).toLocaleString()} (${precomp.last_duration_s != null ? precomp.last_duration_s : '?'}s)`),
          precomp.last_error && h('div', { style: { fontSize: 11, color: '#ef4444' } }, 'Error: ' + precomp.last_error),
          h('button', {
            className: 'btn btn-primary',
            style: { padding: '6px 12px', fontSize: 12 },
            disabled: precomp.running,
            onClick: handlePrecompute
          }, precomp.running ? 'Running full pipeline…' : '▶ Run full fault & snapshot pipeline'),
          h('p', { style: { fontSize: 10, color: 'var(--text-muted)', margin: 0, lineHeight: 1.45 } },
            'Module snapshots (DS, unified, loss) + five fault tab engines per plant, same date range as precompute/ingest. Progress updates every ~1.2s while running. See docs/PERFORMANCE_AND_FAULT_PIPELINE_AUDIT.md for detail.')
        )
      )
    ),

    // Endpoint stats table
    h(Card, { title: 'Endpoint Response Times (Last 60 min)' },
      endpointStats?.endpoints?.length > 0
        ? h(DataTable, {
            columns: epColumns,
            rows: endpointStats.endpoints,
            maxHeight: 400,
            initialSortKey: 'avg_ms',
            initialSortDir: 'desc',
            compact: true,
            filename: 'endpoint_stats.csv',
          })
        : h('p', { style: { color: 'var(--text-muted)', fontSize: 12, padding: 20 } }, 'No endpoint data yet — make some API calls first.')
    ),

    // DB Health — Tables
    h('div', { style: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(400px, 1fr))', gap: 12 } },
      h(Card, { title: 'Database Tables' },
        Array.isArray(dbHealth?.tables)
          ? h(DataTable, {
              columns: dbTableCols,
              rows: dbHealth.tables,
              maxHeight: 300,
              initialSortKey: 'rows',
              initialSortDir: 'desc',
              compact: true,
              filename: 'db_tables.csv',
            })
          : h('p', { style: { color: 'var(--text-muted)', fontSize: 12 } }, 'No table data')
      ),
      h(Card, { title: 'Index Usage Stats' },
        Array.isArray(dbHealth?.indexes)
          ? h(DataTable, {
              columns: dbIdxCols,
              rows: dbHealth.indexes,
              maxHeight: 300,
              initialSortKey: 'scans',
              initialSortDir: 'desc',
              compact: true,
              filename: 'db_indexes.csv',
            })
          : h('p', { style: { color: 'var(--text-muted)', fontSize: 12 } }, 'No index data')
      )
    ),

    // Server time
    h('div', { style: { fontSize: 10, color: 'var(--text-muted)', textAlign: 'right', paddingTop: 4 } },
      'Server: ' + (overview?.server_time ? new Date(overview.server_time).toLocaleString() : '—'))
  );
};
