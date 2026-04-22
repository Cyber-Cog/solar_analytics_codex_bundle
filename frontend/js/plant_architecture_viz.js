// frontend/js/plant_architecture_viz.js
// Animated, expandable plant-architecture visualization.
// Layout: responsive grid of compact inverter tiles. Each tile expands in-place
// to reveal its SCBs; an SCB expands to reveal per-string panels with flowing
// current animation. Exposes window.PlantArchitectureViz({ rows, plantId }).

(() => {
  const h = React.createElement;

  // ─── Styles (injected once) ────────────────────────────────────────────────
  (function injectStyles() {
    if (document.getElementById('plant-arch-viz-styles')) return;
    const css = `
      @keyframes pav-flow {
        from { stroke-dashoffset: 0; }
        to   { stroke-dashoffset: -180; }
      }
      @keyframes pav-sun {
        0%, 100% { transform: scale(1); filter: drop-shadow(0 0 10px rgba(255, 196, 80, 0.5)); }
        50%      { transform: scale(1.06); filter: drop-shadow(0 0 20px rgba(255, 196, 80, 0.9)); }
      }
      @keyframes pav-shine {
        0%   { transform: translateX(-120%); }
        55%  { transform: translateX(180%); }
        100% { transform: translateX(180%); }
      }
      @keyframes pav-fade {
        from { opacity: 0; transform: translateY(3px); }
        to   { opacity: 1; transform: translateY(0); }
      }
      @keyframes pav-pulse-border {
        0%, 100% { box-shadow: 0 0 0 0 rgba(62,183,223,0); }
        50%      { box-shadow: 0 0 0 3px rgba(62,183,223,0.16); }
      }
      @keyframes pav-bar-pulse {
        0%   { filter: brightness(0.85); }
        50%  { filter: brightness(1.25); }
        100% { filter: brightness(0.85); }
      }
      @keyframes pav-electron {
        0%   { transform: translateY(100%); opacity: 0; }
        15%  { opacity: 1; }
        80%  { opacity: 1; }
        100% { transform: translateY(-120%); opacity: 0; }
      }
      @keyframes pav-ring {
        0%   { transform: scale(0.8); opacity: 0.7; }
        100% { transform: scale(1.6); opacity: 0; }
      }

      .pav-wrap { position: relative; }
      .pav-controls {
        display:flex; gap:10px; align-items:center; flex-wrap:wrap;
        margin-bottom:12px;
      }
      .pav-chip {
        display:inline-flex; align-items:center; gap:6px;
        padding:4px 10px; border-radius:999px;
        background: rgba(255,255,255,0.04); border:1px solid var(--line);
        color:var(--text-soft); font-size:11px; font-weight:700; white-space:nowrap;
      }
      .pav-chip strong { color: var(--text); font-weight: 800; }

      .pav-seg {
        display:inline-flex; gap:4px;
        background: rgba(255,255,255,0.04); border:1px solid var(--line);
        border-radius: 10px; padding: 3px;
      }
      .pav-seg-btn {
        border: none; background: transparent; color: var(--text-soft);
        font: inherit; font-size: 11px; font-weight: 700;
        padding: 5px 10px; border-radius: 7px; cursor: pointer;
      }
      .pav-seg-btn.active { background: var(--accent-soft); color: var(--accent); }

      .pav-legend {
        display:flex; gap:10px; align-items:center; flex-wrap:wrap;
        font-size:11px; color:var(--text-muted); margin-left:auto;
      }
      .pav-legend-dot {
        width:10px; height:10px; border-radius:3px;
        display:inline-block; margin-right:6px; vertical-align:middle;
      }

      .pav-canvas {
        position:relative; width:100%;
        border:1px solid var(--line); border-radius:14px;
        background:
          radial-gradient(circle at 15% 5%, rgba(62,183,223,0.10), transparent 36%),
          radial-gradient(circle at 85% 95%, rgba(255, 196, 80, 0.07), transparent 36%),
          var(--panel);
        padding:18px 16px 20px;
      }

      /* Plant node */
      .pav-plant {
        position: relative;
        display:flex; align-items:center; gap:10px;
        padding:8px 14px; width:max-content; margin:0 auto 10px;
        background: linear-gradient(135deg, rgba(255,196,80,0.18), rgba(62,183,223,0.12));
        border:1px solid rgba(255,196,80,0.35);
        border-radius:12px;
        box-shadow: 0 6px 24px rgba(255,196,80,0.18);
      }
      .pav-plant::after {
        content:''; position:absolute; inset:-6px;
        border-radius: 16px; border:1px solid rgba(255,196,80,0.45);
        animation: pav-ring 2.6s ease-out infinite;
        pointer-events:none;
      }
      .pav-sun {
        width:30px; height:30px; border-radius:50%;
        background: radial-gradient(circle at 30% 30%, #fde68a, #f59e0b 60%, #b45309);
        animation: pav-sun 3.2s ease-in-out infinite;
        flex:0 0 auto;
      }
      .pav-plant-name { font-weight:800; color:var(--text); font-size:13px; letter-spacing:-0.01em; }
      .pav-plant-sub { color:var(--text-soft); font-size:11px; }

      /* Grid of inverters */
      .pav-inv-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
        gap: 10px;
        margin-top: 8px;
      }
      .pav-inv-grid.expanded-row > .pav-inv.expanded { grid-column: 1 / -1; }

      .pav-inv {
        position: relative;
        border:1px solid var(--line); border-radius:12px;
        background: rgba(16, 28, 43, 0.68);
        padding:10px 11px;
        display:flex; flex-direction:column; gap:8px;
        transition: border-color .16s ease, transform .16s ease, box-shadow .16s ease;
        animation: pav-fade .3s ease-out both;
        cursor: pointer;
      }
      .pav-inv:hover { border-color: rgba(62,183,223,0.45); transform: translateY(-1px); }
      .pav-inv.expanded {
        background: rgba(16, 28, 43, 0.92);
        border-color: rgba(62,183,223,0.55);
        animation: pav-pulse-border 3s ease-in-out infinite;
        cursor: default;
      }

      .pav-inv-head {
        display:flex; align-items:center; justify-content:space-between; gap:8px;
      }
      .pav-inv-title {
        display:flex; align-items:center; gap:7px;
        font-weight:800; color:var(--text); font-size:12.5px;
        min-width: 0;
      }
      .pav-inv-title span.name {
        overflow:hidden; text-overflow:ellipsis; white-space:nowrap;
      }
      .pav-inv-icon {
        width:24px; height:24px; border-radius:7px;
        display:inline-flex; align-items:center; justify-content:center;
        background: linear-gradient(135deg, rgba(62,183,223,0.32), rgba(62,183,223,0.08));
        color:#9ee6fb; flex:0 0 auto;
      }

      .pav-inv-stats {
        display:grid; grid-template-columns: 1fr 1fr; gap:3px 10px;
        font-size:10.5px; color:var(--text-soft);
      }
      .pav-inv-stats strong { color:var(--text); font-weight:700; }

      .pav-util-bar {
        position: relative; height: 4px; border-radius: 999px;
        background: rgba(255,255,255,0.06); overflow: hidden;
      }
      .pav-util-fill {
        position: absolute; left: 0; top: 0; bottom: 0;
        background: linear-gradient(90deg, #3eb7df, #34c889);
        border-radius: inherit;
        animation: pav-bar-pulse 2.4s ease-in-out infinite;
      }
      .pav-util-fill::after {
        content:''; position:absolute; inset:0;
        background: linear-gradient(90deg, transparent 30%, rgba(255,255,255,0.35) 50%, transparent 70%);
        animation: pav-shine 2.6s linear infinite;
      }

      .pav-expand-hint {
        margin-top: 2px; font-size: 10px; color: var(--text-muted); text-align: right;
        font-weight: 700; letter-spacing: 0.02em;
      }
      .pav-inv.expanded .pav-expand-hint { color: var(--accent); }

      /* SCB grid (inside expanded inverter) */
      .pav-scb-panel {
        margin-top: 6px; padding-top: 10px;
        border-top: 1px dashed var(--line-soft);
        animation: pav-fade .25s ease-out both;
      }
      .pav-scb-grid {
        display:grid;
        grid-template-columns: repeat(auto-fill, minmax(54px, 1fr));
        gap:5px;
      }
      .pav-scb {
        position:relative; cursor:pointer;
        padding:5px 4px 6px; border-radius:7px;
        border:1px solid var(--line-soft);
        background: rgba(255,255,255,0.025);
        color:var(--text-soft); font-size:10px; font-weight:700;
        text-align:center; line-height:1.1; overflow:hidden;
        transition: background .15s ease, border-color .15s ease, color .15s ease, transform .15s ease;
      }
      .pav-scb:hover { border-color: rgba(62,183,223,0.5); color:var(--text); transform:scale(1.04); }
      .pav-scb.spare { background: rgba(228,161,70,0.10); border-color: rgba(228,161,70,0.36); color:#efc17c; }
      .pav-scb.selected { background: rgba(62,183,223,0.18); border-color: rgba(62,183,223,0.65); color:#9ee6fb; }
      .pav-scb-count {
        display:block; font-size:9px; color:var(--text-muted);
        margin-top:2px; font-weight:600;
      }
      .pav-scb-bar {
        position:absolute; left:0; right:0; bottom:0; height:2px;
        background: linear-gradient(90deg, #3eb7df, #34c889);
        opacity: 0.85;
      }
      .pav-scb.spare .pav-scb-bar { background: linear-gradient(90deg, #e4a146, #b45309); }

      /* Strings (inside selected SCB) */
      .pav-str-panel {
        margin-top: 10px; padding: 10px;
        border: 1px solid var(--line); border-radius: 10px;
        background: rgba(62,183,223,0.04);
        animation: pav-fade .25s ease-out both;
      }
      .pav-str-head {
        display:flex; justify-content:space-between; align-items:baseline; gap:8px;
        margin-bottom: 8px; flex-wrap: wrap;
      }
      .pav-str-title { font-weight:800; color:var(--text); font-size:12.5px; }
      .pav-str-sub { color:var(--text-soft); font-size:11px; }

      .pav-str-grid {
        display:grid;
        grid-template-columns: repeat(auto-fill, minmax(32px, 1fr));
        gap:5px;
      }
      .pav-string {
        position:relative; height:54px; border-radius:5px;
        background: linear-gradient(180deg, rgba(18, 30, 48, 0.9), rgba(10, 18, 30, 0.95));
        border:1px solid rgba(62,183,223,0.22);
        overflow:hidden;
        box-shadow: inset 0 0 0 1px rgba(255,255,255,0.04);
      }
      /* Module grid inside each string (solar panel look) */
      .pav-string::before {
        content:''; position:absolute; inset:2px;
        background:
          linear-gradient(rgba(62,183,223,0.12) 1px, transparent 1px) 0 0 / 100% 25%,
          linear-gradient(90deg, rgba(62,183,223,0.12) 1px, transparent 1px) 0 0 / 50% 100%,
          linear-gradient(155deg, rgba(76,170,220,0.35), rgba(20,60,100,0.5));
        border-radius: 3px;
      }
      /* Light sweep over panels */
      .pav-string::after {
        content:''; position:absolute; inset:0;
        background: linear-gradient(110deg, transparent 28%, rgba(255,255,255,0.18) 50%, transparent 72%);
        transform: translateX(-120%);
        animation: pav-shine 3.8s ease-in-out infinite;
      }
      .pav-string .pav-el {
        position:absolute; left:50%; top:0; transform: translate(-50%, 100%);
        width: 6px; height: 6px; border-radius: 50%;
        background: radial-gradient(circle, #ffe8a8 0%, #f59e0b 60%, rgba(245,158,11,0) 100%);
        box-shadow: 0 0 8px rgba(253, 211, 105, 0.9);
        animation: pav-electron 2s linear infinite;
      }
      .pav-string .pav-label {
        position:absolute; left:0; right:0; bottom:2px;
        text-align:center; font-size:8.5px; font-weight:700;
        color: rgba(255,255,255,0.85); letter-spacing: 0.02em;
        text-shadow: 0 0 4px rgba(0,0,0,0.8);
      }

      /* SVG flow rail from plant to inverters */
      .pav-flow-line {
        fill:none;
        stroke: rgba(62,183,223,0.45);
        stroke-width: 1.3;
        stroke-dasharray: 6 9;
        animation: pav-flow 4.5s linear infinite;
      }

      /* Light theme tweaks */
      body.theme-light .pav-canvas { background: #fff; }
      body.theme-light .pav-inv { background: #fff; }
      body.theme-light .pav-inv.expanded { background: #f8fafc; }
      body.theme-light .pav-scb { background: #f8fafc; color:#334155; }
      body.theme-light .pav-str-panel { background: #f0f9ff; }
      body.theme-light .pav-string {
        background: linear-gradient(180deg, #e0f2fe, #bae6fd);
        border-color: rgba(2, 132, 199, 0.3);
      }
      body.theme-light .pav-string::before {
        background:
          linear-gradient(rgba(2,132,199,0.18) 1px, transparent 1px) 0 0 / 100% 25%,
          linear-gradient(90deg, rgba(2,132,199,0.18) 1px, transparent 1px) 0 0 / 50% 100%,
          linear-gradient(155deg, rgba(56, 189, 248, 0.35), rgba(2, 132, 199, 0.25));
      }
      body.theme-light .pav-string .pav-label { color:#0c4a6e; text-shadow:none; }
    `;
    const style = document.createElement('style');
    style.id = 'plant-arch-viz-styles';
    style.textContent = css;
    document.head.appendChild(style);
  })();

  // ─── Helpers ───────────────────────────────────────────────────────────────
  const num = (v, d = 0) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : d;
  };

  function groupArchitecture(rows, { hideSpares } = {}) {
    const inverters = new Map();
    for (const r of rows || []) {
      if (hideSpares && r.spare_flag) continue; // drop spare rows entirely
      const invId = r.inverter_id || '(none)';
      if (!inverters.has(invId)) {
        inverters.set(invId, { inverter_id: invId, scbs: new Map() });
      }
      const inv = inverters.get(invId);
      const scbId = r.scb_id || '(none)';
      if (!inv.scbs.has(scbId)) {
        inv.scbs.set(scbId, {
          scb_id: scbId,
          strings: [],
          strings_per_scb: num(r.strings_per_scb, 0),
          modules_per_string: num(r.modules_per_string, 0),
          dc_capacity_kw: 0,
          spare: !!r.spare_flag,
        });
      }
      const scb = inv.scbs.get(scbId);
      scb.dc_capacity_kw = Math.max(scb.dc_capacity_kw, num(r.dc_capacity_kw, 0));
      if (r.string_id) scb.strings.push(r.string_id);
      if (r.spare_flag) scb.spare = true;
    }
    const list = [];
    inverters.forEach((inv) => {
      const scbs = Array.from(inv.scbs.values())
        .sort((a, b) => String(a.scb_id).localeCompare(String(b.scb_id), undefined, { numeric: true }));
      let totalDc = 0, totalStrings = 0, spareScbs = 0;
      for (const s of scbs) {
        totalDc += s.dc_capacity_kw;
        totalStrings += s.strings_per_scb || s.strings.length;
        if (s.spare) spareScbs += 1;
      }
      list.push({
        inverter_id: inv.inverter_id,
        scbs,
        total_scbs: scbs.length,
        spare_scbs: spareScbs,
        total_strings: totalStrings,
        total_dc_kwp: totalDc,
      });
    });
    list.sort((a, b) => String(a.inverter_id).localeCompare(String(b.inverter_id), undefined, { numeric: true }));
    return list;
  }

  // ─── Static icons ──────────────────────────────────────────────────────────
  const IconBolt = h('svg', {
    width: 14, height: 14, viewBox: '0 0 24 24', fill: 'none',
    stroke: 'currentColor', strokeWidth: 2, strokeLinecap: 'round', strokeLinejoin: 'round',
    'aria-hidden': true,
  }, h('polygon', { points: '13 2 3 14 12 14 11 22 21 10 12 10 13 2' }));

  const IconPlant = h('svg', {
    width: 16, height: 16, viewBox: '0 0 24 24', fill: 'none',
    stroke: 'currentColor', strokeWidth: 2, strokeLinecap: 'round', strokeLinejoin: 'round',
    'aria-hidden': true,
  },
    h('path', { d: 'M12 2v20' }),
    h('path', { d: 'M5 10c0-3 3-5 7-5s7 2 7 5' }),
    h('path', { d: 'M3 20h18' }),
  );

  // ─── Sub-components ────────────────────────────────────────────────────────
  function FlowBackdrop() {
    // Decorative connector field behind the inverter grid
    return h('svg', {
      viewBox: '0 0 1000 50', preserveAspectRatio: 'none',
      style: { width: '100%', height: 36, display: 'block', marginBottom: 2, pointerEvents: 'none' },
      'aria-hidden': true,
    },
      h('path', { className: 'pav-flow-line', d: 'M 500,2 C 500,18 200,22 60,48' }),
      h('path', { className: 'pav-flow-line', d: 'M 500,2 C 500,18 350,22 260,48' }),
      h('path', { className: 'pav-flow-line', d: 'M 500,2 C 500,22 500,22 500,48' }),
      h('path', { className: 'pav-flow-line', d: 'M 500,2 C 500,18 650,22 740,48' }),
      h('path', { className: 'pav-flow-line', d: 'M 500,2 C 500,18 800,22 940,48' }),
    );
  }

  function StringCell({ id, idx, delayMs }) {
    // A single string visualization — a multi-cell panel with a travelling electron dot.
    return h('div', {
      className: 'pav-string',
      title: `String ${id || idx + 1}`,
    },
      h('span', {
        className: 'pav-el',
        style: { animationDelay: `${delayMs || 0}ms` },
      }),
      h('div', { className: 'pav-label' }, id || `S${idx + 1}`),
    );
  }

  function StringsPanel({ inv, scb, cap, onClose }) {
    const stringCount = scb.strings_per_scb || scb.strings.length || 0;
    const shown = (scb.strings.length ? scb.strings : Array.from({ length: stringCount }, (_, i) => `S${i + 1}`)).slice(0, cap);
    const extra = stringCount - shown.length;
    return h('div', { className: 'pav-str-panel' },
      h('div', { className: 'pav-str-head' },
        h('div', null,
          h('div', { className: 'pav-str-title' }, `${inv.inverter_id} · ${scb.scb_id}`),
          h('div', { className: 'pav-str-sub' },
            `${stringCount} strings`,
            scb.modules_per_string ? ` · ${scb.modules_per_string} modules/string` : '',
            scb.dc_capacity_kw ? ` · ${scb.dc_capacity_kw.toFixed(1)} kW DC` : '',
            scb.spare ? ' · SPARE' : '',
          ),
        ),
        h('button', {
          type: 'button',
          className: 'btn btn-outline',
          style: { padding: '4px 10px', fontSize: 11 },
          onClick: onClose,
        }, 'Close'),
      ),
      h('div', { className: 'pav-str-grid' },
        shown.map((id, i) => h(StringCell, {
          key: id || i,
          id,
          idx: i,
          delayMs: (i % 8) * 220,
        })),
        extra > 0 ? h('div', {
          className: 'pav-string',
          style: { display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 10, fontWeight: 700, color: 'var(--text-soft)' },
          title: `+${extra} more strings (first ${cap} shown for performance)`,
        }, `+${extra}`) : null,
      ),
    );
  }

  function InverterTile({ inv, expanded, selectedScbId, onToggle, onSelectScb }) {
    const stringCap = 96;
    const scb = selectedScbId ? inv.scbs.find((s) => s.scb_id === selectedScbId) : null;

    // Simple "utilization" bar: strings relative to biggest inverter (just visual flair).
    const util = Math.min(100, Math.max(4, (inv.total_strings / Math.max(1, inv._maxStrings)) * 100));

    return h('div', {
      className: `pav-inv ${expanded ? 'expanded' : ''}`,
      onClick: (e) => {
        if (expanded) return;
        onToggle(inv.inverter_id);
      },
      role: 'button', tabIndex: 0,
      'aria-expanded': expanded,
    },
      h('div', { className: 'pav-inv-head' },
        h('div', { className: 'pav-inv-title' },
          h('span', { className: 'pav-inv-icon' }, IconBolt),
          h('span', { className: 'name', title: inv.inverter_id }, inv.inverter_id),
        ),
        h('span', { className: 'pav-chip' }, h('strong', null, inv.total_scbs), ' SCB'),
      ),
      h('div', { className: 'pav-inv-stats' },
        h('span', null, 'DC ', h('strong', null, inv.total_dc_kwp ? `${inv.total_dc_kwp.toFixed(1)} kWp` : '—')),
        h('span', null, 'Strings ', h('strong', null, inv.total_strings || '—')),
        h('span', null, 'Spares ', h('strong', null, inv.spare_scbs)),
        h('span', null, 'Modules ', h('strong', null, inv._modulesTotal || '—')),
      ),
      h('div', { className: 'pav-util-bar' }, h('div', { className: 'pav-util-fill', style: { width: `${util}%` } })),
      h('div', { className: 'pav-expand-hint' }, expanded ? 'Expanded — click SCB or Close' : 'Click to expand'),

      // Expanded sub-panel (SCBs + strings)
      expanded && h('div', {
        className: 'pav-scb-panel',
        onClick: (e) => e.stopPropagation(),
      },
        h('div', { style: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6, gap: 6 } },
          h('span', { style: { fontSize: 11, color: 'var(--text-soft)', fontWeight: 700 } }, 'Solar Combiner Boxes'),
          h('button', {
            type: 'button',
            className: 'btn btn-outline',
            style: { padding: '3px 9px', fontSize: 10.5 },
            onClick: () => onToggle(inv.inverter_id),
          }, 'Close'),
        ),
        h('div', { className: 'pav-scb-grid' },
          inv.scbs.map((s) => h('div', {
            key: s.scb_id,
            className: `pav-scb ${s.spare ? 'spare' : ''} ${selectedScbId === s.scb_id ? 'selected' : ''}`,
            title: [
              `SCB: ${s.scb_id}`,
              `Strings: ${s.strings_per_scb || s.strings.length}`,
              s.dc_capacity_kw ? `DC: ${s.dc_capacity_kw.toFixed(1)} kW` : null,
              s.spare ? 'Spare SCB' : null,
            ].filter(Boolean).join('\n'),
            onClick: () => onSelectScb(inv.inverter_id, s.scb_id),
          },
            s.scb_id,
            h('span', { className: 'pav-scb-count' }, `${s.strings_per_scb || s.strings.length || 0}s`),
            h('span', { className: 'pav-scb-bar' }),
          )),
        ),
        scb && h(StringsPanel, {
          inv,
          scb,
          cap: stringCap,
          onClose: () => onSelectScb(inv.inverter_id, null),
        }),
      ),
    );
  }

  // ─── Main component ───────────────────────────────────────────────────────
  window.PlantArchitectureViz = ({ rows, plantId }) => {
    const [hideSpares, setHideSpares] = React.useState(false);
    const [expanded, setExpanded]     = React.useState(() => new Set());
    const [selectedScb, setSelectedScb] = React.useState({}); // { [invId]: scbId }

    const inverters = React.useMemo(() => {
      const list = groupArchitecture(rows || [], { hideSpares });
      const maxStrings = list.reduce((m, i) => Math.max(m, i.total_strings || 0), 1);
      for (const inv of list) {
        inv._maxStrings = maxStrings;
        inv._modulesTotal = inv.scbs.reduce((acc, s) =>
          acc + (s.modules_per_string || 0) * (s.strings_per_scb || s.strings.length || 0), 0) || 0;
      }
      return list;
    }, [rows, hideSpares]);

    const totals = React.useMemo(() => {
      let invCount = inverters.length;
      let scbCount = 0, stringCount = 0, dc = 0, spareCount = 0;
      for (const inv of inverters) {
        scbCount += inv.total_scbs;
        stringCount += inv.total_strings;
        dc += inv.total_dc_kwp;
        spareCount += inv.spare_scbs;
      }
      return { invCount, scbCount, stringCount, dc, spareCount };
    }, [inverters]);

    const allExpanded = inverters.length > 0 && expanded.size === inverters.length;

    const toggle = (id) => {
      setExpanded((prev) => {
        const next = new Set(prev);
        if (next.has(id)) { next.delete(id); }
        else { next.add(id); }
        return next;
      });
    };
    const expandAll = () => setExpanded(new Set(inverters.map((i) => i.inverter_id)));
    const collapseAll = () => { setExpanded(new Set()); setSelectedScb({}); };
    const onSelectScb = (invId, scbId) => {
      setSelectedScb((prev) => ({ ...prev, [invId]: scbId || undefined }));
    };

    if (!rows || rows.length === 0) {
      return h('div', { className: 'empty-state', style: { padding: 40 } },
        h('span', null, 'No architecture loaded. Upload Plant Architecture to see the diagram.'),
      );
    }

    return h('div', { className: 'pav-wrap' },
      // Controls
      h('div', { className: 'pav-controls' },
        h('span', { className: 'pav-chip' }, IconPlant, ' ', h('strong', null, plantId || 'Plant')),
        h('span', { className: 'pav-chip' }, h('strong', null, totals.invCount), ' inverters'),
        h('span', { className: 'pav-chip' }, h('strong', null, totals.scbCount), ' SCBs'),
        h('span', { className: 'pav-chip' }, h('strong', null, totals.stringCount), ' strings'),
        h('span', { className: 'pav-chip' }, h('strong', null, totals.dc.toFixed(1)), ' kWp'),
        h('div', { className: 'pav-seg' },
          h('button', {
            type: 'button',
            className: `pav-seg-btn ${allExpanded ? 'active' : ''}`,
            onClick: expandAll,
          }, 'Expand all'),
          h('button', {
            type: 'button',
            className: `pav-seg-btn ${expanded.size === 0 ? 'active' : ''}`,
            onClick: collapseAll,
          }, 'Collapse all'),
        ),
        h('label', { className: 'toggle-label', style: { marginLeft: 6 } },
          h('input', {
            type: 'checkbox',
            checked: hideSpares,
            onChange: (e) => setHideSpares(e.target.checked),
            style: { accentColor: 'var(--accent)' },
          }),
          h('span', null, 'Hide spares', totals.spareCount ? ` (${totals.spareCount})` : ''),
        ),
        h('div', { className: 'pav-legend' },
          h('span', null, h('i', { className: 'pav-legend-dot', style: { background: 'rgba(62,183,223,0.6)' } }), 'Active SCB'),
          h('span', null, h('i', { className: 'pav-legend-dot', style: { background: 'rgba(228,161,70,0.7)' } }), 'Spare SCB'),
          h('span', null, h('i', { className: 'pav-legend-dot', style: { background: 'radial-gradient(circle, #fde68a, #f59e0b)' } }), 'Current flow'),
        ),
      ),

      // Canvas
      h('div', { className: 'pav-canvas' },
        h('div', { className: 'pav-plant' },
          h('div', { className: 'pav-sun', 'aria-hidden': true }),
          h('div', null,
            h('div', { className: 'pav-plant-name' }, plantId || 'Plant'),
            h('div', { className: 'pav-plant-sub' },
              `${totals.invCount} Inverters · ${totals.scbCount} SCBs · ${totals.stringCount} Strings`
            ),
          ),
        ),
        h(FlowBackdrop),
        h('div', { className: `pav-inv-grid ${expanded.size > 0 ? 'expanded-row' : ''}` },
          inverters.map((inv) => h(InverterTile, {
            key: inv.inverter_id,
            inv,
            expanded: expanded.has(inv.inverter_id),
            selectedScbId: selectedScb[inv.inverter_id] || null,
            onToggle: toggle,
            onSelectScb,
          })),
        ),
        h('div', { style: { marginTop: 10, textAlign: 'center', fontSize: 11, color: 'var(--text-muted)' } },
          expanded.size === 0
            ? 'Click any inverter tile to expand its SCBs. Click an SCB to see its strings.'
            : 'Click an SCB inside an expanded inverter to view per-string panels with live current animation.'
        ),
      ),
    );
  };
})();
