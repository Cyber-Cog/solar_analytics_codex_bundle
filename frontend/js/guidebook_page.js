// frontend/js/guidebook_page.js
// Guidebook: end-user reference for every module, formula and algorithm.
// Structured as a persistent TOC on the left and scrollable content on the right.

(() => {
  const { Card } = window;
  const h = React.createElement;

  // ─── Styled helpers ────────────────────────────────────────────────────────
  function sectionTitle(text) {
    return h('h3', {
      style: { fontSize: 15, marginBottom: 10, marginTop: 6, color: 'var(--text)', fontWeight: 800 }
    }, text);
  }

  function subTitle(text) {
    return h('h4', {
      style: { fontSize: 13, margin: '14px 0 6px', color: 'var(--text)', fontWeight: 700, letterSpacing: '0.02em' }
    }, text);
  }

  function p(text) {
    return h('p', {
      style: { color: 'var(--text-soft)', lineHeight: 1.6, marginBottom: 8, fontSize: 13 }
    }, text);
  }

  function li(text) {
    return h('li', {
      style: { marginBottom: 6, color: 'var(--text-soft)', fontSize: 13, lineHeight: 1.55 }
    }, text);
  }

  function ul(items) {
    return h('ul', { style: { paddingLeft: 18, marginBottom: 10 } }, items.map((t, i) => h(React.Fragment, { key: i }, li(t))));
  }

  function formula(label, expression, note) {
    return h('div', {
      style: {
        padding: '10px 12px',
        border: '1px solid var(--line)',
        borderRadius: 10,
        marginBottom: 10,
        background: 'rgba(62,183,223,0.04)',
      }
    },
      h('div', { style: { fontSize: 11, color: 'var(--text-muted)', marginBottom: 4, textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 700 } }, label),
      h('div', {
        style: {
          fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
          fontSize: 12.5,
          color: 'var(--text)',
          lineHeight: 1.55,
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-word',
        }
      }, expression),
      note ? h('div', { style: { marginTop: 6, fontSize: 12, color: 'var(--text-soft)', lineHeight: 1.5 } }, note) : null
    );
  }

  function kv(label, value) {
    return h('div', { style: { display: 'flex', justifyContent: 'space-between', padding: '6px 0', borderBottom: '1px dashed var(--line-soft)', fontSize: 12.5 } },
      h('span', { style: { color: 'var(--text-soft)' } }, label),
      h('span', { style: { color: 'var(--text)', fontWeight: 600, textAlign: 'right' } }, value),
    );
  }

  function badge(text, color) {
    const colors = {
      blue:  { bg: 'rgba(62,183,223,0.12)',  fg: '#9ee6fb', bd: 'rgba(62,183,223,0.22)' },
      green: { bg: 'rgba(52,200,137,0.12)',  fg: '#7be0b0', bd: 'rgba(52,200,137,0.22)' },
      amber: { bg: 'rgba(228,161,70,0.12)',  fg: '#efc17c', bd: 'rgba(228,161,70,0.22)' },
      red:   { bg: 'rgba(235,107,107,0.12)', fg: '#f4aaaa', bd: 'rgba(235,107,107,0.22)' },
    }[color || 'blue'];
    return h('span', {
      style: {
        display: 'inline-block', padding: '2px 8px', borderRadius: 999,
        background: colors.bg, color: colors.fg, border: `1px solid ${colors.bd}`,
        fontSize: 10.5, fontWeight: 700, letterSpacing: '0.04em', marginRight: 6,
      }
    }, text);
  }

  // ─── Sections ───────────────────────────────────────────────────────────────
  const SECTIONS = [
    {
      id: 'overview',
      title: 'Platform Overview',
      render: () => [
        p('Photon Intelligence Centre is an analytics platform for grid-connected PV plants. It ingests plant architecture, equipment specs, and raw time-series data, and computes KPIs, faults, and losses. Every module uses the same underlying tables so numbers stay consistent across pages.'),
        subTitle('Data inputs'),
        ul([
          'Plant architecture (inverter \u2192 SCB \u2192 string hierarchy, DC capacity per SCB).',
          'Equipment specs (inverter datasheet, module datasheet, Isc/Voc/Pmax, temperature coefficients).',
          'Raw time-series (SCB DC current/voltage, inverter AC power/energy, WMS GTI/GHI/Wind/Module Temp).',
        ]),
        subTitle('Core outputs'),
        ul([
          'Dashboard KPIs (PR, PLF, CUF, PA, GA, Export, Import, Target, Peak, Active Power).',
          'Fault events (Disconnected Strings, Power Limitation, Inverter Shutdown, Grid Breakdown, Communication Issue, Soiling, Inverter Efficiency, Module Damage).',
          'Loss attribution per category with \u201cInvestigate\u201d drill-down.',
          'Analytics Lab for arbitrary signal plotting at inverter/SCB/string/WMS levels.',
        ]),
        subTitle('How to read this Guidebook'),
        p('Use the table of contents on the left to jump to any module. Each module page includes (1) what it does, (2) the exact formulas it uses, (3) the data it expects, and (4) common questions. Formulas are written in the same notation used by the backend. For where each screen reads its numbers from, open Data sources & API map.'),
      ],
    },

    {
      id: 'dashboard',
      title: 'Dashboard',
      render: () => [
        p('The Dashboard is the plant-level summary. It reads raw data and architecture for the selected date range and shows KPI cards, weather cards, energy/target charts, Active Power vs GTI, Expected vs Actual, and an inverter performance table.'),

        subTitle('Performance KPIs'),
        formula('Performance Ratio (Plant)',
          'PR(%) = ( Net Generation [kWh] \u00f7 Plant DC [kWp] \u00f7 Insolation [kWh/m\u00b2] \u00f7 Days ) \u00d7 100',
          'Plant DC is the sum of architecture DC per SCB (preferred) or equipment DC spec as fallback. Insolation uses WMS GTI, GHI fallback, else irradiance.'),
        formula('Plant Load Factor (PLF)',
          'PLF(%) = ( Plant Generation [kWh] \u00f7 ( Plant DC [kWp] \u00d7 24 \u00d7 Days ) ) \u00d7 100',
          'DC-based load factor. Changes with actual connected DC capacity.'),
        formula('Capacity Utilization Factor (CUF)',
          'CUF(%) = ( Plant Generation [kWh] \u00f7 ( Plant AC [kW] \u00d7 24 \u00d7 Days ) ) \u00d7 100',
          'AC-based equivalent of PLF; same formula but uses AC capacity in denominator.'),
        formula('Plant Availability (PA)',
          'PA(%) = ( \u03a3(Running Strings \u00d7 Runtime [h]) ) \u00f7 ( Total Strings \u00d7 Generation Hours ) \u00d7 100',
          'Weighted by string count and time. Disconnected Strings and Communication gaps reduce PA.'),
        formula('Grid Availability (GA)',
          'GA(%) = ( Generation Hours \u2212 Plant-wide Downtime ) \u00f7 Generation Hours \u00d7 100',
          'Plant-wide downtime = intervals when all strings / all inverters were down (e.g. grid breakdown, plant-level communication).'),

        subTitle('Energy KPIs'),
        formula('Energy Export',
          'Export [kWh] = \u03a3 ( inverter AC energy [kWh] )',
          'Prefers metered/inverter energy_export if available; otherwise integrates AC power: \u03a3 P[kW] \u00d7 \u0394t[h] with capped inter-sample gaps.'),
        formula('Energy Import',
          'Import [kWh] = \u03a3 ( inverter / meter import energy [kWh] )',
          'Shown next to Export; typically small (auxiliary load).'),
        formula('Net Generation',
          'Net Gen [kWh] = Export [kWh] \u2212 Import [kWh]',
          'Used in PR denominator; replaces pure Export when Import is non-zero.'),
        formula('Target Generation',
          'Target/min [kWh] = ( Monthly Target [kWh] \u00f7 Days-in-Month ) \u00f7 ( End Hour \u2212 Start Hour ) \u00d7 60',
          'Target is entered per plant in the Dashboard settings and distributed over the user-configured generation window. Changing the month target updates the chart baseline.'),

        subTitle('Instantaneous KPIs'),
        formula('Peak Power',
          'Peak = max over period of \u03a3 inverter AC power [kW] per minute',
          'Peak of the sum of all inverters (plant peak), not per inverter.'),
        formula('Active Power',
          'Active Power = average of \u03a3 inverter AC power [kW] over period',
          'Plant-level average for selected range.'),

        subTitle('WMS / Insolation'),
        formula('Insolation from WMS',
          'Insolation [kWh/m\u00b2] = \u03a3 ( GTI [W/m\u00b2] ) \u00f7 60000       (1-minute sampling)',
          'If GTI is absent, falls back to GHI / irradiance. Used in PR for both plant and per-inverter.'),

        subTitle('Charts'),
        ul([
          'Energy Export vs Target \u2014 daily bars; target is distributed per-minute over the configured generation window.',
          'Active Power vs GTI \u2014 plant AC on primary Y, GTI on secondary Y, datetime on X with Plotly-style zoom.',
          'Expected vs Actual \u2014 irradiance-driven expected power vs measured; gap = loss.',
          'Inverter performance table \u2014 Generation, DC Capacity, PR, PLF, Availability per inverter.',
        ]),

        subTitle('Inverter-level KPIs (same table)'),
        formula('Inverter PR',
          'PR(%) = ( Inverter Gen [kWh] \u00f7 Inverter DC [kWp] \u00f7 Insolation [kWh/m\u00b2] \u00f7 Days ) \u00d7 100',
          'Inverter DC = architecture DC sum for that inverter (preferred).'),
        formula('Inverter PLF',
          'PLF(%) = ( Inverter Gen [kWh] \u00f7 ( Inverter DC [kWp] \u00d7 24 \u00d7 Days ) ) \u00d7 100',
          'Per-inverter load factor.'),
      ],
    },

    {
      id: 'analytics',
      title: 'Analytics Lab',
      render: () => [
        p('Analytics Lab is a free-form plotter. Pick level \u2192 equipment \u2192 signals \u2192 time range and the platform queries raw_data_generic and derived tables. Each chart supports Plotly-style zoom, pan, crop, save as image, and data point inspection.'),

        subTitle('Hierarchy levels'),
        ul([
          'Inverter \u2014 AC power, AC energy, DC power, DC current, DC voltage (sum of SCBs when inverter-level raw is absent).',
          'SCB \u2014 DC current, DC voltage (raw measurements).',
          'String \u2014 derived from SCB and architecture (virtual string current = scb_current / strings_per_scb).',
          'WMS \u2014 GTI, GHI, Wind Speed, Ambient/Module Temperature, Rain.',
        ]),

        subTitle('Normalization'),
        formula('DC current (A/kWp)',
          'normalized = current [A] \u00f7 DC capacity [kWp] of that SCB / inverter / string',
          'Lets you compare SCBs of different sizes on the same chart.'),
        formula('AC or DC power (% of DC capacity)',
          'normalized = power [kW] \u00f7 DC capacity [kWp] \u00d7 100',
          'Puts all inverters/SCBs on the same 0\u2013100 scale for easy relative comparison.'),

        subTitle('Data availability'),
        formula('Availability',
          'Availability(%) = actual_rows \u00f7 ( N_unique_timestamps \u00d7 N_equipment \u00d7 N_signals ) \u00d7 100',
          'Computed on the observed timestamp grid (1-min or 15-min auto-detected). Caps at 100%.'),

        subTitle('Tips'),
        ul([
          'To catch communication gaps, set connectNulls off \u2014 the chart shows real gaps instead of smooth lines.',
          'Date and time are always shown on the X-axis and tooltip so you can match with events.',
          'Switch between raw and derived DC for inverter current/power to compare sources.',
        ]),
      ],
    },

    {
      id: 'fault',
      title: 'Fault Diagnostics',
      render: () => [
        p('Fault Diagnostics groups all automatic fault categories. Each category has a dedicated sub-page with tiles, per-inverter/SCB breakdown, and an Investigate modal to view time-series with red shaded fault windows.'),

        subTitle('Categories'),
        ul([
          h(React.Fragment, null, badge('DS', 'amber'), 'Disconnected Strings \u2014 string-level dropout detection.'),
          h(React.Fragment, null, badge('PL', 'blue'), 'Power Limitation \u2014 inverter under-performance vs peers.'),
          h(React.Fragment, null, badge('IS', 'red'), 'Inverter Shutdown \u2014 inverter off during daylight.'),
          h(React.Fragment, null, badge('GB', 'red'), 'Grid Breakdown \u2014 whole plant stops exporting during daylight.'),
          h(React.Fragment, null, badge('CI', 'amber'), 'Communication Issue \u2014 hierarchical plant/inverter/SCB data gaps.'),
          h(React.Fragment, null, badge('Soil', 'green'), 'Soiling \u2014 per-SCB PR degradation trend.'),
          h(React.Fragment, null, badge('InvEff', 'blue'), 'Inverter Efficiency \u2014 DC\u2192AC conversion loss.'),
          h(React.Fragment, null, badge('BPD', 'amber'), 'ByPass Diode / Module Damage \u2014 recurring string-level deficits.'),
        ]),
      ],
    },

    {
      id: 'fault-ds',
      title: 'Fault \u2014 Disconnected Strings',
      render: () => [
        p('Disconnected Strings (DS) detects individual strings that have stopped contributing current at each SCB. The algorithm compares actual per-string current against a plant reference built from the top-performing inverters.'),

        subTitle('Key definitions'),
        formula('Actual per-string current',
          'virtual_string_current = scb_current [A] \u00f7 strings_per_scb',
          'Per-SCB actual divided by string count.'),
        formula('Reference per-string current',
          'ref_current = MEDIAN(per_string of top 25% inverters, ranked by (\u03a3scb_current / \u03a3strings))',
          'Computed per-timestamp from top performers. Low-light timestamps (ref < 2 A) are skipped.'),
        formula('Missing strings',
          'missing_strings = round( max(0, ref_current \u00d7 strings_per_scb \u2212 scb_current) \u00f7 ref_current )',
          'Integer count of likely disconnected strings at that timestamp.'),

        subTitle('Filters (bad-data removal)'),
        ul([
          'Spare SCBs (spare_flag=true) are excluded.',
          'High-outlier: scb_current > Isc_STC \u00d7 strings_per_scb (default Isc = 10 A).',
          'Negative current: full SCB-day removed if any negative reading.',
          'Flatline: constant for > 120 consecutive timestamps \u2192 SCB-day removed; 10\u2013120 \u2192 points dropped.',
          'Leakage: daily max current < 20 A \u2192 SCB-day removed.',
          'Low irradiance: ref_current < 2 A \u2192 timestamp dropped.',
        ]),

        subTitle('Persistence state machine'),
        ul([
          'Confirm = candidate true for \u2308 30 min / interval \u2309 consecutive points (tolerance 1.5\u00d7 interval).',
          'Recover = clear true for \u2308 15 min / interval \u2309 consecutive points.',
          'While CONFIRMED_DS: power_loss_kw = dc_voltage \u00d7 missing_current \u00f7 1000; energy_loss_kwh = power_loss_kw \u00d7 (interval_min/60).',
        ]),

        subTitle('Loss calculation'),
        formula('DS energy loss',
          'Energy Loss [kWh] = \u03a3 over confirmed windows ( dc_voltage \u00d7 missing_current \u00f7 1000 \u00d7 \u0394t[h] )',
          'Requires dc_voltage in raw_data_generic. If voltage is missing, loss reported as N/A.'),
        formula('DS count in table',
          'displayed = MIN( missing_strings ) over selected range (cap 2000)',
          'Conservative: if any timestamp shows 0 missing, the SCB displays 0.'),

        subTitle('Constants'),
        h('div', null,
          kv('TOP_PERCENTILE', '0.25'),
          kv('LOW_IRRADIANCE_THRESHOLD_A', '2.0'),
          kv('PERSISTENCE_MINUTES', '30'),
          kv('RECOVERY_MINUTES', '15'),
          kv('CONSTANT_CONSECUTIVE_THRESHOLD', '10'),
          kv('FLATLINE_BAD_DATA_THRESHOLD', '120'),
          kv('LEAKAGE_MAX_CURRENT_A', '20'),
          kv('DEFAULT_ISC_STC_A', '10'),
        ),
      ],
    },

    {
      id: 'fault-pl',
      title: 'Fault \u2014 Power Limitation',
      render: () => [
        p('Power Limitation (PL) detects an inverter that is producing less AC power than its peers while conditions allow more. It is used to catch clipping, de-rating, or partial-shutdown cases.'),

        subTitle('Algorithm'),
        ul([
          'Limited to the daytime window (defaults 10:00\u201316:00 unless irradiance-based window is configured).',
          'Builds a peer reference: MEDIAN of normalized AC power (kW / kWp) of all inverters at that timestamp.',
          'Candidate inverter if its normalized AC power < reference \u00d7 threshold (default 0.85).',
          'Confirmed after persistence (same state machine as DS, default 30 min).',
        ]),

        subTitle('Loss'),
        formula('PL energy loss',
          'Loss [kWh] = \u03a3 ( (ref_ac [kW] \u2212 inv_ac [kW]) \u00d7 \u0394t[h] )  for confirmed windows',
          'Ref is the peer median, scaled by the inverter\u2019s own DC capacity.'),
      ],
    },

    {
      id: 'fault-is',
      title: 'Fault \u2014 Inverter Shutdown',
      render: () => [
        p('Inverter Shutdown (IS) detects periods when an individual inverter is off during daylight while others are running.'),

        subTitle('Criteria'),
        ul([
          'Inverter AC power = 0 (or below a small threshold) for \u2265 configured minutes.',
          'At least one peer inverter is producing (else it is classified as Grid Breakdown).',
          'Daylight only: plant irradiance above threshold or within daytime window.',
        ]),

        subTitle('Loss'),
        formula('IS loss',
          'Loss [kWh] = \u03a3 ( ref_ac [kW] \u00d7 \u0394t[h] )  over shutdown window',
          'ref_ac is the expected AC power for that inverter based on peer median normalized by its DC capacity.'),
      ],
    },

    {
      id: 'fault-gb',
      title: 'Fault \u2014 Grid Breakdown',
      render: () => [
        p('Grid Breakdown (GB) detects whole-plant export loss during daylight hours. It takes precedence over IS to avoid attributing the outage to every inverter individually.'),
        subTitle('Criteria'),
        ul([
          '\u03a3 inverter AC power = 0 (all inverters off) during daylight.',
          'Sustained for \u2265 configured minutes.',
        ]),
        formula('GB loss',
          'Loss [kWh] = \u03a3 ( expected_plant_ac [kW] \u00d7 \u0394t[h] )',
          'Expected plant AC derived from irradiance-based expected model.'),
      ],
    },

    {
      id: 'fault-comm',
      title: 'Fault \u2014 Communication Issue',
      render: () => [
        p('Communication Issue (CI) catches periods when data is missing while the plant should be generating (WMS confirms irradiance). Ownership is hierarchical so loss is never counted twice.'),

        subTitle('Hierarchy of events'),
        ul([
          h(React.Fragment, null, badge('Plant', 'red'), '  All inverters missing at a daylight timestamp.'),
          h(React.Fragment, null, badge('Inverter', 'amber'), '  Inverter telemetry missing (or all its SCBs missing while inverter data exists).'),
          h(React.Fragment, null, badge('SCB', 'blue'), '  Inverter is present, but specific SCB(s) missing.'),
        ]),

        subTitle('Loss model'),
        formula('Expected power at timestamp t',
          'P_expected(t) = GTI(t) [W/m\u00b2] \u00f7 1000 \u00d7 DC_kWp \u00d7 perf_factor',
          'perf_factor is learnt from good-data windows. Used to monetize outages in kWh.'),
        formula('CI loss',
          'Loss [kWh] = \u03a3 P_expected(t) \u00d7 \u0394t[h]  over plant/inverter events only',
          'SCB-only ingestion gaps (inverter data present) contribute 0 kWh \u2014 treated as a data/ingest issue, not a real outage.'),

        subTitle('Why this design'),
        ul([
          'Prevents duplicate loss: a plant-wide outage is not also listed under each inverter.',
          'Reflects reality: if the inverter reports AC power, the strings were producing \u2014 missing SCB telemetry is a monitoring gap, not energy lost.',
          'Makes the Investigate modal actionable: red shaded regions match real generation hours.',
        ]),
      ],
    },

    {
      id: 'fault-soiling',
      title: 'Fault \u2014 Soiling',
      render: () => [
        p('Soiling analytics estimates the PR degradation of each SCB over a rolling baseline. It highlights which SCBs are losing yield relative to their expected curve.'),
        subTitle('Approach'),
        ul([
          'Build a per-SCB daily PR using scb_current \u00d7 dc_voltage (energy proxy) \u00f7 SCB DC \u00f7 insolation.',
          'Compute baseline PR as the rolling top-quartile of recent clean days.',
          'Soiling loss % = max(0, baseline_PR \u2212 SCB_PR) \u00f7 baseline_PR \u00d7 100.',
          'Ranked list highlights the SCBs to clean.',
        ]),
      ],
    },

    {
      id: 'fault-inveff',
      title: 'Fault \u2014 Inverter Efficiency',
      render: () => [
        p('Efficiency tab compares DC input to AC output for each inverter.'),
        formula('Efficiency',
          '\u03b7(%) = AC energy [kWh] \u00f7 DC energy [kWh] \u00d7 100',
          'DC energy = \u03a3 (scb_current \u00d7 dc_voltage) \u00f7 1000 \u00d7 \u0394t integrated per inverter.'),
        formula('Conversion Loss',
          'Conversion Loss [kWh] = DC energy \u2212 AC energy',
          'Per inverter, per period; summed for plant-level conversion loss.'),
      ],
    },

    {
      id: 'fault-damage',
      title: 'Fault \u2014 Bypass Diode / Module Damage',
      render: () => [
        p('Detects recurring per-string current deficits that persist across many days, consistent with bypass-diode failure or cell damage rather than a clean DS disconnect.'),
        ul([
          'Aggregate DS confirmations per string across the range.',
          'Flag strings where missing_strings \u2265 1 on \u2265 N distinct days (configurable).',
          'Prioritizes candidates for on-site inspection or IV-curve testing.',
        ]),
      ],
    },

    {
      id: 'loss',
      title: 'Loss Analysis',
      render: () => [
        p('Loss Analysis unifies all fault categories into one ledger. Each row shows category, equipment, duration, energy loss, and an Investigate action.'),
        subTitle('Loss categories shown'),
        ul([
          'Disconnected Strings (DS).',
          'Power Limitation (PL).',
          'Inverter Shutdown (IS).',
          'Grid Breakdown (GB).',
          'Communication Issue (CI).',
          'Soiling.',
          'Conversion / Efficiency loss.',
        ]),
        subTitle('Totals'),
        formula('Total Loss',
          'Total [kWh] = \u03a3 category_losses  (hierarchical; no double-counting)',
          'Plant CI absorbs inverter CI; inverter CI absorbs its SCB CI. GB absorbs concurrent IS.'),
        p('Sortable by loss, date, equipment. Investigate opens a time-series modal with the AC/DC signals and WMS on a secondary axis; red shaded bands mark confirmed fault windows.'),
      ],
    },

    {
      id: 'metadata',
      title: 'Metadata',
      render: () => [
        p('Metadata is where you onboard a plant and keep its static configuration up to date. The page has three sub-tabs: Architecture, Equipment Specs, Raw Data.'),

        subTitle('Plant Architecture'),
        ul([
          'Defines the hierarchy plant \u2192 inverter \u2192 SCB \u2192 string.',
          'Columns: plant_id, inverter_id, scb_id, string_id, modules_per_string, strings_per_scb, dc_capacity_kw, spare_flag.',
          'DC capacity here is the source of truth for per-inverter DC (used by PR, PLF, normalization).',
          'Now available as an animated diagram \u2014 switch view with the Diagram/Table toggle.',
        ]),

        subTitle('Equipment Specs'),
        ul([
          'Inverter spec (AC/DC capacity, rated/Euro efficiency, MPPT range, set-points, degradation, temp coeff).',
          'Module spec (Pmax, Impp/Vmpp, Isc/Voc, \u03b1/\u03b2/\u03b3 at STC/NOCT, degradation schedule, efficiency).',
          'Upload spec PDFs and have them available as downloads.',
        ]),

        subTitle('Raw Data'),
        ul([
          'Bulk upload via Excel; the mapper auto-detects common column names.',
          'After upload, Disconnected String detection runs automatically and caches are invalidated so Dashboard/Faults refresh on next load.',
          'Preview table shows the last N rows per signal to verify ingest before running analytics.',
        ]),
      ],
    },

    {
      id: 'admin',
      title: 'Admin',
      render: () => [
        p('The Admin portal is visible only to users with is_admin=true.'),
        ul([
          'Manage users (create, grant admin, restrict allowed plants).',
          'Delete a plant and all its related data permanently (raw data, architecture, equipment specs, faults, snapshots, caches).',
          'Configure ticket recipient email \u2014 where user-raised support tickets are sent.',
          'Download plant reports (PDF) covering Dashboard KPIs, graphs, and loss analysis.',
        ]),
      ],
    },

    {
      id: 'data-lineage',
      title: 'Data sources & API map',
      render: () => [
        p('Use this table to trace any on-screen value to its HTTP endpoint and, where it helps performance tuning, the underlying data stores. The UI uses window.location.origin for API calls (same host as the app) unless you set localStorage.solar_api_base for development.'),
        subTitle('Request lifecycle'),
        ul([
          'Top bar plant and date range are React state; every module receives plant_id, date_from, date_to as query parameters.',
          'Authentication: Bearer token from /auth/login is sent on all /api/* calls (see frontend/js/api.js).',
          'Slow screens: use browser DevTools \u2192 Network, filter by plant_id, and compare timings between environments.',
        ]),
        subTitle('Dashboard'),
        ul([
          'GET /api/dashboard/bundle (and related) \u2014 assembled KPIs, energy, weather, WMS, inverter table; backed by raw time-series, plant_equipment, architecture, and dashboard cache helpers in backend/routers/dashboard.py.',
        ]),
        subTitle('Analytics Lab'),
        ul([
          'GET /api/analytics/equipment and GET /api/analytics/signals?level=&plant_id= \u2014 equipment ids and distinct signals per hierarchy; reads raw_data_generic, dc_hierarchy_derived, plant_equipment (router: backend/routers/analytics.py).',
          'GET /api/analytics/timeseries \u2014 plotted series for selected equipment and signals; responses are cached server-side for a few minutes.',
          'If parameters stay empty in production but work locally, check Network for /api/analytics/signals (timeouts 502/504, 401) and confirm the production API is the same commit as the environment where signals work (older routers could time out on large plants).',
        ]),
        subTitle('Fault Diagnostics'),
        ul([
          'GET /api/faults/unified-feed (with client-side merge fallbacks) for overview tables and tiles; category-specific GET /api/faults/* (ds-*, pl-*, is-*, comm-*, etc.) in backend/routers/faults.py.',
          'Precomputed snapshots may be read when present; otherwise engines compute from raw data (see backend/engine/).',
        ]),
        subTitle('Loss Analysis'),
        ul([
          'GET /api/loss-analysis/bridge and options (or /api/dashboard/loss-analysis/* aliases) \u2014 per-category energy bridge; sources listed in backend/routers and loss helpers.',
        ]),
        subTitle('Metadata'),
        ul([
          'Architecture, equipment specs, uploads: /api/metadata/* and related routers; persistence in PostgreSQL tables plant_architecture, plant_equipment, raw_data_generic, etc.',
        ]),
        subTitle('Reports, appearance, admin'),
        ul([
          'Reports: /api/reports/* (generated artifacts).',
          'Site theme: /api/site/appearance; Admin: /api/admin/* — user and plant management.',
        ]),
        p('For algorithm-level detail, the FAQ entry \u201cWhere are the raw algorithm sources?\u201d still points at backend/engine/*.py and backend/routers/*.py as the code of record.'),
      ],
    },

    {
      id: 'faqs',
      title: 'FAQs',
      render: () => [
        subTitle('Why did PR or PLF change after a re-upload?'),
        p('They depend on generation, DC capacity, and insolation. Re-ingesting raw data changes generation; re-uploading architecture changes DC. Both recompute caches and new values are reflected.'),
        subTitle('Which DC capacity is used by PR/PLF/normalization?'),
        p('The architecture DC sum per inverter is preferred; the equipment-spec DC is used only when architecture is missing. This matches the actual connected capacity on site.'),
        subTitle('Why does a Communication fault show 0 loss?'),
        p('Only plant- and inverter-level communication events carry loss. SCB-only gaps (inverter is reporting but some SCBs aren\u2019t) are a monitoring gap, not energy lost, so loss is 0.'),
        subTitle('Why does one timestamp not appear in two communication events?'),
        p('Ownership is hierarchical: plant \u2192 inverter \u2192 SCB. A plant-wide gap absorbs the inverter and SCB gaps, so kWh loss is never counted twice.'),
        subTitle('Data availability shows 100% on very few rows \u2014 is it correct?'),
        p('Availability uses the observed unique timestamps instead of assuming 15-min spacing, so 1-min and 15-min data are both judged correctly. If all expected cells are present on the observed grid, availability is 100% even for small samples.'),
        subTitle('Where are the raw algorithm sources?'),
        p('See backend/engine/*.py for detection engines, backend/routers/dashboard.py for KPI assembly, and backend/docs/*.md for extended algorithm notes. This Guidebook mirrors those sources.'),
      ],
    },
  ];

  // ─── Page component ────────────────────────────────────────────────────────
  window.GuidebookPage = () => {
    const [active, setActive] = React.useState('overview');
    const contentRef = React.useRef(null);

    React.useEffect(() => {
      const el = document.getElementById(`gb-section-${active}`);
      if (el && contentRef.current) {
        const top = el.offsetTop - 8;
        contentRef.current.scrollTo({ top, behavior: 'smooth' });
      }
    }, [active]);

    const toc = h('aside', {
      style: {
        width: 220,
        minWidth: 220,
        position: 'sticky',
        top: 0,
        alignSelf: 'flex-start',
        paddingRight: 8,
      }
    },
      h(Card, { title: 'Contents' },
        h('nav', { style: { display: 'flex', flexDirection: 'column', gap: 2 } },
          SECTIONS.map((s) => h('button', {
            key: s.id,
            type: 'button',
            onClick: () => setActive(s.id),
            style: {
              textAlign: 'left',
              background: active === s.id ? 'var(--accent-soft)' : 'transparent',
              color: active === s.id ? 'var(--accent)' : 'var(--text-soft)',
              border: '1px solid',
              borderColor: active === s.id ? 'var(--accent-strong)' : 'transparent',
              borderRadius: 8,
              padding: '7px 10px',
              fontSize: 12.5,
              fontWeight: active === s.id ? 700 : 500,
              cursor: 'pointer',
              fontFamily: 'inherit',
            }
          }, s.title)),
        ),
      ),
    );

    const body = h('div', {
      ref: contentRef,
      style: {
        flex: 1,
        minWidth: 0,
        maxHeight: 'calc(100vh - 110px)',
        overflowY: 'auto',
        paddingRight: 8,
        scrollBehavior: 'smooth',
      }
    },
      h(Card, { title: 'Photon Intelligence Centre \u2014 Guidebook', style: { marginBottom: 14 } },
        p('Reference for every module in the platform: what it does, the formulas it uses, and answers to common questions. The Guidebook mirrors live code so the numbers in every module match these definitions.'),
        p('Tip: jump directly to any section using the Contents panel on the left.')
      ),
      SECTIONS.map((s) => h('div', { key: s.id, id: `gb-section-${s.id}`, style: { marginBottom: 14 } },
        h(Card, { title: s.title }, ...s.render())
      )),
    );

    return h('div', { style: { display: 'flex', gap: 14, alignItems: 'flex-start' } }, toc, body);
  };
})();
