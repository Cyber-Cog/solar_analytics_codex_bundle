// frontend/js/reports_page.js
// ─────────────────────────────────────────────────────────────────────────────
// Reports page — lets the user assemble a branded plant-performance report and
// download it as **PDF / XLSX / DOCX**. Sections are opt-in (checkboxes) and
// format availability is probed against the backend at mount time so we never
// show a format the server cannot actually produce.
// ─────────────────────────────────────────────────────────────────────────────

(function () {
  const { useState, useEffect, useMemo } = React;
  const h = React.createElement;

  const FORMAT_META = {
    pdf:  { label: 'PDF',        note: 'Branded, print-ready document with charts & tables',                 icon: 'M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z', accent: '#ef4444', mime: 'application/pdf' },
    xlsx: { label: 'Excel',      note: 'Multi-sheet workbook — overview, KPIs, inverters, losses, energy',   icon: 'M8 6h13M8 12h13M8 18h13M3 6h.01M3 12h.01M3 18h.01',         accent: '#10b981', mime: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' },
    docx: { label: 'Word',       note: 'Editable document — drop into customer deliverables',               icon: 'M21 15a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z', accent: '#3b82f6', mime: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' },
    html: { label: 'HTML',       note: 'Self-contained page — opens in any browser, easy to read & print', icon: 'M4 4h16v16H4zM4 9h16M9 4v16',                                 accent: '#f97316', mime: 'text/html' },
  };

  const DEFAULT_SECTIONS = [
    { id: 'overview',     label: 'Plant Overview',        hint: 'Name, capacity, COD, technology, location' },
    { id: 'kpis',         label: 'Key Performance Indicators', hint: 'Generation, PR, PLF, insolation, peak power' },
    { id: 'energy_trend', label: 'Daily Energy Trend',    hint: 'Bar chart of generation vs target across the range' },
    { id: 'inverters',    label: 'Per-Inverter Performance', hint: 'Table + chart of generation & PR per inverter' },
    { id: 'losses',       label: 'Loss Analysis',         hint: 'Category-wise loss breakdown with MWh totals' },
    { id: 'faults',       label: 'Fault Summary',         hint: 'Fault counts per category (rolls up with losses)' },
  ];

  window.ReportsPage = ({ plantId, plants, dateFrom, dateTo }) => {
    const [options, setOptions]         = useState(null);
    const [format, setFormat]           = useState('pdf');
    const [selectedSections, setSelected] = useState(
      new Set(['overview', 'kpis', 'energy_trend', 'inverters', 'losses'])
    );
    const [title, setTitle]             = useState('');
    const [rangeFrom, setRangeFrom]     = useState(dateFrom || '');
    const [rangeTo, setRangeTo]         = useState(dateTo || '');
    const [busy, setBusy]               = useState(false);
    const [error, setError]             = useState('');
    const [recent, setRecent]           = useState([]);

    useEffect(() => { setRangeFrom(dateFrom || ''); setRangeTo(dateTo || ''); }, [dateFrom, dateTo]);

    useEffect(() => {
      window.SolarAPI.Reports.options()
        .then((o) => setOptions(o))
        .catch(() => setOptions({ formats: { pdf: true, xlsx: true, docx: true, html: true }, charts_enabled: true, sections: DEFAULT_SECTIONS }));
    }, []);

    const formatsAvailable = (options && options.formats) || {};
    const chartsEnabled = options ? !!options.charts_enabled : true;

    // Auto-fall-back if the currently chosen format isn't available on this server.
    useEffect(() => {
      if (options && !formatsAvailable[format]) {
        const first = ['pdf', 'html', 'xlsx', 'docx'].find((k) => formatsAvailable[k]);
        if (first) setFormat(first);
      }
    }, [options]); // eslint-disable-line

    const toggleSection = (id) => {
      setSelected((prev) => {
        const next = new Set(prev);
        if (next.has(id)) next.delete(id); else next.add(id);
        return next;
      });
    };

    const plantName = useMemo(() => {
      const p = (plants || []).find((x) => x.plant_id === plantId);
      return p ? p.name : plantId;
    }, [plants, plantId]);

    async function handleGenerate() {
      if (!plantId) { setError('Select a plant first (top-bar).'); return; }
      if (selectedSections.size === 0) { setError('Pick at least one section.'); return; }
      setError('');
      setBusy(true);
      try {
        const { blob, filename } = await window.SolarAPI.Reports.generate({
          plant_id: plantId,
          date_from: rangeFrom,
          date_to: rangeTo,
          format,
          sections: Array.from(selectedSections),
          title: title ? title.trim() : undefined,
        });
        // Trigger download
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a); a.click();
        setTimeout(() => { document.body.removeChild(a); URL.revokeObjectURL(url); }, 0);
        setRecent((r) => [{ filename, format, ts: new Date().toISOString() }, ...r].slice(0, 5));
      } catch (e) {
        setError(e && e.message ? e.message : 'Report generation failed.');
      } finally {
        setBusy(false);
      }
    }

    return h('div', { className: 'reports-page' },
      h('div', { className: 'reports-hero' },
        h('div', { className: 'reports-hero-title' },
          h('div', { className: 'reports-hero-eyebrow' }, 'Reporting'),
          h('h2', null, 'Plant Performance Report'),
          h('p', null,
            'Assemble a polished, stakeholder-ready report for ',
            h('strong', null, plantName || 'the selected plant'),
            ' — pick the range, choose the sections you care about, and download in the format you need.'),
        ),
      ),

      error && h('div', { className: 'reports-error' }, error),

      h('div', { className: 'reports-grid' },
        // ── Left column: config ──────────────────────────────────────────────
        h('div', { className: 'reports-left' },
          h('div', { className: 'card' },
            h('div', { className: 'card-header' }, h('span', { className: 'card-title' }, '1 — Coverage')),
            h('div', { className: 'card-body reports-section' },
              h('div', { className: 'reports-row' },
                h('label', { className: 'reports-field' },
                  h('span', null, 'From'),
                  h('input', { type: 'date', className: 'date-input', value: rangeFrom, onChange: (e) => setRangeFrom(e.target.value) }),
                ),
                h('label', { className: 'reports-field' },
                  h('span', null, 'To'),
                  h('input', { type: 'date', className: 'date-input', value: rangeTo, onChange: (e) => setRangeTo(e.target.value) }),
                ),
              ),
              h('label', { className: 'reports-field' },
                h('span', null, 'Report title (optional)'),
                h('input', {
                  type: 'text',
                  className: 'form-input',
                  placeholder: 'e.g. March 2026 — BDL Monthly Review',
                  value: title,
                  onChange: (e) => setTitle(e.target.value),
                }),
              ),
            ),
          ),

          h('div', { className: 'card' },
            h('div', { className: 'card-header' }, h('span', { className: 'card-title' }, '2 — Sections')),
            h('div', { className: 'card-body' },
              h('div', { className: 'reports-sections' },
                DEFAULT_SECTIONS.map((s) => h('label', {
                  key: s.id,
                  className: `reports-section-chip ${selectedSections.has(s.id) ? 'is-active' : ''}`,
                },
                  h('input', { type: 'checkbox', checked: selectedSections.has(s.id), onChange: () => toggleSection(s.id) }),
                  h('div', null,
                    h('div', { className: 'reports-section-label' }, s.label),
                    h('div', { className: 'reports-section-hint' }, s.hint),
                  ),
                )),
              ),
            ),
          ),

          h('div', { className: 'card' },
            h('div', { className: 'card-header' }, h('span', { className: 'card-title' }, '3 — Format')),
            h('div', { className: 'card-body' },
              h('div', { className: 'reports-formats' },
                ['pdf', 'html', 'xlsx', 'docx'].map((k) => {
                  const meta = FORMAT_META[k];
                  const avail = formatsAvailable[k] !== false;
                  return h('button', {
                    key: k,
                    type: 'button',
                    className: `reports-format-card ${format === k ? 'is-selected' : ''} ${avail ? '' : 'is-disabled'}`,
                    onClick: () => avail && setFormat(k),
                    style: { '--rp-accent': meta.accent },
                    disabled: !avail,
                    title: avail ? '' : `${meta.label} is not installed on this server`,
                  },
                    h('span', { className: 'reports-format-icon' },
                      h('svg', { width: 22, height: 22, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 1.8, strokeLinecap: 'round', strokeLinejoin: 'round' },
                        h('path', { d: meta.icon }),
                      ),
                    ),
                    h('span', { className: 'reports-format-label' }, meta.label),
                    h('span', { className: 'reports-format-note' }, meta.note),
                    !avail && h('span', { className: 'reports-badge-missing' }, 'unavailable'),
                  );
                }),
              ),
              !chartsEnabled && h('div', { className: 'reports-hint' },
                'Charts disabled — install ', h('code', null, 'matplotlib'),
                ' on the server to embed bar charts & graphs in the documents.',
              ),
            ),
          ),

          h('div', { className: 'reports-cta-row' },
            h('button', {
              className: 'btn btn-primary reports-generate-btn',
              onClick: handleGenerate,
              disabled: busy,
            }, busy
              ? h(React.Fragment, null, h(window.Spinner, null), ' Generating…')
              : `Generate ${FORMAT_META[format].label}`,
            ),
          ),
        ),

        // ── Right column: preview / summary ──────────────────────────────────
        h('div', { className: 'reports-right' },
          h('div', { className: 'reports-preview card' },
            h('div', { className: 'card-header' }, h('span', { className: 'card-title' }, 'Preview')),
            h('div', { className: 'card-body' },
              h('div', { className: 'reports-preview-page' },
                h('div', { className: 'reports-preview-accent' }, 'SOLAR ANALYTICS • PLANT REPORT'),
                h('div', { className: 'reports-preview-title' }, title || `Performance Report — ${plantName || plantId || 'Plant'} — ${rangeFrom} to ${rangeTo}`),
                h('div', { className: 'reports-preview-meta' },
                  `${format.toUpperCase()}  •  `,
                  `${selectedSections.size} section${selectedSections.size === 1 ? '' : 's'}  •  `,
                  `Range: ${rangeFrom} → ${rangeTo}`,
                ),
                h('div', { className: 'reports-preview-sections' },
                  DEFAULT_SECTIONS.filter((s) => selectedSections.has(s.id)).map((s) => h('div', {
                    key: s.id,
                    className: 'reports-preview-section',
                  },
                    h('div', { className: 'reports-preview-dot' }),
                    h('div', null,
                      h('div', { className: 'reports-preview-section-title' }, s.label),
                      h('div', { className: 'reports-preview-section-hint' }, s.hint),
                    ),
                  )),
                ),
                selectedSections.size === 0 && h('div', { className: 'reports-preview-empty' }, 'No sections selected.'),
              ),
            ),
          ),

          recent.length > 0 && h('div', { className: 'card' },
            h('div', { className: 'card-header' }, h('span', { className: 'card-title' }, 'Recent downloads')),
            h('div', { className: 'card-body reports-recent' },
              recent.map((r) => h('div', { key: r.ts, className: 'reports-recent-row' },
                h('span', { className: 'reports-recent-format' }, r.format.toUpperCase()),
                h('span', { className: 'reports-recent-name' }, r.filename),
                h('span', { className: 'reports-recent-time' }, new Date(r.ts).toLocaleTimeString()),
              )),
            ),
          ),
        ),
      ),
    );
  };
})();
