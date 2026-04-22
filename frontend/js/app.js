// frontend/js/app.js
// Root React application - auth guard, sidebar layout, page router.

const { useState, useEffect, useLayoutEffect, useCallback } = React;
const h = React.createElement;

const HASH_TO_PAGE = {
  'dashboard': 'Dashboard',
  'analytics-lab': 'Analytics Lab',
  'fault-diagnostics': 'Fault Diagnostics',
  'loss-analysis': 'Loss Analysis',
  'reports': 'Reports',
  'guidebook': 'Guidebook',
  'metadata': 'Metadata',
  'admin': 'Admin',
};
const PAGE_TO_HASH = {
  'Dashboard': 'dashboard',
  'Analytics Lab': 'analytics-lab',
  'Fault Diagnostics': 'fault-diagnostics',
  'Loss Analysis': 'loss-analysis',
  'Reports': 'reports',
  'Guidebook': 'guidebook',
  'Metadata': 'metadata',
  'Admin': 'admin',
};

const FAULT_SUB_IDS = ['overview', 'ds', 'pl', 'is', 'gb', 'comm', 'clip', 'derate', 'scb_perf', 'inv_eff', 'damage'];

function parseLocationHash() {
  const raw = (window.location.hash || '').replace(/^#/, '').trim().toLowerCase();
  const parts = raw.split('/').map((p) => p.trim()).filter(Boolean);
  if (parts[0] === 'fault-diagnostics') {
    const sub = parts[1] && FAULT_SUB_IDS.includes(parts[1]) ? parts[1] : 'overview';
    return { page: 'Fault Diagnostics', faultSub: sub };
  }
  const pageKey = parts[0] || '';
  return {
    page: HASH_TO_PAGE[pageKey] || 'Dashboard',
    faultSub: null,
  };
}

const PLANT_STORAGE_KEY = 'solar_selected_plant_id';

function readStoredPlantId() {
  try { return localStorage.getItem(PLANT_STORAGE_KEY) || ''; } catch (e) { return ''; }
}

function isoDateOnly(value) {
  return String(value || '').slice(0, 10);
}

function shiftIsoDate(value, days) {
  const base = new Date(`${value}T00:00:00`);
  if (Number.isNaN(base.getTime())) return value;
  base.setDate(base.getDate() + days);
  return base.toISOString().slice(0, 10);
}

const ThemeIconSun = h('svg', { width: 20, height: 20, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2, strokeLinecap: 'round', strokeLinejoin: 'round' },
  h('circle', { cx: 12, cy: 12, r: 4 }),
  h('line', { x1: 12, y1: 1, x2: 12, y2: 3 }), h('line', { x1: 12, y1: 21, x2: 12, y2: 23 }),
  h('line', { x1: 4.22, y1: 4.22, x2: 5.64, y2: 5.64 }), h('line', { x1: 18.36, y1: 18.36, x2: 19.78, y2: 19.78 }),
  h('line', { x1: 1, y1: 12, x2: 3, y2: 12 }), h('line', { x1: 21, y1: 12, x2: 23, y2: 12 }),
  h('line', { x1: 4.22, y1: 19.78, x2: 5.64, y2: 18.36 }), h('line', { x1: 18.36, y1: 5.64, x2: 19.78, y2: 4.22 }),
);

const ThemeIconMoon = h('svg', { width: 20, height: 20, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2, strokeLinecap: 'round', strokeLinejoin: 'round' },
  h('path', { d: 'M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z' }),
);

// ── Topbar ────────────────────────────────────────────────────────────────────
function Topbar({ page, plants, plantId, onPlantChange, onAddPlant, dateFrom, dateTo, onDateChange, user, theme, onThemeToggle, onThemeSelect, sidebarOpen, onToggleSidebar }) {
  const [showUserMenu, setShowUserMenu] = useState(false);
  const isLight = theme === 'light';
  const PAGE_TITLES = {
    Dashboard: 'Dashboard',
    'Analytics Lab': 'Analytics Lab',
    'Fault Diagnostics': 'Fault Diagnostics',
    'Loss Analysis': 'Loss Analysis',
    Reports: 'Reports',
    Guidebook: 'Guidebook',
    Metadata: 'Metadata',
    Admin: 'Admin',
  };

  return h('div', { className: 'topbar' },
    h('div', { className: 'topbar-leading' },
      h('button', {
        type: 'button',
        className: 'hamburger-btn',
        onClick: onToggleSidebar,
        'aria-label': sidebarOpen ? 'Collapse sidebar' : 'Expand sidebar',
        'aria-expanded': sidebarOpen,
      },
        h('svg', { width: 18, height: 18, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2, 'aria-hidden': true },
          h('line', { x1: '3', y1: '12', x2: '21', y2: '12' }),
          h('line', { x1: '3', y1: '6', x2: '21', y2: '6' }),
          h('line', { x1: '3', y1: '18', x2: '21', y2: '18' }),
        ),
      ),
      h('span', { className: 'topbar-title' }, PAGE_TITLES[page] || page),
    ),
    h('div', { className: 'topbar-controls' },
      h('div', { className: 'topbar-group topbar-group-plant' },
        h('div', { className: 'plant-selector' },
          h('span', { className: 'topbar-field-label' }, 'Plant'),
          h('select', { className: 'topbar-plant-select', value: plantId, onChange: e => onPlantChange(e.target.value) },
            plants.length === 0 ? h('option', { value: '' }, '- No plants -') : plants.map(p => h('option', { key: p.plant_id, value: p.plant_id }, p.name)),
          ),
        ),
        h('button', { type: 'button', className: 'btn btn-outline topbar-add-plant', onClick: onAddPlant }, 'Add Plant'),
      ),
      ['Dashboard', 'Analytics Lab', 'Fault Diagnostics', 'Loss Analysis', 'Reports'].includes(page) && h('div', { className: 'topbar-group topbar-group-dates' },
        h('input', { type: 'date', className: 'date-input', value: dateFrom, onChange: e => onDateChange(e.target.value, dateTo) }),
        h('span', { className: 'topbar-date-sep' }, '\u2192'),
        h('input', { type: 'date', className: 'date-input', value: dateTo, onChange: e => onDateChange(dateFrom, e.target.value) }),
        window.DatePresetPicker && h(window.DatePresetPicker, { dateFrom, dateTo, onDateChange }),
      ),
      h('div', { className: 'topbar-group topbar-group-actions' },
        h('button', { type: 'button', className: 'btn btn-outline theme-toggle-btn', onClick: onThemeToggle }, isLight ? ThemeIconMoon : ThemeIconSun),
        h('div', { className: 'user-menu-wrap' },
          h('div', { className: 'user-avatar', onClick: (e) => { e.stopPropagation(); setShowUserMenu(p => !p); } },
            (user?.full_name || user?.email || 'U')[0].toUpperCase(),
          ),
          showUserMenu && h('div', { className: 'user-menu-dropdown' },
            h('div', { className: 'user-menu-title' }, 'Theme'),
            h('button', { className: `user-menu-item ${theme === 'dark' ? 'active' : ''}`, onClick: () => onThemeSelect('dark') }, 'Dark'),
            h('button', { className: `user-menu-item ${theme === 'light' ? 'active' : ''}`, onClick: () => onThemeSelect('light') }, 'Light'),
            h('button', { className: `user-menu-item ${theme === 'vikram' ? 'active' : ''}`, onClick: () => onThemeSelect('vikram') }, 'Vikram Solar'),
          ),
        ),
      ),
    ),
  );
}

const NavIcons = {
  Dashboard: h('svg', { width: 18, height: 18, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2 }, h('rect', { x: 3, y: 3, width: 7, height: 7 }), h('rect', { x: 14, y: 3, width: 7, height: 7 }), h('rect', { x: 3, y: 14, width: 7, height: 7 }), h('rect', { x: 14, y: 14, width: 7, height: 7 })),
  'Analytics Lab': h('svg', { width: 18, height: 18, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2 }, h('line', { x1: 18, y1: 20, x2: 18, y2: 10 }), h('line', { x1: 12, y1: 20, x2: 12, y2: 4 }), h('line', { x1: 6, y1: 20, x2: 6, y2: 14 })),
  'Fault Diagnostics': h('svg', { width: 18, height: 18, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2 }, h('path', { d: 'M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z' }), h('line', { x1: 12, y1: 9, x2: 12, y2: 13 }), h('line', { x1: 12, y1: 17, x2: 12.01, y2: 17 })),
  'Loss Analysis': h('svg', { width: 18, height: 18, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2 }, h('polyline', { points: '23 18 13.5 8.5 8.5 13.5 1 6' }), h('polyline', { points: '17 18 23 18 23 12' })),
  Reports: h('svg', { width: 18, height: 18, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2, strokeLinecap: 'round', strokeLinejoin: 'round' }, h('path', { d: 'M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z' }), h('polyline', { points: '14 2 14 8 20 8' }), h('line', { x1: 8, y1: 13, x2: 16, y2: 13 }), h('line', { x1: 8, y1: 17, x2: 16, y2: 17 })),
  Guidebook: h('svg', { width: 18, height: 18, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2 }, h('path', { d: 'M4 19.5A2.5 2.5 0 0 1 6.5 17H20' }), h('path', { d: 'M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z' })),
  Metadata: h('svg', { width: 18, height: 18, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2 }, h('ellipse', { cx: 12, cy: 5, rx: 9, ry: 3 }), h('path', { d: 'M21 12c0 1.66-4 3-9 3s-9-1.34-9-3' }), h('path', { d: 'M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5' })),
  Admin: h('svg', { width: 18, height: 18, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2 }, h('circle', { cx: 12, cy: 12, r: 3 }), h('path', { d: 'M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 010 2.83 2 2 0 01-2.83 0l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-2 2 2 2 0 01-2-2v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83 0 2 2 0 010-2.83l.06-.06a1.65 1.65 0 00.33-1.82 1.65 1.65 0 00-1.51-1H3a2 2 0 01-2-2 2 2 0 012-2h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 010-2.83 2 2 0 012.83 0l.06.06a1.65 1.65 0 001.82.33H9a1.65 1.65 0 001-1.51V3a2 2 0 012-2 2 2 0 012 2v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 0 2 2 0 010 2.83l-.06.06a1.65 1.65 0 00-.33 1.82V9a1.65 1.65 0 001.51 1H21a2 2 0 012 2 2 2 0 01-2 2h-.09a1.65 1.65 0 00-1.51 1z' })),
};

const Chevron = ({ open }) => h('svg', { width: 16, height: 16, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2, strokeLinecap: 'round', strokeLinejoin: 'round', style: { transform: open ? 'rotate(90deg)' : 'rotate(0deg)', transition: 'transform 0.2s' } }, h('polyline', { points: '9 18 15 12 9 6' }));

const FAULT_DIAG_SIDEBAR = [
  { sub: 'overview', label: 'Overview' },
  { sub: 'ds', label: 'Disconnected Strings' },
  { sub: 'pl', label: 'Power Limitation' },
  { sub: 'is', label: 'Inverter Shutdown' },
  { sub: 'gb', label: 'Grid Breakdown' },
  { sub: 'comm', label: 'Communication Issue' },
  { sub: 'clip', label: 'Clipping' },
  { sub: 'derate', label: 'Derating' },
  { sub: 'scb_perf', label: 'Soiling' },
  { sub: 'inv_eff', label: 'Inverter Efficiency' },
  { sub: 'damage', label: 'ByPass Diode/Module Damage' },
];

function Sidebar({ page, faultSub, onNavigateFaultSub, onPageChange, user, onLogout, sidebarOpen }) {
  const [fdOpen, setFdOpen] = useState(page === 'Fault Diagnostics');
  useEffect(() => {
    if (page === 'Fault Diagnostics') setFdOpen(true);
  }, [page]);

  const PAGES = [
    { id: 'Dashboard', label: 'Dashboard' },
    { id: 'Analytics Lab', label: 'Analytics Lab' },
    { id: 'Fault Diagnostics', label: 'Fault Diagnostics', expandable: true },
    { id: 'Loss Analysis', label: 'Loss Analysis' },
    { id: 'Reports', label: 'Reports' },
    { id: 'Metadata', label: 'Metadata' },
  ];
  if (user?.is_admin) PAGES.push({ id: 'Admin', label: 'Admin' });

  /** Matches .nav-item grid: lead | icon | label | trail (stable width; mini-sidebar = icon-only child). */
  function navRowSimple(p, isActive) {
    if (!sidebarOpen) {
      return h('button', {
        key: p.id,
        type: 'button',
        className: `nav-item ${isActive ? 'active' : ''}`,
        onClick: () => onPageChange(p.id),
        title: p.label,
      }, h('span', { className: 'nav-icon-slot' }, NavIcons[p.id]));
    }
    return h('button', {
      key: p.id,
      type: 'button',
      className: `nav-item ${isActive ? 'active' : ''}`,
      onClick: () => onPageChange(p.id),
    },
      h('span', { className: 'nav-lead-slot', 'aria-hidden': true }),
      h('span', { className: 'nav-icon-slot' }, NavIcons[p.id]),
      h('span', { className: 'nav-item-label' }, p.label),
      h('span', { className: 'nav-item-trail', 'aria-hidden': true }),
    );
  }

  return h('div', { className: 'sidebar' },
    h('div', { className: 'sidebar-logo' },
      h('div', { className: 'logo-icon logo-icon-img-wrap' },
        h('img', {
          className: 'sidebar-logo-img',
          src: 'images/logo.png',
          alt: '',
          onError: function (e) {
            e.target.style.display = 'none';
            const s = e.target.nextSibling;
            if (s) s.style.display = 'flex';
          },
        }),
        h('span', { className: 'logo-sun-fallback', 'aria-hidden': true }, '☀'),
      ),
      sidebarOpen && h('div', { className: 'sidebar-logo-body' },
        h('h1', { className: 'sidebar-logo-title' }, 'Photon Intelligence Centre'),
        h('div', { className: 'sidebar-plant-dot', title: 'Plant Online' }),
      ),
    ),
    h('nav', { className: 'sidebar-nav', 'aria-label': 'Main navigation' },
      sidebarOpen && h('div', { className: 'nav-label' }, 'Main Menu'),
      PAGES.map((p) => {
        if (!p.expandable) {
          return navRowSimple(p, page === p.id);
        }
        const fdActive = page === 'Fault Diagnostics';
        if (!sidebarOpen) {
          return h('div', { key: p.id, className: 'nav-group-fault' },
            h('button', {
              type: 'button',
              className: `nav-item nav-item-parent ${fdActive ? 'active' : ''}`,
              title: 'Fault Diagnostics — expand to choose a section',
              onClick: () => setFdOpen((o) => !o),
            }, h('span', { className: 'nav-icon-slot' }, NavIcons[p.id])),
          );
        }
        return h('div', { key: p.id, className: fdOpen ? 'nav-group-fault nav-group-fault--open' : 'nav-group-fault' },
          h('button', {
            type: 'button',
            className: `nav-item nav-item-parent ${fdActive ? 'active' : ''}`,
            onClick: () => setFdOpen((o) => !o),
            'aria-expanded': fdOpen,
            ...(fdOpen ? { 'aria-controls': 'sidebar-fault-subnav' } : {}),
            title: 'Expand or collapse sections. Open Overview or another item below to go to Fault Diagnostics.',
          },
            h('span', { className: 'nav-lead-slot', 'aria-hidden': true }),
            h('span', { className: 'nav-icon-slot' }, NavIcons[p.id]),
            h('span', { className: 'nav-item-label' }, p.label),
            h('span', { className: 'nav-item-trail nav-item-trail--chevron', 'aria-hidden': true }, h(Chevron, { open: fdOpen })),
          ),
          fdOpen
            ? h('div', {
              id: 'sidebar-fault-subnav',
              className: 'nav-sublist',
              role: 'group',
              'aria-label': 'Fault Diagnostics sections',
            },
              FAULT_DIAG_SIDEBAR.map((child) => h('button', {
                type: 'button',
                key: child.sub,
                className: `nav-item-sub nav-item-sub-btn ${fdActive && faultSub === child.sub ? 'active' : ''}`,
                title: child.label,
                onClick: () => onNavigateFaultSub(child.sub),
              }, child.label)),
            )
            : null,
        );
      }),
      navRowSimple({ id: 'Guidebook', label: 'Guidebook' }, page === 'Guidebook'),
    ),
    h('div', { className: 'sidebar-footer' },
      sidebarOpen && h('div', { className: 'user-row', style: { display: 'flex', alignItems: 'center', gap: 10, padding: 12, borderTop: '1px solid rgba(255,255,255,0.06)' } },
        h('div', { className: 'user-avatar' }, (user?.full_name || 'U')[0].toUpperCase()),
        h('div', { className: 'user-info' }, h('div', { className: 'user-name' }, user?.full_name || 'Solar User')),
      ),
      h('button', { className: 'logout-btn', onClick: onLogout }, sidebarOpen ? 'Sign Out' : '↪'),
    ),
  );
}

function App() {
  const [user, setUser] = useState(null);
  const [authed, setAuthed] = useState(false);
  const [loading, setLoading] = useState(true);
  const [page, setPageState] = useState(() => parseLocationHash().page);
  const [faultSub, setFaultSub] = useState(() => parseLocationHash().faultSub);
  const [plants, setPlants] = useState([]);
  const [plantId, setPlantId] = useState(readStoredPlantId());
  const [theme, setTheme] = useState(localStorage.getItem('solar_theme') || 'dark');
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [dateFrom, setDateFrom] = useState(new Date(Date.now() - 7 * 864e5).toISOString().slice(0, 10));
  const [dateTo, setDateTo] = useState(new Date().toISOString().slice(0, 10));

  useLayoutEffect(() => {
    document.body.classList.toggle('theme-light', theme === 'light');
    document.body.classList.toggle('theme-vikram', theme === 'vikram');
  }, [theme]);

  const handleToggleSidebar = useCallback(() => setSidebarOpen(v => !v), []);
  const handlePageChange = useCallback((p) => { window.location.hash = PAGE_TO_HASH[p] || 'dashboard'; }, []);
  const handleNavigateFaultSub = useCallback((s) => { window.location.hash = `fault-diagnostics/${s}`; }, []);
  const handleLogout = () => { window.SolarAPI.clearToken(); setUser(null); setAuthed(false); };

  useEffect(() => {
    const sync = () => {
      const p = parseLocationHash();
      // Cancel any in-flight requests from the page we're leaving so they
      // don't land into state setters on an unmounted / replaced tree.
      if (typeof window.__abortRouteRequests === 'function') {
        try { window.__abortRouteRequests(); } catch (e) { /* noop */ }
      }
      setPageState(p.page);
      setFaultSub(p.faultSub);
    };
    window.addEventListener('hashchange', sync);
    return () => window.removeEventListener('hashchange', sync);
  }, []);

  useEffect(() => {
    window.SolarAPI.Auth.me().then(u => { setUser(u); setAuthed(true); }).catch(() => { }).finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (authed) window.SolarAPI.Plants.list().then(ps => { setPlants(ps); if (ps.length && !plantId) setPlantId(ps[0].plant_id); });
  }, [authed]);

  if (loading) return h('div', { style: { display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100vh', background: '#08111b' } }, 'Initializing...');
  if (!authed) return h(window.AuthPage, { onLogin: (u) => { setUser(u); setAuthed(true); } });

  return h('div', { className: `app-layout ${sidebarOpen ? '' : 'mini-sidebar'}` },
    h(Sidebar, { page, faultSub, onNavigateFaultSub: handleNavigateFaultSub, onPageChange: handlePageChange, user, onLogout: handleLogout, sidebarOpen }),
    h('div', { className: 'main-area' },
      h(Topbar, { page, plants, plantId, onPlantChange: (id) => { setPlantId(id); localStorage.setItem(PLANT_STORAGE_KEY, id); }, onAddPlant: () => { }, dateFrom, dateTo, onDateChange: (f, t) => { setDateFrom(f); setDateTo(t); }, user, theme, onThemeToggle: () => setTheme(t => t === 'dark' ? 'light' : 'dark'), onThemeSelect: setTheme, sidebarOpen, onToggleSidebar: handleToggleSidebar }),
      h('div', { className: 'page-content', key: page },
        page === 'Dashboard' && h(window.DashboardPage, { plantId, dateFrom, dateTo, onNavigate: handlePageChange }),
        page === 'Fault Diagnostics' && h(window.FaultPage, { plantId, dateFrom, dateTo, faultSub, onNavigateFaultSub: handleNavigateFaultSub }),
        page === 'Analytics Lab' && window.AnalyticsPage && h(window.AnalyticsPage, { plantId, dateFrom, dateTo, onNavigate: handlePageChange }),
        page === 'Loss Analysis' && window.LossAnalysisPage && h(window.LossAnalysisPage, { plantId, dateFrom, dateTo }),
        page === 'Reports' && window.ReportsPage && h(window.ReportsPage, { plantId, plants, dateFrom, dateTo }),
        page === 'Guidebook' && window.GuidebookPage && h(window.GuidebookPage, null),
        page === 'Metadata' && window.MetadataPage && h(window.MetadataPage, { plantId }),
        page === 'Admin' && user?.is_admin && window.AdminPage && h(window.AdminPage, null),
        page === 'Admin' && !user?.is_admin && h('div', { className: 'card', style: { padding: 24 } },
          h('h2', { style: { fontSize: 16, marginBottom: 8 } }, 'Administrator access required'),
          h('p', { style: { color: 'var(--text-soft)', fontSize: 13 } }, 'Your account does not have permission to open the Admin area.'),
        ),
      ),
    )
  );
}

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(h(App));
