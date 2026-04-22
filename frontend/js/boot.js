/*
 * frontend/js/boot.js
 * -------------------
 * Boot sequence:
 *   1. Wait for React / ReactDOM (from index.html).
 *   2. Load chart/icon CDN libs: PropTypes first (Recharts expects it), then
 *      Recharts + ECharts + Lucide in parallel to cut Vercel cold-boot latency.
 *   3. Load core app scripts (auth + dashboard path).
 *   4. theme_overrides.js (non-critical).
 *
 * Heavy route modules load on demand via window.__ensureRouteChunk(pageName).
 *
 * Diagnostics: localStorage.solar_perf_log = '1' logs phase timings.
 */

(function () {
  'use strict';

  var ASSET_BUILD_ID = 'phase5-vercel-cdn-20260423';

  function perfLog(label, t0) {
    try {
      if (localStorage.getItem('solar_perf_log') !== '1') return;
      var ms = (typeof performance !== 'undefined' && performance.now)
        ? (performance.now() - t0)
        : 0;
      console.info('[solar-perf]', label, Math.round(ms) + 'ms');
    } catch (e) { /* noop */ }
  }

  var _sv = (function () {
    try {
      var v = sessionStorage.getItem('_sv');
      if (!v) { v = Date.now().toString(36); sessionStorage.setItem('_sv', v); }
      return v;
    } catch (e) { return Date.now().toString(36); }
  })();
  var cacheBust = '?v=' + ASSET_BUILD_ID + '_' + _sv;

  // ECharts migration complete — Recharts + PropTypes removed to save ~200KB and 1-2s boot.
  var CDN_PARALLEL = [
    'https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js',
    'https://unpkg.com/lucide@0.487.0/dist/umd/lucide.min.js',
  ];

  function injectExternalScript(src) {
    return new Promise(function (resolve, reject) {
      var s = document.createElement('script');
      s.src = src;
      s.async = false;
      if (/unpkg\.com|jsdelivr\.net/.test(src)) {
        s.crossOrigin = 'anonymous';
      }
      s.onload = function () { resolve(); };
      s.onerror = function () { reject(new Error('failed to load ' + src)); };
      document.head.appendChild(s);
    });
  }

  function loadCdnChain() {
    // Load ECharts + Lucide in parallel (no sequential PropTypes dependency)
    return Promise.all(CDN_PARALLEL.map(function (url) {
      return injectExternalScript(url);
    }));
  }

  var CORE_MODULES = [
    'js/api.js',
    'js/ui_utils.js',
    'js/components.js',
    'js/echart_wrapper.js',
    'js/dashboard_target.js',
    'js/pages.js',
    'js/app.js',
  ];

  var ROUTE_CHUNKS = {
    'Analytics Lab': ['js/analytics_page_override.js'],
    'Fault Diagnostics': ['js/inv_eff_analysis.js', 'js/fault_page.js'],
    'Loss Analysis': ['js/loss_analysis.js'],
    'Reports': ['js/reports_page.js'],
    'Guidebook': ['js/guidebook_page.js'],
    'Metadata': ['js/plant_architecture_viz.js'],
    'Admin': ['js/admin_page.js'],
    'AdminPerf': ['js/perf_admin.js'],
  };

  var _loaded = Object.create(null);
  var _fetchedText = Object.create(null);
  var _chunkPromises = Object.create(null);

  function fetchScriptText(src) {
    if (_fetchedText[src]) return _fetchedText[src];
    _fetchedText[src] = fetch(src + cacheBust, { credentials: 'same-origin' })
      .then(function (res) {
        if (!res.ok) throw new Error('failed to fetch ' + src + ' (' + res.status + ')');
        return res.text();
      });
    return _fetchedText[src];
  }

  function loadScript(src) {
    if (_loaded[src]) return _loaded[src];
    _loaded[src] = fetchScriptText(src).then(function (code) {
      return new Promise(function (resolve, reject) {
        var s = document.createElement('script');
        s.text = [
          '(function () {',
          code,
          '\n})();',
          '\n//# sourceURL=' + src
        ].join('\n');
        s.onload = function () { resolve(src); };
        s.onerror = function (e) {
          console.error('Boot error loading', src, e);
          reject(new Error('failed to execute ' + src));
        };
        try {
          document.head.appendChild(s);
          resolve(src);
        } catch (err) {
          reject(err);
        }
      });
    });
    return _loaded[src];
  }

  function loadAllOrdered(srcs) {
    var fetched = srcs.map(fetchScriptText);
    return fetched.reduce(function (chain, _promise, idx) {
      return chain.then(function () {
        return loadScript(srcs[idx]);
      });
    }, Promise.resolve());
  }

  function showBootError(err) {
    console.error('Boot failed:', err);
    var root = document.getElementById('root');
    if (!root) return;
    root.innerHTML =
      '<div class="auth-page auth-initial-load" style="min-height:100vh;display:flex;align-items:center;justify-content:center;background:linear-gradient(180deg,#08111b 0%,#091421 100%);padding:24px;">' +
        '<div style="max-width:560px;width:100%;background:rgba(10,18,28,0.92);border:1px solid rgba(255,255,255,0.08);border-radius:18px;padding:24px;color:#e6edf3;font-family:Inter,Arial,sans-serif;">' +
          '<h2 style="margin:0 0 10px;font-size:20px;">Frontend boot failed</h2>' +
          '<p style="margin:0;color:#9fb0c0;font-size:13px;line-height:1.5;">The page scripts did not finish starting. Refresh once. If it still fails, open DevTools and check the console.</p>' +
          '<pre style="margin:14px 0 0;padding:12px;border-radius:12px;background:#08111b;color:#fda4af;white-space:pre-wrap;font-size:12px;overflow:auto;">' + String(err && err.message ? err.message : err) + '</pre>' +
        '</div>' +
      '</div>';
  }

  window.__ensureRouteChunk = function (page) {
    var list = ROUTE_CHUNKS[page];
    if (!list || !list.length) return Promise.resolve();
    if (_chunkPromises[page]) return _chunkPromises[page];
    var t0 = (typeof performance !== 'undefined' && performance.now) ? performance.now() : 0;
    _chunkPromises[page] = loadAllOrdered(list).then(function () {
      perfLog('route-chunk:' + page, t0);
      return page;
    });
    return _chunkPromises[page];
  };

  window.__loadModule = function (name) {
    var list = ROUTE_CHUNKS[name];
    if (!list) return Promise.resolve();
    return loadAllOrdered(list);
  };

  function whenReact() {
    return new Promise(function (resolve) {
      if (window.React && window.ReactDOM) return resolve();
      var tries = 0;
      var id = setInterval(function () {
        tries++;
        if (window.React && window.ReactDOM) {
          clearInterval(id);
          resolve();
        } else if (tries > 200) {
          clearInterval(id);
          console.error('Boot: React did not load after 10 s');
          resolve();
        }
      }, 50);
    });
  }

  var _bootT0 = (typeof performance !== 'undefined' && performance.now) ? performance.now() : 0;
  whenReact()
    .then(function () {
      perfLog('react-ready', _bootT0);
      return loadCdnChain();
    })
    .then(function () {
      perfLog('cdn-charts-ready', _bootT0);
      return loadAllOrdered(CORE_MODULES);
    })
    .then(function () {
      perfLog('core-modules-ready', _bootT0);
      return loadScript('js/theme_overrides.js');
    })
    .then(function () {
      perfLog('boot-complete', _bootT0);
      try {
        window.dispatchEvent(new CustomEvent('solar-boot-complete'));
      } catch (e) { /* noop */ }
    })
    .catch(showBootError);
})();
