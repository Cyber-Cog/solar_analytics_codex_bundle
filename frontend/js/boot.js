/*
 * frontend/js/boot.js
 * -------------------
 * Minimal boot loader.
 *
 *   1. Wait for the CDN libraries (React / ReactDOM / Recharts / ECharts) to
 *      finish downloading — they are all loaded with `defer` in index.html,
 *      so once DOMContentLoaded fires we only need to poll briefly for the
 *      globals to appear.
 *   2. Load all app modules in parallel via plain <script src> tags (NOT fetch
 *      + eval, which blocks the browser's parser and disables dev-tools source
 *      mapping).
 *   3. Route-aware lazy chunks (fault_page.js, loss_analysis.js, etc.) are
 *      loaded on first route entry; see app.js for the trigger.
 *
 * The old boot loader also installed a body-wide MutationObserver that
 * re-tagged modal elements on every DOM mutation. That observer has been
 * removed — modal components set their own className directly now (see
 * components.js).
 */

(function () {
  'use strict';

  var ASSET_BUILD_ID = 'phase4-boot-20260421-hotfix1';

  // Session-scoped cache buster: resets on hard reload, sticks across SPA navs.
  var _sv = (function () {
    try {
      var v = sessionStorage.getItem('_sv');
      if (!v) { v = Date.now().toString(36); sessionStorage.setItem('_sv', v); }
      return v;
    } catch (e) { return Date.now().toString(36); }
  })();
  var cacheBust = '?v=' + ASSET_BUILD_ID + '_' + _sv;

  // Modules loaded at startup. For compatibility with the existing app.js
  // (which references page components as top-level globals) we keep the full
  // list here. Once app.js is refactored to use the __loadModule() hook on
  // route entry, heavy modules can move into LAZY_MODULES.
  var CORE_MODULES = [
    'js/api.js',
    'js/ui_utils.js',
    'js/components.js',
    'js/echart_wrapper.js',
    'js/dashboard_target.js',
    'js/pages.js',
    'js/plant_architecture_viz.js',
    'js/analytics_page_override.js',
    'js/loss_analysis.js',
    'js/inv_eff_analysis.js',
    'js/fault_page.js',
    'js/guidebook_page.js',
    'js/reports_page.js',
    'js/admin_page.js',
    'js/app.js'
  ];

  // Reserved for the next iteration: route-specific chunks.
  var LAZY_MODULES = {
    /* Example entry:
    'fault-charts': ['js/fault_charts.js'],
    */
  };

  var _loaded = Object.create(null);
  var _fetchedText = Object.create(null);

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
        // Legacy page files share many top-level `const` names. Wrapping every
        // file in its own IIFE preserves the old isolation semantics while
        // keeping deliberate globals such as `window.AuthPage`.
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

  // Load a list in order-preserving parallel: all start downloading at once,
  // but the browser runs them in the sequence we requested. Resolves when the
  // last one finishes.
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

  // Expose a helper for app.js to lazy-load a feature bundle:
  //     await window.__loadModule('fault')
  window.__loadModule = function (name) {
    var list = LAZY_MODULES[name];
    if (!list) return Promise.resolve();
    return loadAllOrdered(list);
  };

  // Wait for React / ReactDOM to appear (the CDN scripts are deferred; once
  // DOMContentLoaded has fired they are guaranteed to run soon after).
  function whenReact() {
    return new Promise(function (resolve) {
      if (window.React && window.ReactDOM) return resolve();
      var tries = 0;
      var id = setInterval(function () {
        tries++;
        if (window.React && window.ReactDOM) {
          clearInterval(id);
          resolve();
        } else if (tries > 200) {   // ~10 s
          clearInterval(id);
          console.error('Boot: React did not load after 10 s');
          resolve();
        }
      }, 50);
    });
  }

  whenReact().then(function () {
    return loadAllOrdered(CORE_MODULES);
  }).then(function () {
    // theme_overrides waits for the named React components (AuthPage, Card,
    // Spinner) to exist before wrapping them — it is small and non-critical.
    return loadScript('js/theme_overrides.js');
  }).catch(showBootError);
})();
