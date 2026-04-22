/*
 * frontend/js/theme_overrides.js
 * ------------------------------
 * Replacement for the body-wide MutationObserver that used to live in
 * index.html. That observer traversed every mutation subtree on the page
 * looking for <div style="background: white"> modal elements — on a data-
 * heavy page like Fault Detection this fired thousands of times per second.
 *
 * The same effect is now achieved with a one-time wrap at startup: once the
 * core components exist we install a className-applying wrapper on AuthPage
 * so the premium theme can override it. Modal components add the
 * `theme-fault-modal` / `theme-fault-overlay` classes themselves (see
 * components.js) instead of relying on style-attribute snooping.
 *
 * No ongoing DOM observation — zero runtime cost.
 */

(function () {
  'use strict';

  function ready() {
    try {
      if (!window.AuthPage) return setTimeout(ready, 50);

      var h = React.createElement;
      var Original = window.AuthPage;
      window.AuthPage = function WrappedAuthPage(props) {
        return h(Original, props);
      };
    } catch (e) {
      console.warn('Theme overrides:', e);
    }
  }

  ready();
})();
