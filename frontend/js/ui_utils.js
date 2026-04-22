// frontend/js/ui_utils.js
// ─────────────────────────────────────────────────────────────────────────────
// Premium UI utilities:
//   window.Toast              — slide-in toast notifications (replaces alert())
//   window.AnimatedNumber     — count-up animation React component
//   window.DatePresetPicker   — "Today / 7D / 30D / Month" preset buttons
// ─────────────────────────────────────────────────────────────────────────────

/* ── Toast system (DOM-based so it lives outside React tree) ─────────────── */
(function initToastSystem() {
  // Create container div and append to body.
  // This runs before React mounts so we append to body directly.
  const container = document.createElement('div');
  container.className = 'toast-container';
  container.id = 'solar-toast-container';
  document.body.appendChild(container);

  let _id = 0;

  const ICONS = { success: '✓', error: '✕', info: 'i', warn: '!' };

  function showToast(type, title, detail, duration) {
    const id = ++_id;
    const ms  = duration || (type === 'error' ? 6000 : 4000);

    const el = document.createElement('div');
    el.className = 'toast toast-' + type;
    el.id = 'toast-' + id;
    el.setAttribute('role', 'alert');
    el.setAttribute('aria-live', 'polite');

    el.innerHTML =
      '<div class="toast-icon">' + (ICONS[type] || 'i') + '</div>' +
      '<div class="toast-body">' +
        '<div class="toast-title">' + title + '</div>' +
        (detail ? '<div class="toast-message">' + detail + '</div>' : '') +
      '</div>';

    el.addEventListener('click', function() { dismiss(id); });
    container.appendChild(el);

    const t = setTimeout(function() { dismiss(id); }, ms);
    el._toastTimer = t;

    return id;
  }

  function dismiss(id) {
    const el = document.getElementById('toast-' + id);
    if (!el) return;
    clearTimeout(el._toastTimer);
    el.classList.add('toast-exit');
    setTimeout(function() {
      try { el.parentNode && el.parentNode.removeChild(el); } catch(e) {}
    }, 320);
  }

  window.Toast = {
    success : function(title, detail) { return showToast('success', title, detail || ''); },
    error   : function(title, detail) { return showToast('error',   title, detail || ''); },
    info    : function(title, detail) { return showToast('info',    title, detail || ''); },
    warn    : function(title, detail) { return showToast('warn',    title, detail || ''); },
    dismiss : function(id)            { dismiss(id); },
  };
})();


/* ── AnimatedNumber React component ─────────────────────────────────────── */
//
//  Usage:
//    h(window.AnimatedNumber, { to: 1234.5, decimals: 1 })
//
//  Props:
//    to        {number|string}  Target value (parsed as float)
//    decimals  {number}         Decimal places (default 1)
//    duration  {number}         Animation ms (default 900)
//
window.AnimatedNumber = (function() {
  var useState  = React.useState;
  var useEffect = React.useEffect;
  var useRef    = React.useRef;
  var h         = React.createElement;

  return function AnimatedNumber(props) {
    var to       = props.to;
    var decimals = props.decimals != null ? props.decimals : 1;
    var duration = props.duration != null ? props.duration : 900;

    var prevRef = useRef(null);
    var rafRef  = useRef(null);
    var pair    = useState('—');
    var display = pair[0];
    var setDisplay = pair[1];

    useEffect(function() {
      // Parse target
      var target;
      if (to == null || to === '' || to === '-' || to === '—') {
        setDisplay('—');
        prevRef.current = null;
        return;
      }
      target = parseFloat(to);
      if (isNaN(target)) {
        setDisplay(String(to));
        return;
      }

      var from = prevRef.current != null ? prevRef.current : 0;
      prevRef.current = target;

      if (rafRef.current) cancelAnimationFrame(rafRef.current);

      var startTime = null;
      var diff = target - from;

      function tick(ts) {
        if (!startTime) startTime = ts;
        var elapsed  = ts - startTime;
        var progress = Math.min(elapsed / duration, 1);
        // easeOutCubic
        var ease     = 1 - Math.pow(1 - progress, 3);
        var cur      = from + diff * ease;
        setDisplay(cur.toFixed(decimals));
        if (progress < 1) {
          rafRef.current = requestAnimationFrame(tick);
        } else {
          setDisplay(target.toFixed(decimals));
        }
      }

      rafRef.current = requestAnimationFrame(tick);
      return function() {
        if (rafRef.current) cancelAnimationFrame(rafRef.current);
      };
    }, [to, decimals, duration]);

    return h('span', { className: 'kpi-value-animated' }, display);
  };
})();


/* ── DatePresetPicker React component ───────────────────────────────────── */
//
//  Usage:
//    h(window.DatePresetPicker, { dateFrom, dateTo, onDateChange })
//
//  onDateChange(newFrom, newTo) is called when a preset is clicked.
//
window.DatePresetPicker = (function() {
  var h       = React.createElement;
  var useMemo = React.useMemo;

  function toISO(d) { return d.toISOString().slice(0, 10); }

  function makePresets() {
    var now       = new Date();
    var today     = toISO(now);
    var d7        = new Date(now); d7.setDate(d7.getDate() - 6);
    var d30       = new Date(now); d30.setDate(d30.getDate() - 29);
    var monthStart= new Date(now.getFullYear(), now.getMonth(), 1);
    return [
      { label: 'Today', from: today,           to: today },
      { label: '7D',    from: toISO(d7),        to: today },
      { label: '30D',   from: toISO(d30),       to: today },
      { label: 'Month', from: toISO(monthStart),to: today },
    ];
  }

  return function DatePresetPicker(props) {
    var dateFrom     = props.dateFrom;
    var dateTo       = props.dateTo;
    var onDateChange = props.onDateChange;

    var presets = useMemo(makePresets, []);

    var active = useMemo(function() {
      for (var i = 0; i < presets.length; i++) {
        if (presets[i].from === dateFrom && presets[i].to === dateTo) {
          return presets[i].label;
        }
      }
      return null;
    }, [dateFrom, dateTo, presets]);

    return h('div', { className: 'date-presets' },
      presets.map(function(p) {
        return h('button', {
          key:       p.label,
          type:      'button',
          className: 'date-preset-btn' + (active === p.label ? ' active' : ''),
          title:     p.from + ' \u2192 ' + p.to,
          onClick:   function() { onDateChange(p.from, p.to); },
        }, p.label);
      })
    );
  };
})();
