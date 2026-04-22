// React wrapper around window.echarts for this UMD setup.
//
// Key invariants this component enforces (each one was an actual bug before):
//
//   * init / dispose happen EXACTLY ONCE per mount. The previous version
//     depended on [option, theme, onEvents, resizeWithWindow] so every render
//     — e.g. a parent that rebuilt `option` in its render body without
//     useMemo — caused a full dispose + re-init, throwing away the GPU
//     context and any hover / zoom state. Now init runs only on mount and
//     dispose only on unmount.
//
//   * setOption runs on every `option` change, inside its own effect, so
//     updates are cheap.
//
//   * onEvents handlers are reattached when the handler identity changes,
//     without re-initialising the chart.
//
//   * The container is observed with ResizeObserver (in addition to the
//     window `resize` listener) so the chart resizes when its parent is
//     resized — e.g. when a collapsible sidebar opens.
//
// Usage:
//   h(window.EChart, { option, style: { height: 300 }, onEvents: { click: fn } })
//
// Tip: wrap the `option` object in useMemo in the parent to avoid wasted
// setOption calls. This wrapper is now forgiving of that mistake (no more
// dispose/reinit thrash), but setOption on a 10k-point chart is still ~10ms
// of main-thread time you want to skip when nothing has changed.

const { useRef, useEffect } = React;

window.EChart = function EChart(props) {
  const h = React.createElement;
  const { option, theme, style, className, onEvents, resizeWithWindow = true } = props;
  const domRef = useRef(null);
  const chartRef = useRef(null);
  const boundEventsRef = useRef(null);
  const resizeObserverRef = useRef(null);

  // 1. INIT on mount, DISPOSE on unmount. No other triggers.
  useEffect(() => {
    if (!domRef.current || !window.echarts) return undefined;

    const chart = window.echarts.init(domRef.current, theme || null);
    chartRef.current = chart;

    let handleWindowResize;
    if (resizeWithWindow) {
      handleWindowResize = () => { try { chart.resize(); } catch (e) { /* noop */ } };
      window.addEventListener('resize', handleWindowResize);
    }

    // Resize on container size changes (sidebars, tab switches, modal layouts).
    try {
      if (typeof ResizeObserver !== 'undefined') {
        resizeObserverRef.current = new ResizeObserver(() => {
          try { chart.resize(); } catch (e) { /* noop */ }
        });
        resizeObserverRef.current.observe(domRef.current);
      }
    } catch (e) { /* noop */ }

    return () => {
      if (handleWindowResize) window.removeEventListener('resize', handleWindowResize);
      if (resizeObserverRef.current) {
        try { resizeObserverRef.current.disconnect(); } catch (e) { /* noop */ }
        resizeObserverRef.current = null;
      }
      // Detach any bound event handlers before dispose
      if (chartRef.current && boundEventsRef.current) {
        try {
          Object.keys(boundEventsRef.current).forEach((evt) => {
            try { chartRef.current.off(evt); } catch (e) { /* noop */ }
          });
        } catch (e) { /* noop */ }
        boundEventsRef.current = null;
      }
      if (chartRef.current) {
        try { chartRef.current.dispose(); } catch (e) { /* noop */ }
        chartRef.current = null;
      }
    };
    // Intentional: theme / resizeWithWindow are read once at mount.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // 2. Apply `option` updates. Cheap, no re-init.
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart || !option) return;
    try {
      chart.setOption(option, { notMerge: false, lazyUpdate: true, replaceMerge: ['series'] });
    } catch (e) { /* noop */ }
  }, [option]);

  // 3. Bind / rebind event handlers.
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return undefined;
    // Detach any prior bindings
    if (boundEventsRef.current) {
      Object.keys(boundEventsRef.current).forEach((evt) => {
        try { chart.off(evt); } catch (e) { /* noop */ }
      });
    }
    if (onEvents) {
      Object.keys(onEvents).forEach((evt) => {
        try { chart.on(evt, onEvents[evt]); } catch (e) { /* noop */ }
      });
    }
    boundEventsRef.current = onEvents || null;
    return undefined;
  }, [onEvents]);

  return h('div', {
    ref: domRef,
    className: className || '',
    style: Object.assign({ width: '100%', height: 300 }, style || {}),
  });
};
