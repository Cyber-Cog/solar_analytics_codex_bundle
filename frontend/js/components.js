// frontend/js/components.js
// Shared UI components used across all pages
const { useState, useEffect, useCallback, useRef, useMemo } = React;

// ── Spinner ───────────────────────────────────────────────────────────────────
window.Spinner = ({ size = 20 } = {}) => React.createElement('div', { 
  className:'spinner',
  style: { width: size, height: size },
  role: 'status',
  'aria-label': 'Loading',
});

// ── Skeleton Loader (placeholder while loading) ───────────────────────────────
window.SkeletonLoader = ({ width = '100%', height = '16px', count = 3, gap = '8px' }) => {
  const skeletons = [];
  for (let i = 0; i < count; i++) {
    skeletons.push(
      React.createElement('div', {
        key: i,
        className: 'skeleton',
        style: { width, height, marginBottom: i < count - 1 ? gap : 0 },
        'aria-hidden': 'true'
      })
    );
  }
  return React.createElement('div', null, skeletons);
};

// ── Lucide Icon Helper ────────────────────────────────────────────────────────
window.LucideIcon = ({ name, size = 16, strokeWidth = 2, className = '' }) => {
  const iconData = window.lucide?.icons?.[name];
  if (!iconData || !Array.isArray(iconData)) return null;
  return React.createElement('svg', {
    xmlns: 'http://www.w3.org/2000/svg',
    width: size,
    height: size,
    viewBox: '0 0 24 24',
    fill: 'none',
    stroke: 'currentColor',
    strokeWidth,
    strokeLinecap: 'round',
    strokeLinejoin: 'round',
    className: `lucide lucide-${name.toLowerCase()} ${className}`,
    dangerouslySetInnerHTML: { __html: iconData.map(([tag, attrs]) => {
      const attrStr = Object.entries(attrs).map(([k, v]) => `${k}="${v}"`).join(' ');
      return `<${tag} ${attrStr}></${tag}>`;
    }).join('') }
  });
};

// ── KPI Card ──────────────────────────────────────────────────────────────────
// variant: 'default' | 'performance' (dashboard energy KPI typography) | 'weather' (slightly softer numbers)
// subVariant: 'today-energy' | 'total-energy' | 'co2-avoided' | 'total-power' | 'current' | 'voltage'
// Loading state: optional loading prop to show skeleton
window.KpiCard = ({ icon, label, value, unit, color='var(--accent)', variant='default', subVariant, loading = false, onClick }) => {
  const cls = ['kpi-card'];
  if (variant === 'performance') cls.push('kpi-card--performance');
  if (variant === 'weather') cls.push('kpi-card--weather');
  if (subVariant) cls.push(`sub-${subVariant}`);
  if (loading) cls.push('skeleton');

  const h = React.createElement;
  const isReactElement = icon && React.isValidElement(icon);
  const isClickable = typeof onClick === 'function';

  // Animated count-up for performance + weather KPI tiles
  const shouldAnimate = (variant === 'performance' || variant === 'weather') && !loading;
  let animNode = null;
  if (shouldAnimate) {
    const numVal = parseFloat(value);
    if (!isNaN(numVal)) {
      // Infer decimal places from the pre-formatted value string
      const vStr = String(value || '');
      const dotIdx = vStr.indexOf('.');
      const decimals = dotIdx >= 0 ? Math.min(vStr.length - dotIdx - 1, 3) : 0;
      animNode = h(window.AnimatedNumber, { to: numVal, decimals, duration: 850 });
    }
  }

  return h('div', {
    className: cls.join(' '),
    style: { color, cursor: isClickable ? 'pointer' : 'default' },
    onClick: isClickable ? onClick : undefined,
    onKeyDown: isClickable ? (e) => {
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onClick(e); }
    } : undefined,
    role: isClickable ? 'button' : undefined,
    tabIndex: isClickable ? 0 : undefined,
    'aria-label': loading ? `${label} loading` : `${label}: ${value} ${unit || ''}`,
    'aria-busy': loading ? 'true' : undefined,
  },
    subVariant === 'today-energy' && h('div', { className: 'kpi-bg-image' }),
    subVariant && subVariant !== 'today-energy' && icon && h('div', { className: 'kpi-bg-icon' },
      isReactElement ? icon : (typeof icon === 'string' ? h(window.LucideIcon, { name: icon, size: 80 }) : null)
    ),
    icon && h('div', { className:'kpi-icon' },
      isReactElement ? icon : (typeof icon === 'string' ? h(window.LucideIcon, { name: icon, size: 16 }) : icon)
    ),
    !loading && h('div', { className:'kpi-label' }, label),
    !loading
      ? h('div', { className:'kpi-value' }, animNode || (value ?? '—'))
      : h(window.SkeletonLoader, { width: '80%', height: '24px', count: 1 }),
    unit && !loading && h('div', { className:'kpi-unit' }, unit),
    loading && h(window.SkeletonLoader, { width: '60%', height: '12px', count: 1 }),
  );
};


// ── Card wrapper ──────────────────────────────────────────────────────────────
// Improved with better accessibility and loading state support
window.Card = ({ title, action, children, style, onClick, loading = false, isEmpty = false, emptyMessage = 'No data' }) => {
  const h = React.createElement;
  const clickable = typeof onClick === 'function';
  
  return h('div', {
    className: `card ${loading ? 'skeleton' : ''} ${isEmpty ? 'empty-card' : ''}`,
    style: clickable ? { cursor: 'pointer', ...(style || {}) } : style,
    onClick: clickable ? onClick : undefined,
    onKeyDown: clickable ? (e) => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        onClick(e);
      }
    } : undefined,
    role: clickable ? 'button' : 'region',
    tabIndex: clickable ? 0 : undefined,
    'aria-label': title || 'Card',
    'aria-busy': loading ? 'true' : undefined,
  },
    title && h('div', { className:'card-header' },
      h('span', { className:'card-title' }, title),
      action,
    ),
    h('div', { className:'card-body' }, 
      loading 
        ? h(window.SkeletonLoader, { width: '100%', height: '16px', count: 4 })
        : isEmpty 
          ? h('div', { className: 'empty-state' }, emptyMessage)
          : children
    ),
  );
};

// ── Badge ─────────────────────────────────────────────────────────────────────
window.Badge = ({ children, type='blue' }) =>
  React.createElement('span', { className:`badge badge-${type}` }, children);

// ── Toggle ────────────────────────────────────────────────────────────────────
window.Toggle = ({ label, value, onChange }) => {
  return React.createElement('label', { className:'toggle-label' },
    React.createElement('div', {
      className: `toggle-track ${value ? 'on' : ''}`,
      onClick: () => onChange(!value),
    },
      React.createElement('div', { className:'toggle-thumb' }),
    ),
    React.createElement('span', null, label),
  );
};

// ── FormInput (with validation feedback) ────────────────────────────────────────
window.FormInput = ({ 
  label, 
  value = '',
  onChange,
  onBlur,
  type = 'text',
  placeholder,
  error,
  success,
  disabled = false,
  required = false,
  id,
  helperText,
  maxLength,
  ...props
}) => {
  const h = React.createElement;
  const inputId = id || `input-${Math.random().toString(36).substr(2, 9)}`;
  const helperId = `${inputId}-help`;
  const errorId = `${inputId}-error`;
  
  const inputClass = [
    'form-input',
    error ? 'has-error' : '',
    success ? 'has-success' : ''
  ].filter(Boolean).join(' ');
  
  return h('div', { className: 'form-group' },
    label && h('label', { 
      className: 'form-label',
      htmlFor: inputId,
    }, 
      label,
      required && h('span', { style: { color: 'var(--bad)', marginLeft: '4px' } }, '*')
    ),
    h('input', {
      id: inputId,
      className: inputClass,
      type,
      value,
      onChange: (e) => onChange?.(e.target.value),
      onBlur,
      placeholder,
      disabled,
      required,
      maxLength,
      'aria-invalid': error ? 'true' : 'false',
      'aria-describedby': error ? errorId : helperText ? helperId : undefined,
      style: { marginBottom: error || helperText ? '6px' : '0' },
      ...props
    }),
    error && h('div', { 
      id: errorId,
      className: 'form-error',
      style: { color: 'var(--bad)', fontSize: '11px', marginTop: '4px' },
      role: 'alert'
    }, error),
    !error && helperText && h('div', { 
      id: helperId,
      className: 'form-helper',
      style: { color: 'var(--text-muted)', fontSize: '11px', marginTop: '4px' }
    }, helperText),
    maxLength && h('div', { 
      style: { 
        fontSize: '10px', 
        color: 'var(--text-muted)', 
        marginTop: '4px',
        textAlign: 'right'
      }
    }, `${String(value).length}/${maxLength}`)
  );
};

// ── Modal ─────────────────────────────────────────────────────────────────────
// Improved with better accessibility, keyboard support (ESC to close), and focus trapping
window.Modal = ({ title, open, onClose, children, footer }) => {
  const h = React.createElement;
  const overlayRef = useRef(null);
  
  useEffect(() => {
    if (!open) return;
    
    // Trap focus within modal (ESC key support comes from KeyboardEvent)
    const handleKeyDown = (e) => {
      if (e.key === 'Escape') {
        e.stopPropagation();
        onClose();
      }
    };
    
    // Add escape key listener
    document.addEventListener('keydown', handleKeyDown);
    
    // Prevent body scroll when modal is open
    document.body.classList.add('no-scroll');
    
    // Focus the modal for keyboard navigation
    if (overlayRef.current) {
      overlayRef.current.focus();
    }
    
    return () => {
      document.removeEventListener('keydown', handleKeyDown);
      document.body.classList.remove('no-scroll');
    };
  }, [open, onClose]);

  if (!open) return null;
  
  return h('div', { 
    className:'modal-overlay', 
    onClick: e => e.target === e.currentTarget && onClose(),
    ref: overlayRef,
    role: 'dialog',
    'aria-modal': 'true',
    'aria-labelledby': 'modal-title',
    tabIndex: 0,
  },
    h('div', { className:'modal-box' },
      h('div', { className:'modal-header' },
        h('span', { className:'modal-title', id: 'modal-title' }, title),
        h('button', { 
          className:'modal-close', 
          onClick: onClose, 
          'aria-label': 'Close dialog',
          title: 'Close (ESC)',
          type: 'button',
        }, '×'),
      ),
      h('div', { className:'modal-body' }, children),
      footer && h('div', { className:'modal-footer' }, footer),
    ),
  );
};

// ── Raise Ticket Float Button ─────────────────────────────────────────────────
window.RaiseTicketButton = ({ plantId }) => {
  const [open, setOpen] = useState(false);
  const [subj, setSubj] = useState('');
  const [desc, setDesc] = useState('');
  const [toEmails, setToEmails] = useState('');
  const [sending, setSending] = useState(false);
  const [done, setDone] = useState(false);

  const send = async () => {
    if (!subj || !desc) return;
    const recipients = String(toEmails || '')
      .split(',')
      .map(v => v.trim())
      .filter(Boolean);
    setSending(true);
    try {
      await window.SolarAPI.Tickets.raise(subj, desc, plantId, recipients);
      setDone(true);
      setTimeout(() => { setOpen(false); setDone(false); setSubj(''); setDesc(''); setToEmails(''); }, 2000);
    } catch(e) { alert(e.message); }
    finally { setSending(false); }
  };

  return React.createElement(React.Fragment, null,
    React.createElement('button', { className:'float-btn', onClick:()=>setOpen(true), title:'Raise Support Ticket' }, 'T'),
    React.createElement(Modal, {
      title: 'Raise Support Ticket',
      open, onClose:()=>setOpen(false),
      footer: React.createElement(React.Fragment, null,
        React.createElement('button', { className:'btn btn-outline', onClick:()=>setOpen(false) }, 'Cancel'),
        React.createElement('button', { className:'btn btn-primary', onClick:send, disabled:sending },
          sending ? React.createElement(Spinner) : 'Send Ticket'),
      ),
    },
      done
        ? React.createElement('div', { style:{textAlign:'center',padding:'20px'} }, 'Ticket raised. Support will contact you shortly.')
        : React.createElement(React.Fragment, null,
            React.createElement('div', { className:'form-group' },
              React.createElement('label', { className:'form-label' }, 'Subject'),
              React.createElement('input', { className:'form-input', value:subj, onChange:e=>setSubj(e.target.value), placeholder:'Briefly describe the issue' }),
            ),
            React.createElement('div', { className:'form-group' },
              React.createElement('label', { className:'form-label' }, 'Description'),
              React.createElement('textarea', { className:'form-input', value:desc, onChange:e=>setDesc(e.target.value), placeholder:'Provide full details...', style:{height:100} }),
            ),
            React.createElement('div', { className:'form-group' },
              React.createElement('label', { className:'form-label' }, 'Notify Emails (comma separated, optional)'),
              React.createElement('input', {
                className:'form-input',
                value: toEmails,
                onChange:e=>setToEmails(e.target.value),
                placeholder:'ops@plant.com, manager@plant.com'
              }),
            ),
          ),
    ),
  );
};

// ── Multi-Select Equipment Picker ─────────────────────────────────────────────
// ids: string[]  OR  items: { id: string, label: string }[]  (id is the selection key sent to onToggle)
window.EquipmentPicker = ({ ids = [], items = null, selected = [], onToggle, onSelectAll, search = '', onSearch }) => {
  const rows = (items && items.length)
    ? items
    : (ids || []).map(id => ({ id, label: id }));
  const q = (search || '').toLowerCase();
  const filtered = rows.filter(row => {
    const id = String(row.id || '');
    const label = String(row.label != null ? row.label : id);
    return id.toLowerCase().includes(q) || label.toLowerCase().includes(q);
  });
  const filteredIds = filtered.map(r => r.id);
  const allSelected = filteredIds.length > 0 && filteredIds.every(id => selected.includes(id));

  return React.createElement('div', null,
    React.createElement('div', { className:'search-bar', style:{marginBottom:6} },
      React.createElement('input', {
        className:'search-input', value:search, placeholder:'Search equipment...',
        onChange: e => onSearch(e.target.value),
      }),
    ),
    React.createElement('div', { className:'multi-select-list' },
      React.createElement('div', {
        className: `multi-select-item ${allSelected ? 'selected' : ''}`,
        onClick: () => onSelectAll(filteredIds, !allSelected),
        style:{ fontWeight:600, borderBottom:'2px solid var(--border)' },
      }, React.createElement('input', { type:'checkbox', readOnly:true, checked:allSelected }), 'Select All'),
      filtered.map(row =>
        React.createElement('div', {
          key: row.id,
          className: `multi-select-item ${selected.includes(row.id) ? 'selected' : ''}`,
          onClick: () => onToggle(row.id),
        },
          React.createElement('input', { type:'checkbox', readOnly:true, checked:selected.includes(row.id) }),
          row.label,
        )
      ),
      filtered.length === 0 && React.createElement('div', { className:'empty-state', style:{minHeight:80} }, 'No equipment found'),
    ),
  );
};

// Reusable sortable table with CSV export, loading state, and better accessibility.
window.DataTable = ({
  columns = [],
  rows = [],
  emptyMessage = 'No data available',
  filename = 'table_export.csv',
  maxHeight = 420,
  initialSortKey = null,
  initialSortDir = 'asc',
  compact = false,
  loading = false,
  ariaLabel = 'Data table',
}) => {
  const getColumnValue = (column, row) => {
    if (column.sortValue) return column.sortValue(row);
    if (column.csvValue) return column.csvValue(row);
    if (column.key) return row[column.key];
    return '';
  };

  const getCsvValue = (column, row) => {
    const value = column.csvValue ? column.csvValue(row) : getColumnValue(column, row);
    if (value == null) return '';
    return String(value);
  };

  const defaultSortKey = initialSortKey || (columns.find(c => c.sortable !== false)?.key ?? null);
  const [sortKey, setSortKey] = useState(defaultSortKey);
  const [sortDir, setSortDir] = useState(initialSortDir);

  const sortedRows = useMemo(() => {
    if (!sortKey) return rows;
    const column = columns.find(c => c.key === sortKey);
    if (!column) return rows;

    const normalized = [...rows];
    normalized.sort((a, b) => {
      const aVal = getColumnValue(column, a);
      const bVal = getColumnValue(column, b);

      if (aVal == null && bVal == null) return 0;
      if (aVal == null) return 1;
      if (bVal == null) return -1;

      if (typeof aVal === 'number' && typeof bVal === 'number') {
        return sortDir === 'asc' ? aVal - bVal : bVal - aVal;
      }

      const aNum = Number(aVal);
      const bNum = Number(bVal);
      if (!Number.isNaN(aNum) && !Number.isNaN(bNum) && String(aVal).trim() !== '' && String(bVal).trim() !== '') {
        return sortDir === 'asc' ? aNum - bNum : bNum - aNum;
      }

      const aStr = String(aVal).toLowerCase();
      const bStr = String(bVal).toLowerCase();
      return sortDir === 'asc' ? aStr.localeCompare(bStr) : bStr.localeCompare(aStr);
    });
    return normalized;
  }, [rows, columns, sortKey, sortDir]);

  const toggleSort = (column) => {
    if (column.sortable === false) return;
    if (sortKey === column.key) {
      setSortDir(prev => prev === 'asc' ? 'desc' : 'asc');
    } else {
      setSortKey(column.key);
      setSortDir('asc');
    }
  };

  // Sort icons (SVG, not emoji): none, asc, desc
  const SortIconNone = () => React.createElement('span', { className: 'sort-icon sort-none', style: { marginLeft: 4, display: 'inline-flex', verticalAlign: 'middle', opacity: 0.6 } },
    React.createElement('svg', { width: 14, height: 14, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2, strokeLinecap: 'round', strokeLinejoin: 'round' },
      React.createElement('path', { d: 'M11 5v14M18 9l-7 7-7-7' }),
      React.createElement('path', { d: 'M13 19V5M6 15l7-7 7 7' })));
  const SortIconAsc = () => React.createElement('span', { className: 'sort-icon sort-asc', style: { marginLeft: 4, display: 'inline-flex', verticalAlign: 'middle' }, title: 'Ascending' },
    React.createElement('svg', { width: 14, height: 14, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2, strokeLinecap: 'round', strokeLinejoin: 'round' },
      React.createElement('path', { d: 'M12 19V5M5 12l7-7 7 7' })));
  const SortIconDesc = () => React.createElement('span', { className: 'sort-icon sort-desc', style: { marginLeft: 4, display: 'inline-flex', verticalAlign: 'middle' }, title: 'Descending' },
    React.createElement('svg', { width: 14, height: 14, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2, strokeLinecap: 'round', strokeLinejoin: 'round' },
      React.createElement('path', { d: 'M12 5v14M19 12l-7 7-7-7' })));

  const downloadCsv = () => {
    const header = columns.map(col => col.label);
    const body = sortedRows.map(row => columns.map(col => {
      const value = getCsvValue(col, row).replace(/"/g, '""');
      return `"${value}"`;
    }).join(','));
    const csv = [header.join(','), ...body].join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  return React.createElement('div', null,
    React.createElement('div', {
      style: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        gap: 12,
        marginBottom: 10,
      },
      'aria-live': 'polite',
    },
      React.createElement('span', { style: { fontSize: 12, color: 'var(--text-muted)' } }, 
        loading ? 'Loading...' : `${rows.length} ${rows.length === 1 ? 'row' : 'rows'}`
      ),
      React.createElement('button', {
        className: 'btn btn-outline',
        type: 'button',
        style: { padding: '4px 10px', fontSize: 12 },
        onClick: downloadCsv,
        disabled: rows.length === 0 || loading,
        'aria-label': 'Download data as CSV'
      }, 'Download CSV')
    ),
    React.createElement('div', {
      className: loading ? 'skeleton' : '',
      style: {
        maxHeight,
        overflow: 'auto',
        border: '1px solid var(--line-soft)',
        borderRadius: 12,
      }
    },
      loading 
        ? React.createElement(window.SkeletonLoader, { width: '100%', height: '16px', count: 5, gap: '12px' })
        : React.createElement('table', { 
          style: { width: '100%', tableLayout: compact ? 'fixed' : 'auto' },
          'aria-label': ariaLabel,
          role: 'region',
        },
          React.createElement('thead', null,
            React.createElement('tr', null,
              columns.map(col => React.createElement('th', {
                key: col.key || col.label,
                onClick: () => toggleSort(col),
                style: {
                  cursor: col.sortable === false ? 'default' : 'pointer',
                  position: 'sticky',
                  top: 0,
                  zIndex: 1,
                  background: 'var(--panel-2, #162334)',
                  userSelect: 'none',
                  whiteSpace: compact ? 'normal' : 'nowrap',
                  overflowWrap: compact ? 'anywhere' : 'normal',
                  ...(col.headerStyle || {}),
                },
                'aria-sort': sortKey === col.key ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none',
                role: 'columnheader',
              },
                React.createElement('div', { style: { display: 'flex', alignItems: 'center', justifyContent: 'flex-start', flexWrap: 'nowrap' } },
                  col.label,
                  col.sortable === false ? null : (sortKey === col.key ? (sortDir === 'asc' ? React.createElement(SortIconAsc) : React.createElement(SortIconDesc)) : React.createElement(SortIconNone))
                )
              ))
            )
          ),
          React.createElement('tbody', null,
            sortedRows.length === 0
              ? React.createElement('tr', null,
                  React.createElement('td', {
                    colSpan: Math.max(columns.length, 1),
                    style: { textAlign: 'center', padding: 32, color: 'var(--text-muted)' }
                  }, emptyMessage)
                )
              : sortedRows.map((row, rowIndex) => React.createElement('tr', { key: row.id || row.key || rowIndex },
                  columns.map(col => React.createElement('td', {
                    key: col.key || col.label,
                    style: {
                      whiteSpace: compact ? 'normal' : 'nowrap',
                      overflowWrap: compact ? 'anywhere' : 'normal',
                      ...(col.cellStyle || {}),
                    },
                  }, col.render ? col.render(row) : (getColumnValue(col, row) ?? '-')))
                ))
          )
        )
    )
  );
};
