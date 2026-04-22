/**
 * js/admin_page.js
 * =================
 * Admin-only page for User Management and RBAC Plant Mapping.
 */

window.AdminPage = (props = {}) => {
  const { orgDefaultTheme, onOrgThemeSaved } = props;
  const { useState, useEffect } = React;
  const h = React.createElement;
  const { Card, Badge, Modal, DataTable } = window;

  const [activeTab, setActiveTab] = useState('users');
  const [perfLoaded, setPerfLoaded] = useState(false);
  const [users, setUsers] = useState([]);
  const [plants, setPlants] = useState([]);
  const [loading, setLoading] = useState(false);
  const [showCreate, setShowCreate] = useState(false);
  const [showEdit, setShowEdit] = useState(false);
  const [editingUser, setEditingUser] = useState(null);
  
  const [form, setForm] = useState({ email:'', full_name:'', password:'', is_admin: false, allowed_plants:'' });
  const [editForm, setEditForm] = useState({ email:'', full_name:'', password:'', is_active:true, is_admin: false, allowed_plants:'' });

  const THEME_SWATCHES = [
    { id: 'dark_ocean', label: 'Dark · Ocean' },
    { id: 'dark_ink', label: 'Dark · Ink' },
    { id: 'dark_forest', label: 'Dark · Forest' },
    { id: 'light_paper', label: 'Light · Paper' },
    { id: 'light_air', label: 'Light · Air' },
    { id: 'light_sand', label: 'Light · Sand' },
    { id: 'vikram', label: 'Vikram Solar' },
  ];
  const [appearanceDraft, setAppearanceDraft] = useState(() => orgDefaultTheme || 'dark_ocean');
  const [appearanceSaving, setAppearanceSaving] = useState(false);

  useEffect(() => {
    setAppearanceDraft(orgDefaultTheme || 'dark_ocean');
  }, [orgDefaultTheme]);

  useEffect(() => {
    return function () {
      try {
        var fn = window.__solarApplyThemeForPreview;
        var gt = window.__solarGetStoredTheme;
        if (fn && gt) fn(gt());
      } catch (e) {}
    };
  }, []);


  const normalizePlants = (value) => {
    return String(value || '')
      .split(',')
      .map(v => v.trim())
      .filter(Boolean)
      .join(',');
  };

  const parsePlants = (value) => {
    return String(value || '')
      .split(',')
      .map(v => v.trim())
      .filter(Boolean);
  };

  const toPlantString = (list) => normalizePlants(Array.from(new Set(list || [])).join(','));

  const renderPlantSelector = (selectedCsv, onChange) => {
    const allPlantIds = (plants || []).map(p => p.plant_id).filter(Boolean);
    const selected = parsePlants(selectedCsv);
    const allSelected = selected.includes('*');

    const toggleAll = () => {
      onChange(allSelected ? '' : '*');
    };

    const togglePlant = (plantId) => {
      if (allSelected) {
        onChange(plantId);
        return;
      }
      const exists = selected.includes(plantId);
      const next = exists ? selected.filter(v => v !== plantId) : [...selected, plantId];
      onChange(toPlantString(next));
    };

    const chipStyle = (active) => ({
      padding: '6px 10px',
      borderRadius: 999,
      border: active ? '1px solid #0284c7' : '1px solid var(--line)',
      background: active ? 'rgba(2,132,199,0.12)' : 'var(--panel)',
      color: active ? '#0369a1' : 'var(--text)',
      fontSize: 12,
      fontWeight: 600,
      cursor: 'pointer',
    });

    return h('div', { className: 'form-group' },
      h('label', { className:'form-label' }, 'Plant Access'),
      h('div', { style:{display:'flex', flexWrap:'wrap', gap:8, marginBottom:8} },
        h('button', { type:'button', style:chipStyle(allSelected), onClick:toggleAll }, 'All Plants'),
        allPlantIds.map(pid =>
          h('button', {
            key: pid,
            type:'button',
            style: chipStyle(allSelected || selected.includes(pid)),
            onClick: () => togglePlant(pid)
          }, pid)
        )
      ),
      h('div', { style:{fontSize:11, color:'var(--text-muted)'} },
        allSelected
          ? 'Access: All plants'
          : (`Access: ${selected.length ? selected.join(', ') : 'No plants selected'}`)
      )
    );
  };

  const loadAll = () => {
    setLoading(true);
    Promise.all([
      window.SolarAPI.Admin.listUsers(),
      window.SolarAPI.Plants.list()
    ]).then(([u, p]) => {
      setUsers(u);
      setPlants(p);
    }).catch(e => console.error(e))
    .finally(() => setLoading(false));
  };

  useEffect(loadAll, []);

  const handleCreate = async () => {
    if (!form.email || !form.password) return alert('Email and Password required');
    try {
      await window.SolarAPI.Admin.createUser({
        email: form.email.trim(),
        full_name: (form.full_name || '').trim() || null,
        password: form.password,
        is_admin: !!form.is_admin,
        allowed_plants: normalizePlants(form.allowed_plants) || null,
      });
      setShowCreate(false);
      setForm({ email: '', full_name: '', password: '', is_admin: false, allowed_plants: '' });
      loadAll();
    } catch (e) {
      alert(e && (e.message || e.detail || String(e)) || 'Failed to create user');
    }
  };

  const openEdit = (user) => {
    setEditingUser(user);
    setEditForm({
      email: user.email || '',
      full_name: user.full_name || '',
      password: '',
      is_active: user.is_active !== false,
      is_admin: !!user.is_admin,
      allowed_plants: user.allowed_plants || '',
    });
    setShowEdit(true);
  };

  const handleEditSave = async () => {
    if (!editingUser) return;
    if (!editForm.email || !String(editForm.email).trim()) return alert('Email is required');
    try {
      await window.SolarAPI.Admin.updateUser(editingUser.id, {
        email: String(editForm.email || '').trim(),
        full_name: (editForm.full_name || '').trim() || null,
        password: (editForm.password || '').trim() || null,
        is_active: !!editForm.is_active,
        is_admin: !!editForm.is_admin,
        allowed_plants: editForm.is_admin ? null : (normalizePlants(editForm.allowed_plants) || null),
      });
      setShowEdit(false);
      setEditingUser(null);
      loadAll();
    } catch (e) {
      alert(e && (e.message || e.detail || String(e)) || 'Failed to update user');
    }
  };

  const handleDelete = async (userId) => {
    if (!confirm('Are you sure you want to delete this user?')) return;
    try {
      await window.SolarAPI.Admin.deleteUser(userId);
      loadAll();
    } catch (e) {
      alert(e && (e.message || e.detail || String(e)) || 'Failed to delete user');
    }
  };


  const saveOrgAppearance = async () => {
    if (!window.SolarAPI.Admin.updateSiteAppearance) {
      alert('Update API not available. Refresh the app.');
      return;
    }
    setAppearanceSaving(true);
    try {
      var r = await window.SolarAPI.Admin.updateSiteAppearance({ org_default_theme: appearanceDraft });
      if (onOrgThemeSaved) onOrgThemeSaved(r.org_default_theme || appearanceDraft);
      alert('Organization default theme saved.');
    } catch (e) {
      alert((e && (e.message || e.detail)) ? (e.message || e.detail) : String(e));
    } finally {
      setAppearanceSaving(false);
    }
  };

  const handleDeletePlant = async (plantId) => {
    const ok = confirm(
      `Delete plant "${plantId}" forever?\n\nThis will permanently remove the plant, raw data, architecture, equipment specs, faults, snapshots, and related records from the database.`
    );
    if (!ok) return;
    try {
      await window.SolarAPI.Admin.deletePlant(plantId);
      loadAll();
    } catch (e) {
      alert(e && (e.message || e.detail || String(e)) || 'Failed to delete plant');
    }
  };

  const userColumns = [
    { key:'id', label:'ID', csvValue:(u)=>u.id },
    {
      key:'user',
      label:'User',
      sortValue:(u)=>(u.full_name || u.email || '').toLowerCase(),
      render:(u)=>h('div', null,
        h('div', { style:{fontWeight:600} }, u.full_name || 'No Name'),
        h('div', { style:{fontSize:11, color:'var(--text-muted)'} }, u.email)
      ),
      csvValue:(u)=>`${u.full_name || 'No Name'} (${u.email})`,
    },
    {
      key:'role',
      label:'Role',
      sortValue:(u)=>u.is_admin ? 1 : 0,
      render:(u)=>h(Badge, { type: u.is_admin ? 'blue' : 'amber' }, u.is_admin ? 'Administrator' : 'General User'),
      csvValue:(u)=>u.is_admin ? 'Administrator' : 'General User',
    },
    {
      key:'allowed_plants',
      label:'Allowed Plants',
      sortValue:(u)=>(u.allowed_plants || '').toLowerCase(),
      render:(u)=>u.is_admin
        ? h('span', { style:{fontSize:11, color:'var(--text-muted)'} }, 'All Plants (Admin)')
        : h('div', { style:{display:'flex', flexWrap:'wrap', gap:4} },
            (u.allowed_plants || '').split(',').filter(Boolean).map(p => h(Badge, { key:p, type:'green' }, p))
          ),
      csvValue:(u)=>u.is_admin ? 'All Plants (Admin)' : (u.allowed_plants || 'No access'),
    },
    {
      key:'actions',
      label:'Actions',
      sortable:false,
      render:(u)=>h('div', { style:{display:'flex', gap:6, flexWrap:'wrap'} },
        h('button', { className:'btn btn-outline', style:{padding:'4px 8px', fontSize:11}, onClick:()=>openEdit(u) }, 'Edit'),
        h('button', { className:'btn btn-outline', style:{padding:'4px 8px', fontSize:11, color:'var(--solar-red)', borderColor:'#FECACA'}, onClick:()=>handleDelete(u.id) }, 'Delete')
      ),
      csvValue:()=> 'Edit/Delete',
    },
  ];

  const plantColumns = [
    {
      key: 'plant_id',
      label: 'Plant ID',
      render: (p) => h('strong', null, p.plant_id),
      csvValue: (p) => p.plant_id,
    },
    {
      key: 'name',
      label: 'Plant Name',
      render: (p) => p.name || '-',
      csvValue: (p) => p.name || '-',
    },
    {
      key: 'capacity_mwp',
      label: 'Capacity (MWp)',
      sortValue: (p) => p.capacity_mwp ?? -Infinity,
      render: (p) => p.capacity_mwp != null ? Number(p.capacity_mwp).toFixed(3) : '-',
      csvValue: (p) => p.capacity_mwp != null ? Number(p.capacity_mwp).toFixed(3) : '-',
    },
    {
      key: 'status',
      label: 'Status',
      render: (p) => p.status || '-',
      csvValue: (p) => p.status || '-',
    },
    {
      key: 'actions',
      label: 'Actions',
      sortable: false,
      render: (p) => h('button', {
        className:'btn btn-outline',
        style:{padding:'4px 8px', fontSize:11, color:'var(--solar-red)', borderColor:'#FECACA'},
        onClick:()=>handleDeletePlant(p.plant_id)
      }, 'Delete Forever'),
      csvValue: () => 'Delete Forever',
    },
  ];

  // ── Lazy-load Performance panel ────────────────────────────────────────────
  useEffect(() => {
    if (activeTab === 'performance' && !perfLoaded && !window.PerfAdminPanel) {
      if (window.__loadModule) {
        window.__loadModule('AdminPerf').then(() => setPerfLoaded(true)).catch(() => setPerfLoaded(true));
      } else {
        setPerfLoaded(true);
      }
    }
  }, [activeTab, perfLoaded]);

  const TAB_DEFS = [
    { id: 'users', label: 'Users & Access' },
    { id: 'appearance', label: 'Appearance' },
    { id: 'performance', label: '⚡ Performance' },
  ];

  const tabBarStyle = { display: 'flex', gap: 0, borderBottom: '1px solid var(--line)', marginBottom: 18 };
  const tabStyle = (active) => ({
    padding: '10px 18px', cursor: 'pointer', fontSize: 13, fontWeight: active ? 700 : 500,
    color: active ? 'var(--accent)' : 'var(--text-muted)', borderBottom: active ? '2px solid var(--accent)' : '2px solid transparent',
    background: 'none', border: 'none', borderRadius: 0, transition: 'color .15s',
  });

  return h('div', null,
    h('div', { className:'page-header', style:{display:'flex', flexDirection:'row', justifyContent:'space-between', alignItems:'center', gap:12, flexWrap:'wrap'} },
      h('div', null,
        h('h2', null, 'Admin Panel'),
        h('p', null, 'Manage users, appearance, and monitor system performance'),
      ),
      activeTab === 'users' && h('button', { className:'btn btn-primary', onClick: () => setShowCreate(true) }, 'Create New User')
    ),

    // Tab bar
    h('div', { style: tabBarStyle },
      TAB_DEFS.map(t => h('button', {
        key: t.id, style: tabStyle(activeTab === t.id),
        onClick: () => setActiveTab(t.id)
      }, t.label))
    ),

    // ── Users & Access tab ──────────────────────────────────────────────────
    activeTab === 'users' && h(React.Fragment, null,
      h(Card, { title:`System Users (${users.length})` },
        h(DataTable, {
          columns: userColumns,
          rows: users,
          emptyMessage: 'No users found',
          filename: 'system_users.csv',
          maxHeight: 420,
          initialSortKey: 'id',
          compact: true,
        })
      ),

      h(Card, { title:`Plants (${plants.length})`, style:{ marginTop: 16 } },
        h('div', { style:{fontSize:12, color:'var(--text-muted)', marginBottom:10} },
          'Admin-only permanent delete. This removes the plant and all related database records.'
        ),
        h(DataTable, {
          columns: plantColumns,
          rows: plants,
          emptyMessage: 'No plants found',
          filename: 'plants.csv',
          maxHeight: 320,
          initialSortKey: 'plant_id',
          compact: true,
        })
      )
    ),

    // ── Appearance tab ──────────────────────────────────────────────────────
    activeTab === 'appearance' && h(Card, { title: 'Organization appearance' },
      h('p', { style: { fontSize: 12, color: 'var(--text-muted)', marginBottom: 12 } },
        'Choose the default theme for new visitors who have not picked a personal theme. Hover swatches to preview; click to select; Save applies for the organization.'),
      h('div', {
        style: { display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))', gap: 10, marginBottom: 12 },
        onMouseLeave: function () {
          try {
            var fn = window.__solarApplyThemeForPreview;
            var gt = window.__solarGetStoredTheme;
            if (fn && gt) fn(gt());
          } catch (e) {}
        },
      },
        THEME_SWATCHES.map(function (sw) {
          var active = appearanceDraft === sw.id;
          return h('button', {
            key: sw.id,
            type: 'button',
            onClick: function () { setAppearanceDraft(sw.id); },
            onMouseEnter: function () {
              try {
                if (window.__solarApplyThemeForPreview) window.__solarApplyThemeForPreview(sw.id);
              } catch (e) {}
            },
            style: {
              textAlign: 'left',
              padding: '10px 12px',
              borderRadius: 10,
              border: active ? '2px solid var(--accent)' : '1px solid var(--line)',
              background: active ? 'var(--accent-soft)' : 'var(--panel)',
              color: 'var(--text)',
              cursor: 'pointer',
              fontSize: 12,
              fontWeight: 600,
            },
          }, sw.label);
        }),
      ),
      h('div', { style: { display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center' } },
        h('button', { className: 'btn btn-primary', disabled: appearanceSaving, onClick: saveOrgAppearance },
          appearanceSaving ? 'Saving…' : 'Save organization default'),
        h('span', { style: { fontSize: 11, color: 'var(--text-muted)' } }, 'Current draft: ' + appearanceDraft),
      ),
    ),

    // ── Performance tab ─────────────────────────────────────────────────────
    activeTab === 'performance' && (
      window.PerfAdminPanel
        ? h(window.PerfAdminPanel)
        : h('div', { style: { padding: 40, textAlign: 'center', color: 'var(--text-muted)' } },
            h('div', { className: 'loading-spinner', style: { margin: '0 auto 12px', width: 24, height: 24 } }),
            'Loading performance module…'
          )
    ),

    // ── Modals (Create / Edit) ──────────────────────────────────────────────
    showCreate && h(Modal, {
      title: 'Create New User',
      open: true,
      onClose: () => setShowCreate(false),
      footer: h(React.Fragment, null,
        h('button', { className:'btn btn-outline', onClick:()=>setShowCreate(false) }, 'Cancel'),
        h('button', { className:'btn btn-primary', onClick:handleCreate }, 'Create User')
      )
    },
      h('div', { style:{display:'flex', flexDirection:'column', gap:15} },
        h('div', { className:'form-group' },
          h('label', { className:'form-label' }, 'Full Name'),
          h('input', { className:'form-input', value:form.full_name, onChange:e=>setForm({...form, full_name:e.target.value}), placeholder:'John Doe' })
        ),
        h('div', { className:'form-group' },
          h('label', { className:'form-label' }, 'Email Address'),
          h('input', { className:'form-input', type:'email', value:form.email, onChange:e=>setForm({...form, email:e.target.value}), placeholder:'john@example.com' })
        ),
        h('div', { className:'form-group' },
          h('label', { className:'form-label' }, 'Password'),
          h('input', { className:'form-input', type:'password', value:form.password, onChange:e=>setForm({...form, password:e.target.value}), placeholder:'••••••••' })
        ),
        h('div', { style:{display:'flex', alignItems:'center', gap:10, margin:'10px 0'} },
          h('input', { type:'checkbox', id:'is_admin', checked:form.is_admin, onChange:e=>setForm({...form, is_admin:e.target.checked}) }),
          h('label', { htmlFor:'is_admin', style:{fontSize:13, fontWeight:500} }, 'Grant Administrator Privileges')
        ),
        !form.is_admin && renderPlantSelector(form.allowed_plants, (value) => setForm({ ...form, allowed_plants: value }))
      )
    ),

    showEdit && h(Modal, {
      title: `Edit User #${editingUser?.id || ''}`,
      open: true,
      onClose: () => { setShowEdit(false); setEditingUser(null); },
      footer: h(React.Fragment, null,
        h('button', { className:'btn btn-outline', onClick:()=>{ setShowEdit(false); setEditingUser(null); } }, 'Cancel'),
        h('button', { className:'btn btn-primary', onClick:handleEditSave }, 'Save Changes')
      )
    },
      h('div', { style:{display:'flex', flexDirection:'column', gap:15} },
        h('div', { className:'form-group' },
          h('label', { className:'form-label' }, 'Full Name'),
          h('input', { className:'form-input', value:editForm.full_name, onChange:e=>setEditForm({...editForm, full_name:e.target.value}), placeholder:'John Doe' })
        ),
        h('div', { className:'form-group' },
          h('label', { className:'form-label' }, 'Email Address'),
          h('input', { className:'form-input', type:'email', value:editForm.email, onChange:e=>setEditForm({...editForm, email:e.target.value}), placeholder:'john@example.com' })
        ),
        h('div', { className:'form-group' },
          h('label', { className:'form-label' }, 'New Password (Optional)'),
          h('input', { className:'form-input', type:'password', value:editForm.password, onChange:e=>setEditForm({...editForm, password:e.target.value}), placeholder:'Leave empty to keep existing password' })
        ),
        h('div', { style:{display:'flex', alignItems:'center', gap:10} },
          h('input', { type:'checkbox', id:'is_active_edit', checked:editForm.is_active, onChange:e=>setEditForm({...editForm, is_active:e.target.checked}) }),
          h('label', { htmlFor:'is_active_edit', style:{fontSize:13, fontWeight:500} }, 'User is active')
        ),
        h('div', { style:{display:'flex', alignItems:'center', gap:10} },
          h('input', { type:'checkbox', id:'is_admin_edit', checked:editForm.is_admin, onChange:e=>setEditForm({...editForm, is_admin:e.target.checked}) }),
          h('label', { htmlFor:'is_admin_edit', style:{fontSize:13, fontWeight:500} }, 'Grant Administrator Privileges')
        ),
        !editForm.is_admin && renderPlantSelector(editForm.allowed_plants, (value) => setEditForm({ ...editForm, allowed_plants: value }))
      )
    )
  );
};

