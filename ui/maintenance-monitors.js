import {
  apiFetch,
  escapeHtml,
  formatDateTime,
  idChip,
  setInnerHTMLIfChanged,
  setModalState,
  statusPill,
} from './core/api.js';
import { renderIcebergMaintenance } from './iceberg-maintenance.js';

let refreshDashboard = async () => {};
let initialized = false;
let currentMonitorDetailID = '';

export function initMaintenanceMonitors(handlers = {}) {
  refreshDashboard = handlers.refreshDashboard || refreshDashboard;
  if (initialized) return;
  initialized = true;
  document.addEventListener('click', handleMonitorAction);
}

function setNotice(message, tone = 'info') {
  const el = document.getElementById('maintenanceMonitorsNotice');
  if (!el) return;
  if (!message) {
    el.className = 'hidden rounded-xl border px-4 py-3 text-sm';
    el.textContent = '';
    return;
  }
  const cls = tone === 'error'
    ? 'border-rose-200 bg-rose-50 text-rose-700'
    : tone === 'success'
      ? 'border-emerald-200 bg-emerald-50 text-emerald-800'
      : 'border-slate-200 bg-slate-50 text-slate-700';
  el.className = `rounded-xl border px-4 py-3 text-sm ${cls}`;
  el.textContent = message;
}

function monitorActions(monitor) {
  const id = escapeHtml(monitor.id || '');
  const active = String(monitor.status || '').toUpperCase() === 'ACTIVE';
  const base = 'whitespace-nowrap rounded-md border px-3 py-2 text-xs font-semibold transition';
  return `
    <div class="flex min-w-max flex-nowrap items-center justify-end gap-2">
      <button type="button" class="${base} border-slate-300 text-slate-800 hover:bg-slate-50" data-monitor-action="details" data-monitor-id="${id}">Details</button>
      ${active
        ? `<button type="button" class="${base} border-violet-200 text-violet-800 hover:bg-violet-50" data-monitor-action="pause" data-monitor-id="${id}">Pause</button>`
        : `<button type="button" class="${base} border-emerald-200 text-emerald-800 hover:bg-emerald-50" data-monitor-action="resume" data-monitor-id="${id}">Resume</button>`}
      <button type="button" class="${base} border-rose-200 text-rose-700 hover:bg-rose-50" data-monitor-action="delete" data-monitor-id="${id}">Delete</button>
    </div>
  `;
}

function monitorRow(monitor) {
  const tables = Array.isArray(monitor.tables) ? monitor.tables : [];
  const sample = tables.slice(0, 3).map((table) => `${table.namespace}.${table.table}`).join(', ');
  const remaining = Math.max(0, tables.length - 3);
  const catalogDiscovery = String(monitor.catalog || '').startsWith('auto:');
  const selectionSummary = catalogDiscovery
    ? 'Catalog discovery · non-streaming tables'
    : (sample || 'Waiting for table registration');
	const discovered = Number(monitor.discovered_count ?? monitor.table_count ?? tables.length ?? 0);
	const owned = Number(monitor.owned_count ?? monitor.table_count ?? 0);
	const reserved = Number(monitor.reserved_count || 0);
	const conflicts = Number(monitor.conflict_count || 0);
  const healthStatus = String(monitor.health_status || '').trim().toUpperCase();
  const shownStatus = healthStatus && healthStatus !== 'HEALTHY' ? healthStatus : monitor.status;
  return `
    <tr class="align-top transition hover:bg-white/70">
      <td data-label="ID" class="px-6 py-4">${idChip(monitor.id)}</td>
      <td data-label="Monitor" class="px-6 py-4">
        <div class="font-semibold text-slate-900">${escapeHtml(monitor.name || monitor.id)}</div>
        <div class="mono mt-1 max-w-[34rem] text-xs leading-5 text-slate-500">${escapeHtml(selectionSummary)}${!catalogDiscovery && remaining ? ` +${remaining} more` : ''}</div>
        ${monitor.last_error ? `<div class="mt-2 text-xs text-rose-700">${escapeHtml(monitor.last_error)}</div>` : ''}
		${monitor.last_discovery_error ? `<div class="mt-2 text-xs text-rose-700">Discovery: ${escapeHtml(monitor.last_discovery_error)}</div>` : ''}
      </td>
      <td data-label="Status" class="px-6 py-4">
        ${statusPill(shownStatus)}
      </td>
      <td data-label="Catalog" class="px-6 py-4">
        <div class="mono text-xs font-semibold text-slate-800">${escapeHtml(monitor.catalog || '-')}</div>
        <div class="mt-1 text-xs text-slate-500">${escapeHtml(monitor.executor || 'hybrid')} · ${escapeHtml(monitor.resource_profile || 'small')}</div>
      </td>
      <td data-label="Tables" class="px-6 py-4">
		<div class="text-sm font-semibold text-slate-800">${escapeHtml(owned)} maintained here</div>
		<div class="mt-1 text-xs text-slate-500">${escapeHtml(discovered)} discovered · ${escapeHtml(reserved)} streaming/snapshot · ${escapeHtml(conflicts)} another monitor</div>
	  </td>
      <td data-label="Last inventory" class="px-6 py-4 text-xs text-slate-600">${escapeHtml(formatDateTime(monitor.last_inventory_at))}</td>
      <td data-label="Actions" class="px-6 py-4 text-right">${monitorActions(monitor)}</td>
    </tr>
  `;
}

export async function loadMaintenanceMonitors() {
  const body = document.getElementById('maintenanceMonitorBody');
  if (!body) return;
  try {
    const response = await apiFetch('/api/iceberg/maintenance/monitors');
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data.error || `Unable to load monitors (${response.status})`);
    const monitors = Array.isArray(data.monitors) ? data.monitors : [];
    setInnerHTMLIfChanged(body, monitors.map(monitorRow).join(''), { preserveScroll: true });
    document.getElementById('maintenanceMonitorEmpty')?.classList.toggle('hidden', monitors.length > 0);
    document.querySelectorAll('[data-bind="maintenance-monitor-count"]').forEach((el) => {
      el.textContent = String(monitors.length);
    });
    const detail = document.getElementById('maintenanceMonitorDetail');
    if (currentMonitorDetailID && detail && !detail.classList.contains('hidden')) {
      await showMaintenanceMonitorDetails(currentMonitorDetailID);
    }
  } catch (error) {
    setNotice(error.message, 'error');
  }
}

async function mutateMonitor(id, action, method = 'POST') {
  const response = await apiFetch(`/api/iceberg/maintenance/monitors/${encodeURIComponent(id)}${action ? `/${action}` : ''}`, { method });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error || `${action || method} failed (${response.status})`);
  return data;
}

async function handleMonitorAction(event) {
  const button = event.target.closest('[data-monitor-action]');
  if (!button) return;
  const action = button.dataset.monitorAction;
  const id = button.dataset.monitorId;
  if (!id || !action) return;
  if (action === 'details') {
    button.disabled = true;
    try {
      await showMaintenanceMonitorDetails(id);
    } catch (error) {
      setNotice(error.message, 'error');
    } finally {
      button.disabled = false;
    }
    return;
  }
  if (action === 'delete' && !confirm(`Delete maintenance monitor ${id}?`)) return;
  button.disabled = true;
  try {
    const data = await mutateMonitor(id, action === 'delete' ? '' : action, action === 'delete' ? 'DELETE' : 'POST');
    if (action === 'run') {
      const count = Number(data.requested || 0);
      setNotice(count > 0
        ? `Queued ${count} table(s).`
        : 'No tables queued.', 'success');
    } else {
      setNotice(`Maintenance monitor ${id} ${action === 'delete' ? 'deleted' : `${action}d`}.`, 'success');
    }
    await refreshDashboard();
  } catch (error) {
    setNotice(error.message, 'error');
  } finally {
    button.disabled = false;
  }
}

export async function showMaintenanceMonitorDetails(id) {
  const response = await apiFetch(`/api/iceberg/maintenance/monitors/${encodeURIComponent(id)}`);
  const monitor = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(monitor.error || `Unable to load monitor details (${response.status})`);

  const list = document.getElementById('maintenanceMonitorList');
  const detail = document.getElementById('maintenanceMonitorDetail');
  const panel = document.getElementById('maintenanceMonitorDetailPanel');
  if (!detail || !panel) return;

  currentMonitorDetailID = id;

  document.getElementById('maintenanceMonitorDetailName').textContent = monitor.name || monitor.id || 'Maintenance monitor';
  document.getElementById('maintenanceMonitorDetailID').textContent = monitor.id || '-';
  const healthStatus = String(monitor.health_status || '').trim().toUpperCase();
  const shownStatus = healthStatus && healthStatus !== 'HEALTHY' ? healthStatus : monitor.status;
  document.getElementById('maintenanceMonitorDetailStatus').innerHTML = statusPill(shownStatus);
  list?.classList.add('hidden');
  detail.classList.remove('hidden');

  const maintenance = monitor.maintenance || {
    enabled: true,
    state: 'inventory_pending',
    tables_total: Number(monitor.table_count || 0),
    tables_scanned: 0,
    tables: [],
  };
  const job = {
    id: `monitor:${monitor.id}`,
    sink_type: 'iceberg_native',
    config: {
      sink: {
        type: 'iceberg_native',
        config: {
          table_maintenance: {
            enabled: true,
            executor: monitor.executor || 'hybrid',
          },
        },
      },
    },
    iceberg_maintenance: maintenance,
  };
  renderIcebergMaintenance(job, {
    panel,
    ownershipSummary: [
      ['Discovered', monitor.discovered_count || 0],
      ['Maintained here', monitor.owned_count ?? monitor.table_count ?? 0],
      ['Used by streaming/snapshot', monitor.reserved_count || 0],
      ['Excluded scopes', monitor.excluded_scope_count || 0],
      ['Owned by another monitor', monitor.conflict_count || 0],
      ['Retired', monitor.retired_count || 0],
    ],
    historyOwnerID: `monitor:${monitor.id}`,
    historyLabel: 'Maintenance runs',
    refreshLabel: 'Refresh inventory',
    refreshDisabled: String(monitor.status || '').toUpperCase() !== 'ACTIVE',
    onRefreshInventory: async () => {
      await mutateMonitor(monitor.id, 'run');
      await showMaintenanceMonitorDetails(monitor.id);
    },
  });

}

export function closeMaintenanceMonitorDetails() {
  currentMonitorDetailID = '';
  document.getElementById('maintenanceMonitorDetail')?.classList.add('hidden');
  document.getElementById('maintenanceMonitorList')?.classList.remove('hidden');
}

export function openMaintenanceMonitorModal() {
  setModalState('maintenanceMonitorModal', true);
  document.getElementById('maintenanceMonitorYaml')?.focus();
}

export function closeMaintenanceMonitorModal() {
  setModalState('maintenanceMonitorModal', false);
}

export function clearMaintenanceMonitorYaml() {
  const input = document.getElementById('maintenanceMonitorYaml');
  if (input) input.value = '';
  setMonitorSubmitStatus('Ready for a maintenance-only YAML definition.');
}

export async function loadMaintenanceMonitorFile(event) {
  const file = event?.target?.files?.[0];
  if (!file) return;
  try {
    document.getElementById('maintenanceMonitorYaml').value = await file.text();
    setMonitorSubmitStatus(`Loaded file: ${file.name}`);
  } catch (error) {
    setMonitorSubmitStatus(error.message, 'error');
  }
}

function setMonitorSubmitStatus(message, tone = 'muted') {
  const el = document.getElementById('maintenanceMonitorSubmitStatus');
  if (!el) return;
  el.className = `text-sm ${tone === 'error' ? 'text-rose-600' : tone === 'success' ? 'text-emerald-700' : 'text-slate-500'}`;
  el.textContent = message;
}

export async function submitMaintenanceMonitor() {
  const input = document.getElementById('maintenanceMonitorYaml');
  const button = document.getElementById('maintenanceMonitorSubmitButton');
  const yaml = input?.value || '';
  if (!yaml.trim()) {
    setMonitorSubmitStatus('Maintenance monitor YAML is empty.', 'error');
    return;
  }
  button.disabled = true;
  setMonitorSubmitStatus('Creating maintenance monitor...');
  try {
    const response = await apiFetch('/api/iceberg/maintenance/monitors', {
      method: 'POST', headers: { 'Content-Type': 'application/x-yaml' }, body: yaml,
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data.error || `Create failed (${response.status})`);
    setMonitorSubmitStatus(`Created ${data.id}.`, 'success');
    closeMaintenanceMonitorModal();
    setNotice(`Maintenance monitor ${data.id} is active.`, 'success');
    await refreshDashboard();
  } catch (error) {
    setMonitorSubmitStatus(error.message, 'error');
  } finally {
    button.disabled = false;
  }
}
