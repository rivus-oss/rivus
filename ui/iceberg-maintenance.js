import {
  escapeHtml,
  fmtBytes,
  fmtWholeNumber,
  formatDateTime,
  setInnerHTMLIfChanged,
} from './core/api.js';
import { cleanupContent, bindCleanupControls } from './iceberg-cleanup.js';

function maintenanceConfig(job) {
  return job?.config?.sink?.config?.table_maintenance || {};
}

function configuredMaintenanceEnabled(job) {
  const cfg = maintenanceConfig(job);
  if (typeof cfg.enabled === 'boolean') return cfg.enabled;
  return !!job?.iceberg_maintenance?.enabled;
}

function maintenanceExecutor(job, enabled) {
  if (!enabled) return '-';
  const value = String(maintenanceConfig(job)?.executor || '').trim().toLowerCase();
  return value === 'native' || value === 'spark' || value === 'hybrid' ? value : 'hybrid';
}

function maintenanceStateLabel(state) {
  const labels = {
    waiting_for_snapshot: 'Waiting for snapshot',
    scanning: 'Scanning manifests',
    watching: 'Watching',
    accumulating: 'Collecting files',
    ready: 'Ready for maintenance',
    running: 'Maintenance running',
    inventory_pending: 'Waiting for inventory scan',
    stale: 'Inventory stale',
    partitioned: 'No eligible file groups',
    healthy: 'Healthy',
    paused: 'Paused',
    error: 'Inventory error',
    disabled: 'Disabled',
  };
  return labels[String(state || '').toLowerCase()] || String(state || 'Unknown');
}

function maintenanceStateClass(state) {
  switch (String(state || '').toLowerCase()) {
    case 'running':
      return 'border-blue-200 bg-blue-50 text-blue-700';
    case 'inventory_pending':
    case 'scanning':
    case 'stale':
      return 'border-amber-200 bg-amber-50 text-amber-700';
    case 'ready':
      return 'border-amber-200 bg-amber-50 text-amber-700';
    case 'partitioned':
      return 'border-slate-200 bg-slate-50 text-slate-600';
    case 'paused':
      return 'border-violet-200 bg-violet-50 text-violet-700';
    case 'error':
      return 'border-rose-200 bg-rose-50 text-rose-700';
    case 'healthy':
    case 'watching':
      return 'border-emerald-200 bg-emerald-50 text-emerald-700';
    default:
      return 'border-slate-200 bg-slate-50 text-slate-600';
  }
}

function maintenanceReason(table) {
  const operations = Array.isArray(table?.operations) ? table.operations : [];
  const route = operations.find((item) => String(item || '').startsWith('route:'));
  if (route) return String(route).slice('route:'.length).trim();

  const operation = operations.find((item) => !String(item || '').startsWith('engine:'));
  if (!operation) return '';

  const labels = {
    compact: 'Compaction is being evaluated',
    expire_snapshots: 'Snapshot cleanup is being evaluated',
    remove_orphan_files: 'Orphan cleanup is being evaluated',
  };
  return labels[String(operation).trim()] || String(operation).trim();
}

function partitioningLabel(table) {
  const fields = table?.partition_fields;
  if (!Array.isArray(fields) || !fields.every((field) => typeof field === 'string' && field.trim())) {
    return 'Partitioning pending scan';
  }
  return fields.length ? `Partitioned · ${fields.join(', ')}` : 'Unpartitioned';
}

function compactionEligibility(table) {
  const files = table?.compactable_files;
  const groups = table?.compaction_groups;
  if (!table?.checked_at || files == null || groups == null
      || !Number.isInteger(files) || files < 0 || !Number.isInteger(groups) || groups < 0) {
    return 'Eligibility pending scan';
  }
  const stale = ['stale', 'inventory_pending', 'scanning', 'error'].includes(String(table.state || '').toLowerCase());
  const when = stale ? ' at last scan' : '';
  if (groups === 0) {
    const reason = Array.isArray(table.partition_fields) && table.partition_fields.length > 0
      ? 'No eligible groups within partitions.'
      : 'No eligible file groups.';
    return `${fmtWholeNumber(files)} files eligible${when}. ${reason}`;
  }
  return `${fmtWholeNumber(files)} files in ${fmtWholeNumber(groups)} eligible group${groups === 1 ? '' : 's'}${when}.`;
}

function maintenanceMetric(label, value, detail) {
  return `
    <div class="stat-card min-w-0">
      <div class="stat-label">${escapeHtml(label)}</div>
      <div class="mono mt-2 text-xl font-semibold text-slate-900">${escapeHtml(value)}</div>
      ${detail ? `<div class="mt-1 text-xs leading-5 text-slate-500">${escapeHtml(detail)}</div>` : ''}
    </div>
  `;
}

function workerConfigStrip(job, enabled, paused = false) {
  const executor = maintenanceExecutor(job, enabled);
  const executorLabel = executor === '-' ? '-' : executor.charAt(0).toUpperCase() + executor.slice(1);
  return `
    <div class="grid gap-3 border-b border-slate-200 bg-slate-50 px-5 py-4 sm:grid-cols-3 sm:px-6">
      ${maintenanceMetric('Automatic maintenance', paused ? 'Paused' : enabled ? 'Enabled' : 'Disabled')}
      ${maintenanceMetric('Scheduler', 'Maintenance Worker')}
      ${maintenanceMetric('Executor', executorLabel)}
    </div>
  `;
}

function historyLink(job, options = {}) {
  const jobID = String(options.historyOwnerID || job?.id || '').trim();
  const maintenanceUI = String(options.maintenanceUI || 'https://data-platform.asmat.app').replace(/\/$/, '');
  const href = jobID
    ? `${maintenanceUI}/ui/admin/iceberg-maintenance/runs?job_id=${encodeURIComponent(jobID)}`
    : `${maintenanceUI}/ui/admin/iceberg-maintenance/runs`;
  const label = options.historyLabel || (jobID ? "View this job's maintenance runs" : 'View maintenance runs');
  return `<a href="${href}" target="_blank" rel="noopener noreferrer" class="brand-outline-btn rounded-md px-3 py-1.5 text-xs font-semibold">${label}</a>`;
}

export function renderIcebergMaintenance(job, options = {}) {
  const panel = options.panel || document.getElementById('icebergMaintenancePanel');
  if (!panel) return;

  const sinkType = String(job?.sink_type || job?.config?.sink?.type || '').trim().toLowerCase();
  if (sinkType !== 'iceberg_native') {
    panel.classList.add('hidden');
    panel.innerHTML = '';
    return;
  }

  panel.classList.remove('hidden');
  const maintenance = job?.iceberg_maintenance;
  const configuredEnabled = configuredMaintenanceEnabled(job);

  if (!maintenance) {
    setInnerHTMLIfChanged(panel, `
      <div class="px-5 py-5 sm:px-6">
        <div class="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Iceberg Maintenance</div>
        <div class="mt-3 rounded-[16px] border border-slate-200 bg-slate-50 px-4 py-4 text-sm text-slate-600">
          Waiting for maintenance data.
        </div>
      </div>
      ${workerConfigStrip(job, configuredEnabled)}
    `);
    return;
  }

  if (!maintenance.enabled) {
    setInnerHTMLIfChanged(panel, `
      <div class="flex flex-col gap-3 px-5 py-5 sm:flex-row sm:items-center sm:justify-between sm:px-6">
        <div>
          <div class="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Iceberg Maintenance</div>
        </div>
        <span class="rounded-full border px-3 py-1.5 text-xs font-semibold ${maintenanceStateClass('disabled')}">Disabled</span>
      </div>
      ${workerConfigStrip(job, configuredEnabled)}
    `);
    return;
  }

  const state = String(maintenance.state || 'watching');
  const paused = state.toLowerCase() === 'paused' || maintenance.paused === true;
  const tables = Array.isArray(maintenance.tables) ? [...maintenance.tables] : [];
  const stateOrder = { running: 0, ready: 1, error: 2, stale: 3, accumulating: 4, scanning: 5, inventory_pending: 6, waiting_for_snapshot: 7, partitioned: 8, healthy: 9 };
  tables.sort((left, right) => {
    const stateDelta = (stateOrder[left?.state] ?? 9) - (stateOrder[right?.state] ?? 9);
    if (stateDelta !== 0) return stateDelta;
    const fileDelta = Number(right?.eligible_small_files || 0) - Number(left?.eligible_small_files || 0);
    if (fileDelta !== 0) return fileDelta;
    return String(left?.identifier || '').localeCompare(String(right?.identifier || ''));
  });

  const configuredPageSize = Number(options.pageSize);
  const pageSize = Number.isFinite(configuredPageSize) && configuredPageSize > 0
    ? Math.floor(configuredPageSize)
    : 100;
  const pageOwner = String(options.pageKey || options.historyOwnerID || job?.id || 'maintenance');
  if (panel.dataset.maintenancePageOwner !== pageOwner) {
    panel.dataset.maintenancePageOwner = pageOwner;
    panel.dataset.maintenancePage = '1';
    panel.dataset.maintenanceView = 'compaction';
    panel.dataset.cleanupOperation = 'remove_orphan_files';
    panel.dataset.cleanupStatus = '';
    panel.dataset.cleanupSearch = '';
    panel.dataset.cleanupPage = '1';
  }
  const cleanupSelected = panel.dataset.maintenanceView === 'cleanup';
  const pageCount = Math.max(1, Math.ceil(tables.length / pageSize));
  const requestedPage = Number.parseInt(panel.dataset.maintenancePage || '1', 10);
  const currentPage = Math.min(pageCount, Math.max(1, Number.isFinite(requestedPage) ? requestedPage : 1));
  panel.dataset.maintenancePage = String(currentPage);
  const pageStart = (currentPage - 1) * pageSize;
  const pageTables = tables.slice(pageStart, pageStart + pageSize);

  const dataThreshold = Number(maintenance.data_files_threshold || 0);
  const deleteThreshold = Number(maintenance.equality_delete_files_threshold || 0);
  const positionDeleteThreshold = Number(maintenance.position_delete_files_threshold || 25);
  const tableRows = pageTables.map((table) => {
    const applicableEquality = table?.applicable_equality_delete_files == null ? null : Number(table.applicable_equality_delete_files);
    const applicablePosition = table?.applicable_position_delete_files == null ? null : Number(table.applicable_position_delete_files);
    const retainedEquality = applicableEquality == null ? 0 : Math.max(0, Number(table?.active_equality_delete_files || 0) - applicableEquality);
    const retainedPosition = applicablePosition == null ? 0 : Math.max(0, Number(table?.active_position_delete_files || 0) - applicablePosition);
    const equalityLabel = applicableEquality == null ? '–' : fmtWholeNumber(applicableEquality);
    const positionLabel = applicablePosition == null ? '–' : fmtWholeNumber(applicablePosition);
    const reason = maintenanceReason(table) || compactionEligibility(table);
    const reasonCell = table?.error
      ? `<div class="max-w-md break-words text-[11px] leading-4 text-rose-600">${escapeHtml(table.error)}</div>`
      : reason
        ? `<div class="max-w-md break-words text-[11px] leading-4 text-slate-600">${escapeHtml(reason)}</div>`
        : '<span class="text-[11px] text-slate-400">-</span>';
    const statusDetail = table?.error
      ? '<div class="mt-1 text-[11px] text-rose-600">Needs retry</div>'
      : String(table?.state || '').toLowerCase() === 'stale'
        ? '<div class="mt-1 text-[11px] text-amber-700">Refresh required</div>'
        : '';
    return `
      <tr class="border-b border-slate-100 align-top last:border-0">
        <td class="px-4 py-3">
          <div class="mono text-xs font-semibold text-slate-800">${escapeHtml(table?.identifier || '-')}</div>
          <div class="mt-1 max-w-sm break-words text-[11px] leading-4 text-slate-500">${escapeHtml(partitioningLabel(table))}</div>
          <div class="mt-1 text-[11px] text-slate-500">Checked ${escapeHtml(table?.checked_at ? formatDateTime(table.checked_at) : '-')}</div>
        </td>
        <td class="mono px-4 py-3 text-right text-xs font-semibold text-slate-800">${fmtWholeNumber(table?.active_data_files || 0)}</td>
        <td class="px-4 py-3 text-right">
          <div class="mono text-xs font-semibold text-slate-800">${fmtWholeNumber(table?.eligible_small_files || 0)}</div>
          <div class="mt-1 text-[11px] text-slate-500">${escapeHtml(fmtBytes(Number(table?.eligible_small_bytes || 0)))}</div>
        </td>
        <td class="px-4 py-3 text-right"><div class="mono text-xs font-semibold text-slate-800">${equalityLabel}</div>${retainedEquality ? `<div class="mt-1 text-[11px] text-slate-500">${fmtWholeNumber(retainedEquality)} retained</div>` : ''}</td>
        <td class="px-4 py-3 text-right"><div class="mono text-xs font-semibold text-slate-800">${positionLabel}</div>${retainedPosition ? `<div class="mt-1 text-[11px] text-slate-500">${fmtWholeNumber(retainedPosition)} retained</div>` : ''}</td>
        <td class="px-4 py-3 text-xs text-slate-600">
          <div>Small: <span class="mono font-semibold text-slate-800">${fmtWholeNumber(table?.eligible_small_files || 0)} / ${fmtWholeNumber(dataThreshold)}</span></div>
          <div class="mt-1">Equality: <span class="mono font-semibold text-slate-800">${equalityLabel} / ${fmtWholeNumber(deleteThreshold)}</span></div>
          <div class="mt-1">Position: <span class="mono font-semibold text-slate-800">${positionLabel} / ${fmtWholeNumber(positionDeleteThreshold)}</span></div>
        </td>
        <td class="px-4 py-3">
          <span class="inline-flex rounded-full border px-2.5 py-1 text-[11px] font-semibold ${maintenanceStateClass(table?.state)}">${escapeHtml(maintenanceStateLabel(table?.state))}</span>
          ${statusDetail}
        </td>
        <td class="min-w-64 px-4 py-3">
          ${reasonCell}
        </td>
      </tr>
    `;
  }).join('');

  const checkedAt = maintenance.checked_at ? formatDateTime(maintenance.checked_at) : 'Waiting for first scan';
  const scanned = `${fmtWholeNumber(maintenance.tables_scanned || 0)} / ${fmtWholeNumber(maintenance.tables_total || 0)} tables scanned`;
  const visibleStart = tables.length === 0 ? 0 : pageStart + 1;
  const visibleEnd = Math.min(pageStart + pageSize, tables.length);
  const totalTables = Number(maintenance.tables_total || 0);
  const listLimited = totalTables > tables.length;
  const pagination = tables.length > pageSize
    ? `<div class="flex items-center gap-2">
        <button type="button" data-maintenance-page="${currentPage - 1}" class="brand-outline-btn rounded-md px-3 py-1.5 text-xs font-semibold disabled:cursor-not-allowed disabled:opacity-50" ${currentPage === 1 ? 'disabled' : ''}>Previous</button>
        <span class="mono text-xs text-slate-500">${fmtWholeNumber(currentPage)} / ${fmtWholeNumber(pageCount)}</span>
        <button type="button" data-maintenance-page="${currentPage + 1}" class="brand-outline-btn rounded-md px-3 py-1.5 text-xs font-semibold disabled:cursor-not-allowed disabled:opacity-50" ${currentPage === pageCount ? 'disabled' : ''}>Next</button>
      </div>`
    : '';
  const refreshLabel = String(options.refreshLabel || 'Refresh inventory');
  const refreshDisabled = options.refreshDisabled === true;
  const errors = Number(maintenance.inventory_errors || 0);
  const inventoryNotice = state === 'paused'
    ? 'Maintenance is paused.'
    : state === 'waiting_for_snapshot'
    ? 'Waiting for the initial snapshot.'
    : errors > 0
      ? `${fmtWholeNumber(errors)} table scan(s) failed.`
      : state.toLowerCase() === 'stale'
        ? 'Inventory is over one hour old. Refresh to update file counts.'
        : '';
  const inventoryTone = errors > 0
    ? 'border-rose-200 bg-rose-50 text-rose-700'
    : state === 'waiting_for_snapshot' || state === 'paused' || state.toLowerCase() === 'stale'
      ? 'border-amber-200 bg-amber-50 text-amber-800'
      : 'border-slate-200 bg-slate-50 text-slate-600';

  setInnerHTMLIfChanged(panel, `
    <div class="border-b border-slate-200 px-5 py-4 sm:px-6">
      <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <div class="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Iceberg Maintenance</div>
        </div>
        <div class="flex flex-wrap items-center gap-2">
          ${cleanupSelected ? '' : `<span class="rounded-full border px-3 py-1.5 text-xs font-semibold ${maintenanceStateClass(state)}">${escapeHtml(maintenanceStateLabel(state))}</span>
          <span class="rounded-full border border-slate-200 bg-white px-3 py-1.5 text-[11px] font-semibold text-slate-600">${escapeHtml(scanned)}</span>`}
          <button type="button" data-maintenance-inventory-refresh class="brand-outline-btn rounded-md px-3 py-1.5 text-xs font-semibold disabled:cursor-not-allowed disabled:opacity-50" ${refreshDisabled ? 'disabled' : ''}>${escapeHtml(refreshLabel)}</button>
          ${historyLink(job, options)}
        </div>
      </div>
      ${cleanupSelected ? '' : `<div class="mt-3 text-xs text-slate-500">Last successful inventory check: <span class="font-semibold text-slate-700">${escapeHtml(checkedAt)}</span></div>`}
    </div>

    ${workerConfigStrip(job, configuredEnabled, paused)}

    <div class="border-b border-slate-200 px-5 py-3 sm:px-6">
      <div class="inline-flex gap-1 rounded-lg border border-slate-200 bg-slate-50 p-1" role="tablist" aria-label="Maintenance operation">
        ${['compaction', 'cleanup'].map((view) => `<button type="button" role="tab" tabindex="${cleanupSelected === (view === 'cleanup') ? '0' : '-1'}" aria-selected="${cleanupSelected === (view === 'cleanup')}" data-maintenance-view="${view}" class="rounded-md border px-4 py-2 text-xs font-semibold ${cleanupSelected === (view === 'cleanup') ? 'border-slate-300 bg-white text-slate-900' : 'border-transparent text-slate-500'}">${view === 'cleanup' ? 'Cleanup' : 'Compaction'}</button>`).join('')}
      </div>
    </div>
    <div role="tabpanel" aria-label="${cleanupSelected ? 'Cleanup' : 'Compaction'}">
    ${cleanupSelected ? cleanupContent(maintenance, panel, pageOwner, maintenanceMetric, pageSize) : `
    <div class="grid gap-3 bg-slate-50 px-5 py-4 sm:grid-cols-2 xl:grid-cols-5 sm:px-6">
      ${maintenanceMetric('Active data files', fmtWholeNumber(maintenance.active_data_files || 0))}
      ${maintenanceMetric('Applicable equality deletes', fmtWholeNumber(maintenance.applicable_equality_delete_files || 0), Number(maintenance.applicable_counts_pending || 0) ? `${fmtWholeNumber(maintenance.applicable_counts_pending)} tables pending scan` : '')}
      ${maintenanceMetric('Small files', fmtWholeNumber(maintenance.eligible_small_files || 0), `Below ${fmtBytes(Number(maintenance.small_file_size_bytes || 0))}`)}
      ${maintenanceMetric('Small-file bytes', fmtBytes(Number(maintenance.eligible_small_bytes || 0)), 'Across all partitions')}
      ${maintenanceMetric(listLimited ? 'Tables ready (shown)' : 'Tables ready', fmtWholeNumber(maintenance.tables_ready || 0), `${fmtWholeNumber(maintenance.active_runs || 0)} maintenance run(s) active`)}
    </div>

    ${inventoryNotice ? `<div class="mx-5 mt-4 rounded-[16px] border px-4 py-3 text-sm ${inventoryTone} sm:mx-6">${escapeHtml(inventoryNotice)}</div>` : ''}

    <div class="px-5 pb-5 pt-4 sm:px-6 sm:pb-6">
      <div class="mb-3 flex flex-wrap items-center justify-between gap-2">
        <div class="text-xs text-slate-500">Rows ${fmtWholeNumber(visibleStart)}–${fmtWholeNumber(visibleEnd)} of ${fmtWholeNumber(tables.length)}${listLimited ? ` shown (${fmtWholeNumber(totalTables)} total)` : ''}</div>
        ${pagination}
      </div>
      <div class="overflow-hidden rounded-[16px] border border-slate-200">
        <div class="max-h-[560px] overflow-auto">
          <table class="min-w-full text-sm">
            <thead class="sticky top-0 z-10 bg-slate-50 text-slate-600">
              <tr class="border-b border-slate-200">
                <th class="min-w-64 px-4 py-3 text-left text-[11px] font-semibold uppercase tracking-[0.14em]">Iceberg table</th>
                <th class="px-4 py-3 text-right text-[11px] font-semibold uppercase tracking-[0.14em]">Data files</th>
                <th class="px-4 py-3 text-right text-[11px] font-semibold uppercase tracking-[0.14em]">Small files</th>
                <th class="px-4 py-3 text-right text-[11px] font-semibold uppercase tracking-[0.14em]">Applicable equality deletes</th>
                <th class="px-4 py-3 text-right text-[11px] font-semibold uppercase tracking-[0.14em]">Applicable position deletes</th>
                <th class="min-w-44 px-4 py-3 text-left text-[11px] font-semibold uppercase tracking-[0.14em]">Current / trigger</th>
                <th class="min-w-32 px-4 py-3 text-left text-[11px] font-semibold uppercase tracking-[0.14em]">Status</th>
                <th class="min-w-64 px-4 py-3 text-left text-[11px] font-semibold uppercase tracking-[0.14em]">Maintenance reason</th>
              </tr>
            </thead>
            <tbody class="bg-white">
              ${tableRows || '<tr><td colspan="8" class="px-4 py-8 text-center text-sm text-slate-500">No Iceberg table inventory is available yet.</td></tr>'}
            </tbody>
          </table>
        </div>
      </div>
      ${pagination ? `<div class="mt-3 flex justify-end">${pagination}</div>` : ''}
    </div>
    `}
    </div>
  `);

  panel.querySelectorAll('[data-maintenance-view]').forEach((button) => {
    button.onclick = () => {
      panel.dataset.maintenanceView = button.dataset.maintenanceView;
      renderIcebergMaintenance(job, options);
    };
    button.onkeydown = (event) => {
      if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
      event.preventDefault();
      panel.dataset.maintenanceView = event.key === 'Home' ? 'compaction' : event.key === 'End' ? 'cleanup'
        : panel.dataset.maintenanceView === 'cleanup' ? 'compaction' : 'cleanup';
      renderIcebergMaintenance(job, options);
      panel.querySelector(`[data-maintenance-view="${panel.dataset.maintenanceView}"]`)?.focus();
    };
  });
  if (cleanupSelected) bindCleanupControls(panel, pageOwner, () => renderIcebergMaintenance(job, options));

  const refreshButton = panel.querySelector('[data-maintenance-inventory-refresh]');
  if (refreshButton && typeof options.onRefreshInventory === 'function') {
    refreshButton.onclick = async () => {
      refreshButton.disabled = true;
      refreshButton.textContent = 'Queueing scan...';
      try {
        await options.onRefreshInventory();
      } finally {
        refreshButton.disabled = false;
        refreshButton.textContent = refreshLabel;
      }
    };
  }

  panel.querySelectorAll('[data-maintenance-page]').forEach((button) => {
    button.onclick = () => {
      panel.dataset.maintenancePage = button.dataset.maintenancePage;
      renderIcebergMaintenance(job, options);
    };
  });
}
