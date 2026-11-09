import { apiFetch, escapeHtml, fmtBytes, fmtWholeNumber, formatDateTime, formatDuration } from './core/api.js';

const historyStates = new WeakMap();
const renderCallbacks = new WeakMap();
const operations = { remove_orphan_files: 'Orphan cleanup', expire_snapshots: 'Snapshot expiration' };
const labels = {
  scheduled: 'Scheduled', due: 'Due', queued: 'Queued', running: 'Running', retry: 'Retry pending',
  failed: 'Failed', skipped: 'Skipped', cancelled: 'Cancelled', paused: 'Paused', disabled: 'Disabled',
  lease_expired: 'Lease expired', waiting_for_snapshot: 'Waiting for snapshot', unscheduled: 'Not scheduled',
  succeeded: 'Succeeded', unavailable: 'Unavailable',
};

function badge(status) {
  const tone = ['failed', 'lease_expired'].includes(status) ? 'border-rose-200 bg-rose-50 text-rose-700'
    : ['running', 'queued'].includes(status) ? 'border-blue-200 bg-blue-50 text-blue-700'
      : ['due', 'retry', 'skipped'].includes(status) ? 'border-amber-200 bg-amber-50 text-amber-700'
        : status === 'succeeded' ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
          : 'border-slate-200 bg-slate-50 text-slate-600';
  return `<span class="inline-flex rounded-full border px-2.5 py-1 text-[11px] font-semibold ${tone}">${escapeHtml(labels[status] || status)}</span>`;
}

function date(value, empty = 'Not recorded') {
  return escapeHtml(value ? formatDateTime(value) : empty);
}

function resultCounts(result, operation) {
  if (!result) return '<span class="text-slate-500">No result recorded</span>';
  if (result.status === 'running') return '<span class="text-slate-500">In progress</span>';
  if (result.dry_run) return '<span class="text-slate-500">Dry run · no files deleted</span>';
  if (operation === 'expire_snapshots') {
    return `${fmtWholeNumber(result.expired_snapshots || 0)} snapshots expired<div class="mt-1 text-[11px] text-slate-500">Deleted files / bytes not measured</div>`;
  }
  return result.deleted_files == null || result.deleted_bytes == null
    ? '<span class="text-slate-500">Deleted files / bytes not measured</span>'
    : `${fmtWholeNumber(result.deleted_files)} files<div class="mt-1 text-[11px] text-slate-500">${escapeHtml(fmtBytes(result.deleted_bytes))}</div>`;
}

function historyState(panel, owner) {
  let state = historyStates.get(panel);
  if (!state || state.owner !== owner) {
    state = { owner, key: '', operation: '', loading: false, error: '', results: [], request: 0 };
    historyStates.set(panel, state);
  }
  return state;
}

function historyRow(state, operation) {
  const body = state.loading ? 'Loading cleanup history…'
    : state.error ? `<span class="text-rose-700">${escapeHtml(state.error)}</span>`
      : state.results.length ? `<div class="mb-3 text-xs font-semibold text-slate-700">Latest ${state.results.length} recorded attempts</div>
        <div class="overflow-auto"><table class="min-w-full text-xs"><thead><tr class="text-left text-slate-500">
        <th class="pb-2 pr-4">Recorded at</th><th class="pb-2 pr-4">Result</th><th class="pb-2 pr-4">Reclaimed</th><th class="pb-2 pr-4">Duration</th><th class="pb-2">Reason</th></tr></thead>
        <tbody>${state.results.map((r) => `<tr class="align-top"><td class="py-2 pr-4 whitespace-nowrap">${date(r.created_at)}</td>
          <td class="py-2 pr-4">${badge(r.status)}<div class="mt-1 text-slate-500">Run #${fmtWholeNumber(r.run_id)} · ${escapeHtml(r.engine)}</div></td>
          <td class="py-2 pr-4">${resultCounts(r, operation)}</td><td class="py-2 pr-4 whitespace-nowrap">${escapeHtml(formatDuration(Number(r.duration_ms || 0) / 1000))}</td>
          <td class="py-2 max-w-md break-words">${escapeHtml(r.error || r.reason || '—')}</td></tr>`).join('')}</tbody></table></div>`
        : 'No cleanup history recorded for this table and operation.';
  return `<tr class="border-b border-slate-100 bg-slate-50"><td colspan="6" class="px-4 py-4">${body}</td></tr>`;
}

export function cleanupContent(maintenance, panel, owner, metric, pageSize = 100) {
  const op = panel.dataset.cleanupOperation || 'remove_orphan_files';
  const query = (panel.dataset.cleanupSearch || '').trim().toLowerCase();
  const filter = panel.dataset.cleanupStatus || '';
  const history = historyState(panel, owner);
  const all = (maintenance.tables || []).map((table) => ({ table,
    cleanup: (table.cleanup || []).find((item) => item.operation === op) || {
      operation: op, status: 'unavailable', reason: 'Cleanup monitoring is unavailable for this table.',
    },
  }));
  const rows = all.filter(({ table, cleanup }) => (!query || String(table.identifier).toLowerCase().includes(query))
    && (!filter || cleanup.status === filter));
  const priority = { running: 0, failed: 1, lease_expired: 2, retry: 3, queued: 4, due: 5, skipped: 6 };
  rows.sort((a, b) => (priority[a.cleanup.status] ?? 9) - (priority[b.cleanup.status] ?? 9)
    || (Date.parse(a.cleanup.next_due_at) || Infinity) - (Date.parse(b.cleanup.next_due_at) || Infinity)
    || String(a.table.identifier).localeCompare(String(b.table.identifier)));
  const pages = Math.max(1, Math.ceil(rows.length / pageSize));
  const page = Math.min(pages, Math.max(1, parseInt(panel.dataset.cleanupPage || '1', 10) || 1));
  panel.dataset.cleanupPage = String(page);
  const start = (page - 1) * pageSize;
  const count = (...statuses) => all.filter(({ cleanup }) => statuses.includes(cleanup.status)).length;
  const limited = Number(maintenance.tables_total || 0) > all.length;
  const summary = limited ? 'Across returned tables' : 'Across monitored tables';
  const pagination = pages > 1 ? `<div class="flex items-center gap-2">
    <button type="button" class="brand-outline-btn rounded-md px-3 py-1.5 text-xs font-semibold" data-cleanup-page="${page - 1}" ${page === 1 ? 'disabled' : ''}>Previous</button>
    <span class="mono text-xs text-slate-500">${page} / ${pages}</span>
    <button type="button" class="brand-outline-btn rounded-md px-3 py-1.5 text-xs font-semibold" data-cleanup-page="${page + 1}" ${page === pages ? 'disabled' : ''}>Next</button></div>` : '';
  return `<div class="grid gap-3 bg-slate-50 px-5 py-4 sm:grid-cols-2 xl:grid-cols-5 sm:px-6">
    ${metric('Scheduled', fmtWholeNumber(count('scheduled')), summary)}
    ${metric('Due', fmtWholeNumber(count('due')), 'Waiting for scheduling')}
    ${metric('Queued / retry', fmtWholeNumber(count('queued', 'retry')), 'Waiting for a worker or retry time')}
    ${metric('Running', fmtWholeNumber(count('running')), operations[op])}
    ${metric('Failed / expired leases', fmtWholeNumber(count('failed', 'lease_expired')), 'Latest task needs attention')}
    </div>
    <div class="px-5 py-4 sm:px-6">
      <div class="mb-3 flex flex-wrap items-end gap-3">
        <label class="text-xs text-slate-600">Operation<select data-cleanup-operation class="mt-1 block rounded-md border border-slate-300 bg-white px-3 py-2 text-xs">${Object.entries(operations).map(([value, label]) => `<option value="${value}" ${value === op ? 'selected' : ''}>${label}</option>`).join('')}</select></label>
        <label class="text-xs text-slate-600">Find table<input type="search" data-cleanup-search value="${escapeHtml(panel.dataset.cleanupSearch || '')}" placeholder="Catalog, schema or table" class="mt-1 block w-64 max-w-full rounded-md border border-slate-300 px-3 py-2 text-xs"></label>
        <label class="text-xs text-slate-600">Status<select data-cleanup-status class="mt-1 block rounded-md border border-slate-300 bg-white px-3 py-2 text-xs"><option value="">All statuses</option>${Object.entries(labels).filter(([v]) => v !== 'succeeded').map(([value, label]) => `<option value="${value}" ${value === filter ? 'selected' : ''}>${label}</option>`).join('')}</select></label>
      </div>
      <p class="mb-4 text-xs leading-5 text-slate-500">Next due is the scheduler deadline, not a guaranteed start time. Cleanup has its own status, independent of compaction inventory. Times use your browser timezone. Results below come from Rivus worker records.</p>
      <div class="mb-3 flex flex-wrap items-center justify-between gap-2"><span class="text-xs text-slate-500">Rows ${rows.length ? start + 1 : 0}–${Math.min(start + pageSize, rows.length)} of ${fmtWholeNumber(rows.length)}${limited ? ` shown (${fmtWholeNumber(maintenance.tables_total)} total tables)` : ''}</span>${pagination}</div>
      <div class="overflow-hidden rounded-[16px] border border-slate-200"><div class="max-h-[560px] overflow-auto" data-scroll-key="${escapeHtml(JSON.stringify([owner, 'cleanup', op, page, query, filter]))}"><table class="min-w-full text-xs">
        <thead class="sticky top-0 z-10 bg-slate-50 text-slate-600"><tr class="border-b border-slate-200">${['Iceberg table', 'Last successful cleanup', 'Next due', 'Status', 'Latest attempt result', 'Reason'].map((label) => `<th class="px-4 py-3 text-left text-[11px] font-semibold uppercase tracking-[0.14em]">${label}</th>`).join('')}</tr></thead>
        <tbody class="bg-white">${rows.slice(start, start + pageSize).map(({ table, cleanup: c }) => {
          const key = table.table_key || table.identifier;
          const open = history.key === key && history.operation === op;
          return `<tr class="border-b border-slate-100 align-top"><td class="min-w-64 px-4 py-3"><div class="mono font-semibold text-slate-800">${escapeHtml(table.identifier)}</div><div class="mt-1 text-slate-500">${operations[op]}</div>
          <button type="button" data-cleanup-history="${escapeHtml(key)}" aria-expanded="${open}" class="brand-outline-btn mt-2 whitespace-nowrap rounded-md px-3 py-1.5 text-xs font-semibold">${open ? 'Hide history' : 'View history'}</button></td>
          <td class="min-w-32 px-4 py-3">${date(c.last_success_at, 'No successful run recorded')}</td>
          <td class="min-w-40 px-4 py-3">${date(c.next_due_at, 'Not scheduled')}${c.not_before ? `<div class="mt-1 text-[11px] text-slate-500">Task eligible: ${date(c.not_before)}</div>` : ''}</td>
          <td class="px-4 py-3">${badge(c.status)}</td>
          <td class="min-w-44 px-4 py-3">${resultCounts(c.last_result, op)}${c.last_result ? `<div class="mt-2 text-[11px] text-slate-500">${escapeHtml(labels[c.last_result.status] || c.last_result.status)} · ${date(c.last_result.created_at)}</div>` : ''}</td>
          <td class="min-w-44 max-w-md break-words px-4 py-3 text-slate-600">${escapeHtml(c.reason || '—')}</td></tr>${open ? historyRow(history, op) : ''}`;
        }).join('') || '<tr><td colspan="6" class="px-4 py-8 text-center text-slate-500">No tables match these cleanup filters.</td></tr>'}</tbody>
      </table></div></div>${pagination ? `<div class="mt-3 flex justify-end">${pagination}</div>` : ''}
    </div>`;
}

export function bindCleanupControls(panel, owner, rerender) {
  renderCallbacks.set(panel, rerender);
  for (const [attribute, datasetKey] of [['operation', 'cleanupOperation'], ['status', 'cleanupStatus']]) {
    const element = panel.querySelector(`[data-cleanup-${attribute}]`);
    if (element) element.onchange = () => { panel.dataset[datasetKey] = element.value; panel.dataset.cleanupPage = '1'; rerender(); };
  }
  const search = panel.querySelector('[data-cleanup-search]');
  if (search) search.oninput = () => {
    const start = search.selectionStart, end = search.selectionEnd;
    panel.dataset.cleanupSearch = search.value;
    panel.dataset.cleanupPage = '1';
    rerender();
    const replacement = panel.querySelector('[data-cleanup-search]');
    replacement?.focus();
    if (start != null) replacement?.setSelectionRange(start, end);
  };
  panel.querySelectorAll('[data-cleanup-page]').forEach((button) => {
    button.onclick = () => { panel.dataset.cleanupPage = button.dataset.cleanupPage; rerender(); };
  });
  panel.querySelectorAll('[data-cleanup-history]').forEach((button) => {
    button.onclick = async () => {
      const state = historyState(panel, owner);
      const request = ++state.request;
      const key = button.dataset.cleanupHistory;
      const operation = panel.dataset.cleanupOperation || 'remove_orphan_files';
      if (state.key === key && state.operation === operation) { state.key = ''; rerender(); return; }
      Object.assign(state, { key, operation, loading: true, error: '', results: [] });
      rerender();
      try {
        const query = new URLSearchParams({ job_id: owner, table_key: key, operation, limit: '10' });
        const response = await apiFetch(`/api/iceberg/maintenance/cleanup/history?${query}`);
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || 'Unable to load cleanup history.');
        if (state.request !== request || state.key !== key || state.operation !== operation || historyStates.get(panel) !== state) return;
        state.results = Array.isArray(data.results) ? data.results : [];
      } catch (error) {
        if (state.request !== request || state.key !== key || state.operation !== operation || historyStates.get(panel) !== state) return;
        state.error = error.message || 'Unable to load cleanup history.';
      }
      state.loading = false;
      if (panel.dataset.maintenancePageOwner === owner) renderCallbacks.get(panel)?.();
    };
  });
}
