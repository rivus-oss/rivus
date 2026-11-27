import {
  apiFetch,
  escapeHtml,
  fmtWholeNumber,
  idChip,
  jobDisplay,
  jobRuntimeProgress,
  markSynced,
  operationErrorMessage,
  progressMetaParts,
  progressPill,
  setBoundText,
  setInnerHTMLIfChanged,
  setJobsNotice,
  setModalState,
  setSubmitStatus,
  sleep,
  statusPill,
  tableDateCell,
} from './core/api.js';

export const ACTIVE = new Set(['RUNNING', 'PAUSING', 'PENDING', 'QUEUED', 'STOPPING']);
const RESUMABLE = new Set(['STOPPED', 'PAUSED']);

let latestDorisArchiveJobs = [];
let latestIcebergArchiveJobs = [];
let bulkDeleteInFlight = false;
let bulkResumeInFlight = false;
let refreshDashboard = async () => {};
let showJobDetails = () => {};
let showLogsForJob = () => {};
let showErrors = () => {};
let initialized = false;
let openJobMenu = null;

export function initJobs(handlers = {}) {
  refreshDashboard = handlers.refreshDashboard || refreshDashboard;
  showJobDetails = handlers.showJobDetails || showJobDetails;
  showLogsForJob = handlers.showLogsForJob || showLogsForJob;
  showErrors = handlers.showErrors || showErrors;

  if (initialized) return;
  initialized = true;
  document.addEventListener('click', handleJobClick);
  document.addEventListener('keydown', handleJobMenuKeydown);
  document.addEventListener('scroll', (event) => {
    if (!openJobMenu || openJobMenu.element.contains(event.target)) return;
    const rect = openJobMenu.trigger.getBoundingClientRect();
    const anchor = openJobMenu.anchorRect;
    // A click may scroll the trigger into view before opening the menu, while
    // its scroll event arrives afterwards. Only dismiss if the anchor moved.
    if (anchor && (Math.abs(rect.top - anchor.top) > 1 || Math.abs(rect.left - anchor.left) > 1)) closeJobMenu();
  }, true);
  window.addEventListener('resize', () => closeJobMenu());
}

export function isBulkBusy() {
  return bulkDeleteInFlight || bulkResumeInFlight;
}

function actionButtons(job) {
  const errorCount = Number(job.error_count || 0);
  const isActive = ACTIVE.has(job.status);
  const showErr = !isActive && (job.status === 'FAILED' || errorCount > 0);
  const canPause = job.status === 'RUNNING';
  const canCancel = isActive;
  const canResubmit = job.status === 'FAILED' || job.status === 'STOPPED' || job.status === 'PAUSED';
  const idAttr = escapeHtml(job.id ?? '');

  const item = (action, label, tone = '') => `<button type="button" role="menuitem" tabindex="-1" class="job-menu-item ${tone}" data-job-action="${action}" data-job-id="${idAttr}"${action === 'rename' ? ` data-job-name="${escapeHtml(job.name || '')}"` : ''}>${label}</button>`;

  return `
    <div class="job-actions">
      <button type="button" class="whitespace-nowrap rounded-md border border-blue-300 px-3 py-2 text-xs font-semibold text-blue-800 transition hover:bg-blue-50" data-job-action="details" data-job-id="${idAttr}">Details</button>
      <button type="button" class="job-menu-trigger" data-job-menu-trigger data-job-id="${idAttr}" aria-label="More actions for ${idAttr}" aria-haspopup="menu" aria-expanded="false">
        <svg viewBox="0 0 24 24" aria-hidden="true" fill="currentColor"><circle cx="12" cy="5" r="1.8"/><circle cx="12" cy="12" r="1.8"/><circle cx="12" cy="19" r="1.8"/></svg>
      </button>
      <template data-job-menu-content>
        ${item('logs', 'Logs')}
        ${item('rename', 'Rename')}
        ${showErr ? item('errors', `Errors${errorCount > 0 ? ` (${errorCount})` : ''}`) : ''}
        <div class="job-menu-divider" role="separator"></div>
        ${canPause ? item('pause', 'Pause') : ''}
        ${canCancel ? item('cancel', 'Cancel', 'job-menu-warning') : ''}
        ${canResubmit ? item('resubmit', 'Resubmit') : ''}
        ${item('delete', 'Delete', 'job-menu-danger')}
      </template>
    </div>
  `;
}

function closeJobMenu(returnFocus = false) {
  if (!openJobMenu) return;
  const { trigger, element } = openJobMenu;
  openJobMenu = null;
  trigger.setAttribute('aria-expanded', 'false');
  trigger.removeAttribute('aria-controls');
  element.remove();
  if (returnFocus && trigger.isConnected) trigger.focus({ preventScroll: true });
}

function positionJobMenu() {
  if (!openJobMenu) return;
  const { trigger, element } = openJobMenu;
  if (!trigger.isConnected || !trigger.getClientRects().length) {
    closeJobMenu();
    return;
  }
  const rect = trigger.getBoundingClientRect();
  openJobMenu.anchorRect = { top: rect.top, left: rect.left };
  const menu = element.getBoundingClientRect();
  const gap = 8;
  const left = Math.max(gap, Math.min(rect.right - menu.width, window.innerWidth - menu.width - gap));
  const top = rect.bottom + gap + menu.height <= window.innerHeight - gap
    ? rect.bottom + gap
    : Math.max(gap, rect.top - menu.height - gap);
  element.style.left = `${left}px`;
  element.style.top = `${top}px`;
}

function showJobMenu(trigger, focusLast = false) {
  if (openJobMenu?.trigger === trigger) {
    closeJobMenu(true);
    return;
  }
  closeJobMenu();
  const template = trigger.parentElement.querySelector('[data-job-menu-content]');
  if (!template) return;
  const element = document.createElement('div');
  element.id = 'jobActionsMenu';
  element.className = 'job-action-menu';
  element.setAttribute('role', 'menu');
  element.setAttribute('aria-label', 'Job actions');
  element.innerHTML = template.innerHTML;
  document.body.appendChild(element);
  trigger.setAttribute('aria-expanded', 'true');
  trigger.setAttribute('aria-controls', element.id);
  openJobMenu = { trigger, element, jobId: trigger.dataset.jobId };
  positionJobMenu();
  const items = element.querySelectorAll('[role="menuitem"]');
  (focusLast ? items[items.length - 1] : items[0])?.focus({ preventScroll: true });
}

// The menu lives outside the scrolling table. Rebind its anchor and actions
// after polling replaces rows, preserving the focused action when possible.
function refreshJobMenu() {
  if (!openJobMenu) return;
  const { element, jobId } = openJobMenu;
  const trigger = Array.from(document.querySelectorAll('[data-job-menu-trigger]'))
    .find((button) => button.dataset.jobId === jobId);
  if (!trigger) {
    closeJobMenu();
    return;
  }
  const focused = element.contains(document.activeElement);
  const action = focused ? document.activeElement.dataset.jobAction : null;
  openJobMenu.trigger = trigger;
  trigger.setAttribute('aria-expanded', 'true');
  trigger.setAttribute('aria-controls', element.id);
  element.innerHTML = trigger.parentElement.querySelector('[data-job-menu-content]').innerHTML;
  positionJobMenu();
  if (focused && openJobMenu) {
    const items = Array.from(element.querySelectorAll('[role="menuitem"]'));
    (items.find((item) => item.dataset.jobAction === action) || items[0])?.focus({ preventScroll: true });
  }
}

function handleJobMenuKeydown(event) {
  const trigger = event.target.closest('[data-job-menu-trigger]');
  if (trigger && (event.key === 'ArrowDown' || event.key === 'ArrowUp')) {
    event.preventDefault();
    if (!openJobMenu || openJobMenu.trigger !== trigger) showJobMenu(trigger, event.key === 'ArrowUp');
    else {
      const items = openJobMenu.element.querySelectorAll('[role="menuitem"]');
      (event.key === 'ArrowUp' ? items[items.length - 1] : items[0])?.focus();
    }
    return;
  }
  if (!openJobMenu) return;
  if (event.key === 'Escape') {
    event.preventDefault();
    event.stopPropagation();
    closeJobMenu(true);
    return;
  }
  if (event.key === 'Tab') {
    closeJobMenu(true);
    return;
  }
  if (!openJobMenu.element.contains(event.target)) return;
  const items = Array.from(openJobMenu.element.querySelectorAll('[role="menuitem"]'));
  let next = items.indexOf(document.activeElement);
  if (event.key === 'ArrowDown') next = (next + 1) % items.length;
  else if (event.key === 'ArrowUp') next = (next - 1 + items.length) % items.length;
  else if (event.key === 'Home') next = 0;
  else if (event.key === 'End') next = items.length - 1;
  else return;
  event.preventDefault();
  items[next]?.focus();
}

function renderJobProgress(job) {
  const progress = jobRuntimeProgress(job);
  if (!progress) {
    if (!ACTIVE.has(job?.status)) return '';
    return `
      <div class="mt-3 text-xs text-slate-500">
        Waiting for runtime progress...
      </div>
    `;
  }

  const summary = String(progress.summary || '').trim() || 'Waiting for update';
  const metaParts = progressMetaParts(progress);

  return `
    <div class="mt-3 flex flex-wrap items-center gap-2 text-xs">
      ${progressPill(progress)}
      <div class="font-semibold text-slate-700">${escapeHtml(summary)}</div>
      ${metaParts.length > 0 ? `<div class="mono text-[11px] text-slate-500">${escapeHtml(metaParts.join(' | '))}</div>` : ''}
    </div>
  `;
}

function rowTemplate(job) {
  const id = job.id ?? '-';
  const display = jobDisplay(job.name ?? '-');
  const displayDetail = String(display.detail || '').trim();
  const healthStatus = String(job.health_status || '').trim().toUpperCase();
  const shownStatus = healthStatus && healthStatus !== 'HEALTHY' ? healthStatus : job.status;

  return `
    <tr class="align-top transition hover:bg-white/70">
      <td data-label="ID" class="px-6 py-4">${idChip(id)}</td>
      <td data-label="Job" class="px-6 py-4">
        <div class="max-w-[34rem] md:min-w-[16rem]">
          <div class="font-semibold leading-6 text-slate-900 break-words">${escapeHtml(display.title)}</div>
          ${displayDetail ? `<div class="mt-1 text-xs leading-5 text-slate-500 break-words">${escapeHtml(displayDetail)}</div>` : ''}
          ${renderJobProgress(job)}
        </div>
      </td>
      <td data-label="Status" class="px-6 py-4">
        ${statusPill(shownStatus)}
      </td>
      <td data-label="Created" class="px-6 py-4 text-slate-600 whitespace-nowrap">${tableDateCell(job.created)}</td>
      <td data-label="Updated" class="px-6 py-4 text-slate-600 whitespace-nowrap">${tableDateCell(job.updated)}</td>
      <td data-label="Actions" class="cell-actions px-6 py-4">${actionButtons(job)}</td>
    </tr>
  `;
}

function partitionJobsByDomain(jobs) {
  const standardActive = [];
  const standardArchive = [];
  for (const job of jobs) {
    const target = ACTIVE.has(job.status) ? standardActive : standardArchive;
    target.push(job);
  }
  return { standardActive, standardArchive };
}

function normalizedSinkType(job) {
  return String(job?.sink_type || '').trim().toLowerCase();
}

function isDorisJob(job) {
  return normalizedSinkType(job) === 'doris';
}

function isIcebergJob(job) {
  return normalizedSinkType(job).startsWith('iceberg');
}

function compareJobsForDisplay(a, b) {
  const updatedA = String(a?.updated || '');
  const updatedB = String(b?.updated || '');
  if (updatedA !== updatedB) return updatedB.localeCompare(updatedA);
  const createdA = String(a?.created || '');
  const createdB = String(b?.created || '');
  if (createdA !== createdB) return createdB.localeCompare(createdA);
  return String(a?.id || '').localeCompare(String(b?.id || ''));
}

function updateArchiveBulkButton(scope, jobs) {
  const button = document.querySelector(`[data-bulk-delete-archive="${scope}"]`);
  if (!button) return;
  const count = jobs.length;
  button.disabled = count === 0 || isBulkBusy();
  button.textContent = count > 0 ? `Delete archive (${count})` : 'Delete archive';
}

function updateArchiveResumeButton(scope, jobs) {
  const button = document.querySelector(`[data-bulk-resume-archive="${scope}"]`);
  if (!button) return;
  const count = jobs.filter((job) => RESUMABLE.has(job?.status)).length;
  button.disabled = count === 0 || isBulkBusy();
  button.textContent = count > 0 ? `Resume all (${count})` : 'Resume all';
}

function updateArchiveBulkButtons(scope, jobs) {
  updateArchiveResumeButton(scope, jobs);
  updateArchiveBulkButton(scope, jobs);
}

export async function loadJobs() {
  try {
    const res = await apiFetch('/api/jobs');
    if (!res.ok) throw new Error(`jobs ${res.status}`);

    const jobs = (await res.json()).slice().sort(compareJobsForDisplay);
    const { standardActive, standardArchive } = partitionJobsByDomain(jobs);
    const dorisActive = standardActive.filter(isDorisJob);
    const icebergActive = standardActive.filter(isIcebergJob);
    const dorisArchive = standardArchive.filter(isDorisJob);
    const icebergArchive = standardArchive.filter(isIcebergJob);
    const otherStandardJobs = jobs.filter((job) => !isDorisJob(job) && !isIcebergJob(job));

    latestDorisArchiveJobs = dorisArchive.slice();
    latestIcebergArchiveJobs = icebergArchive.slice();

    setInnerHTMLIfChanged(document.getElementById('dorisActiveBody'), dorisActive.map(rowTemplate).join(''), { preserveScroll: true });
    setInnerHTMLIfChanged(document.getElementById('icebergActiveBody'), icebergActive.map(rowTemplate).join(''), { preserveScroll: true });
    setInnerHTMLIfChanged(document.getElementById('dorisArchiveBody'), dorisArchive.map(rowTemplate).join(''), { preserveScroll: true });
    setInnerHTMLIfChanged(document.getElementById('icebergArchiveBody'), icebergArchive.map(rowTemplate).join(''), { preserveScroll: true });
    refreshJobMenu();
    document.getElementById('dorisActiveEmpty').classList.toggle('hidden', dorisActive.length !== 0);
    document.getElementById('icebergActiveEmpty').classList.toggle('hidden', icebergActive.length !== 0);
    document.getElementById('dorisArchiveEmpty').classList.toggle('hidden', dorisArchive.length !== 0);
    document.getElementById('icebergArchiveEmpty').classList.toggle('hidden', icebergArchive.length !== 0);
    updateArchiveBulkButtons('doris', dorisArchive);
    updateArchiveBulkButtons('iceberg', icebergArchive);

    setBoundText('active-doris-count', String(dorisActive.length));
    setBoundText('active-iceberg-count', String(icebergActive.length));
    setBoundText('archive-count', String(standardArchive.length));
    setBoundText('total-count', String(standardActive.length + standardArchive.length));

    const otherStandardJobsNotice = document.getElementById('otherStandardJobsNotice');
    if (otherStandardJobs.length > 0) {
      otherStandardJobsNotice.textContent = `${otherStandardJobs.length} standard job(s) use a sink other than doris or iceberg and are not shown in these tabs.`;
      otherStandardJobsNotice.classList.remove('hidden');
    } else {
      otherStandardJobsNotice.textContent = '';
      otherStandardJobsNotice.classList.add('hidden');
    }
    setJobsNotice('');
    markSynced('Synced');
  } catch (err) {
    const otherStandardJobsNotice = document.getElementById('otherStandardJobsNotice');
    otherStandardJobsNotice.textContent = '';
    otherStandardJobsNotice.classList.add('hidden');
    setJobsNotice('Failed to load jobs. Check the API status and try again.', 'error');
  }
}

async function cancelJob(id) {
  if (!confirm('Cancel job ' + id + '?')) return;
  const res = await apiFetch('/api/jobs/' + encodeURIComponent(id) + '/cancel', { method: 'POST' });
  if (!res.ok) {
    alert(await operationErrorMessage(res, 'Cancel'));
    return;
  }
  await refreshDashboard();
}

async function pauseJob(id) {
  if (!confirm('Pause job ' + id + ' after draining and committing pending events?')) return;
  const res = await apiFetch('/api/jobs/' + encodeURIComponent(id) + '/pause', { method: 'POST' });
  if (!res.ok) {
    alert(await operationErrorMessage(res, 'Pause'));
    return;
  }
  await refreshDashboard();
}

async function deleteJob(id) {
  if (!confirm('Delete job ' + id + '? (This removes it from the list)')) return;
  const res = await apiFetch('/api/jobs/' + encodeURIComponent(id), { method: 'DELETE' });
  if (!res.ok) alert(await operationErrorMessage(res, 'Delete'));
  await refreshDashboard();
}

async function deleteJobWithoutPrompt(id) {
  const res = await apiFetch('/api/jobs/' + encodeURIComponent(id), { method: 'DELETE' });
  if (res.status === 404 || res.ok) return null;
  return await operationErrorMessage(res, 'Delete');
}

async function deleteJobsWithLimit(ids, limit = 6) {
  const failures = [];
  let next = 0;
  const workerCount = Math.min(limit, ids.length);
  const workers = Array.from({ length: workerCount }, async () => {
    while (next < ids.length) {
      const id = ids[next];
      next += 1;
      const error = await deleteJobWithoutPrompt(id);
      if (error) failures.push({ id, error });
    }
  });
  await Promise.all(workers);
  return failures;
}

async function resubmitJobWithoutPrompt(id) {
  const res = await apiFetch('/api/jobs/' + encodeURIComponent(id) + '/resubmit', { method: 'POST' });
  if (res.ok) return null;
  return await operationErrorMessage(res, 'Resume');
}

function jobResumeHasSettled(job) {
  const status = String(job?.status || '').toUpperCase();
  if (status === 'QUEUED' || status === 'DONE') return true;
  if (status !== 'RUNNING') return false;
  const phase = String(job?.progress?.phase || '').trim().toLowerCase();
  return phase === 'streaming' || phase === 'cdc_health';
}

async function waitForJobResumeToSettle(id, timeoutMs = 5 * 60 * 1000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    await sleep(2000);
    const res = await apiFetch('/api/jobs/' + encodeURIComponent(id));
    if (!res.ok) return await operationErrorMessage(res, 'Check resumed job');
    const job = await res.json();
    const status = String(job?.status || '').toUpperCase();
    if (status === 'FAILED') return String(job?.last_error?.message || 'job failed during startup');
    if (status === 'STOPPED' || status === 'PAUSED') return `job became ${status} during startup`;
    if (jobResumeHasSettled(job)) return null;
  }
  return 'startup did not settle within 5 minutes';
}

async function resumeJobsStaged(ids, onProgress) {
  const failures = [];
  for (let index = 0; index < ids.length; index += 1) {
    const id = ids[index];
    onProgress?.(index + 1, ids.length, id);
    const resumeError = await resubmitJobWithoutPrompt(id);
    if (resumeError) {
      failures.push({ id, error: resumeError });
      continue;
    }
    const settleError = await waitForJobResumeToSettle(id);
    if (settleError) failures.push({ id, error: settleError });
    await sleep(2000);
  }
  return failures;
}

async function bulkResumeArchive(scope) {
  const jobs = scope === 'iceberg' ? latestIcebergArchiveJobs : latestDorisArchiveJobs;
  const ids = jobs.filter((job) => RESUMABLE.has(job?.status)).map((job) => job?.id).filter(Boolean);
  const label = scope === 'iceberg' ? 'Iceberg' : 'Doris';
  if (ids.length === 0) return;
  if (!confirm(`Resume ${ids.length} archived ${label} job(s)?`)) return;

  bulkResumeInFlight = true;
  updateArchiveBulkButtons('doris', latestDorisArchiveJobs);
  updateArchiveBulkButtons('iceberg', latestIcebergArchiveJobs);
  setJobsNotice(`Resuming ${ids.length} archived ${label} job(s)...`);

  try {
    const failures = await resumeJobsStaged(ids, (current, total, id) => {
      setJobsNotice(`Resuming archived ${label} jobs ${current}/${total}: ${id}`);
    });
    await refreshDashboard();
    if (failures.length > 0) {
      const sample = failures.slice(0, 3).map((item) => item.id).join(', ');
      setJobsNotice(`Resumed ${ids.length - failures.length} job(s). Failed to resume ${failures.length}: ${sample}.`, 'error');
      return;
    }
    setJobsNotice(`Resumed ${ids.length} archived ${label} job(s).`);
  } finally {
    bulkResumeInFlight = false;
    updateArchiveBulkButtons('doris', latestDorisArchiveJobs);
    updateArchiveBulkButtons('iceberg', latestIcebergArchiveJobs);
  }
}

async function bulkDeleteArchive(scope) {
  const jobs = scope === 'iceberg' ? latestIcebergArchiveJobs : latestDorisArchiveJobs;
  const ids = jobs.map((job) => job?.id).filter(Boolean);
  const label = scope === 'iceberg' ? 'Iceberg' : 'Doris';
  if (ids.length === 0) return;
  if (!confirm(`Delete ${ids.length} archived ${label} job(s)? This removes them from the list.`)) return;

  bulkDeleteInFlight = true;
  updateArchiveBulkButtons('doris', latestDorisArchiveJobs);
  updateArchiveBulkButtons('iceberg', latestIcebergArchiveJobs);
  setJobsNotice(`Deleting ${ids.length} archived ${label} job(s)...`);

  try {
    const failures = await deleteJobsWithLimit(ids);
    await refreshDashboard();
    if (failures.length > 0) {
      const sample = failures.slice(0, 3).map((item) => item.id).join(', ');
      setJobsNotice(`Deleted ${ids.length - failures.length} job(s). Failed to delete ${failures.length}: ${sample}.`, 'error');
      return;
    }
    setJobsNotice(`Deleted ${ids.length} archived ${label} job(s).`);
  } finally {
    bulkDeleteInFlight = false;
    updateArchiveBulkButtons('doris', latestDorisArchiveJobs);
    updateArchiveBulkButtons('iceberg', latestIcebergArchiveJobs);
  }
}

async function resubmitJob(id) {
  if (!confirm('Resubmit job ' + id + '?')) return;
  const res = await apiFetch('/api/jobs/' + encodeURIComponent(id) + '/resubmit', { method: 'POST' });
  if (!res.ok) {
    alert(await operationErrorMessage(res, 'Resubmit'));
    return;
  }
  await refreshDashboard();
}

export function openSubmitModal() {
  setSubmitStatus('Ready for a new YAML definition.');
  setModalState('submitModal', true);
  setTimeout(() => document.getElementById('yamlInput')?.focus(), 0);
}

export function closeSubmitModal() {
  setModalState('submitModal', false);
}

export function clearYaml() {
  document.getElementById('yamlInput').value = '';
  document.getElementById('yamlFileInput').value = '';
  setSubmitStatus('Editor cleared.');
}

export async function loadYamlFile(event) {
  const file = event.target.files && event.target.files[0];
  if (!file) return;
  try {
    const text = await file.text();
    document.getElementById('yamlInput').value = text;
    setSubmitStatus('Loaded file: ' + file.name);
  } catch (error) {
    setSubmitStatus('Failed to read file: ' + error.message, 'error');
  }
}

export async function submitJob() {
  const yaml = document.getElementById('yamlInput').value;
  if (!yaml.trim()) {
    setSubmitStatus('YAML input is empty.', 'error');
    return;
  }

  const button = document.getElementById('submitButton');
  button.disabled = true;
  button.classList.add('opacity-80', 'cursor-wait');
  setSubmitStatus('Submitting job...', 'muted');

  try {
    const res = await apiFetch('/api/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-yaml' },
      body: yaml,
    });

    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      setSubmitStatus('Submit failed: ' + (data.error || res.status), 'error');
      return;
    }

    if (String(data.status || '').toUpperCase() === 'FAILED') {
      setSubmitStatus('Submitted but job failed: ' + (data.error || 'open the failed job details for the error.'), 'error');
      await refreshDashboard();
      return;
    }

    if (data.batch) {
      const counts = data.counts || {};
      const failed = Number(counts.failed || 0);
      const message = `Jobs file submitted: ${counts.submitted || 0} submitted (${counts.queued || 0} queued), ${counts.skipped || 0} skipped, ${failed} failed.`;
      setSubmitStatus(message, failed > 0 ? 'error' : 'success');
      await refreshDashboard();
      if (failed === 0) closeSubmitModal();
      return;
    }

    if (data.action === 'skipped') {
      setSubmitStatus('Skipped existing job: ' + (data.id || '-'), 'success');
      await refreshDashboard();
      closeSubmitModal();
      return;
    }

    setSubmitStatus('Submitted: ' + JSON.stringify(data), 'success');
    await refreshDashboard();
    closeSubmitModal();
  } finally {
    button.disabled = false;
    button.classList.remove('opacity-80', 'cursor-wait');
  }
}

async function renameJob(id,currentName) {
 const name=prompt('Job name',currentName);
 if(name===null) return;
 if(!name.trim()){setJobsNotice('Job name cannot be empty.','error');return;}
 const res=await apiFetch('/api/jobs/'+encodeURIComponent(id)+'/name',{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:name.trim()})});
 if(!res.ok){setJobsNotice(await operationErrorMessage(res,'Rename'),'error');return;}
 await refreshDashboard();setJobsNotice('Job name updated.');
}

function handleJobClick(event) {
  const menuTrigger = event.target.closest('[data-job-menu-trigger]');
  if (menuTrigger) {
    showJobMenu(menuTrigger);
    return;
  }
  if (openJobMenu && !openJobMenu.element.contains(event.target)) closeJobMenu();
  const bulkArchiveButton = event.target.closest('[data-bulk-delete-archive]');
  if (bulkArchiveButton) {
    bulkDeleteArchive(bulkArchiveButton.dataset.bulkDeleteArchive);
    return;
  }

  const bulkResumeButton = event.target.closest('[data-bulk-resume-archive]');
  if (bulkResumeButton) {
    bulkResumeArchive(bulkResumeButton.dataset.bulkResumeArchive);
    return;
  }

  const jobButton = event.target.closest('[data-job-action]');
  if (!jobButton) return;

  const jobId = jobButton.dataset.jobId;
  const action = jobButton.dataset.jobAction;
  if (!jobId || !action) return;
  closeJobMenu();

  if (action === 'rename') renameJob(jobId, jobButton.dataset.jobName || '');
  if (action === 'details') showJobDetails(jobId);
  if (action === 'logs') showLogsForJob(jobId);
  if (action === 'pause') pauseJob(jobId);
  if (action === 'cancel') cancelJob(jobId);
  if (action === 'resubmit') resubmitJob(jobId);
  if (action === 'delete') deleteJob(jobId);
  if (action === 'errors') showErrors(jobId);
}
