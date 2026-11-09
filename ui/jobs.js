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

export function initJobs(handlers = {}) {
  refreshDashboard = handlers.refreshDashboard || refreshDashboard;
  showJobDetails = handlers.showJobDetails || showJobDetails;
  showLogsForJob = handlers.showLogsForJob || showLogsForJob;
  showErrors = handlers.showErrors || showErrors;

  if (initialized) return;
  initialized = true;
  document.addEventListener('click', handleJobClick);
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

  const btnBase = 'whitespace-nowrap rounded-md border px-3 py-2 text-xs font-semibold transition';
  const btnCancel = `${btnBase} border-amber-200 text-amber-800 hover:bg-amber-50`;
  const btnPause = `${btnBase} border-violet-200 text-violet-800 hover:bg-violet-50`;
  const btnDelete = `${btnBase} border-rose-200 text-rose-700 hover:bg-rose-50`;
  const btnError = `${btnBase} border-rose-200 bg-rose-50 text-rose-700 hover:bg-rose-100`;
  const btnDetail = `${btnBase} border-blue-300 text-blue-800 hover:bg-blue-50`;
  const btnLogs = `${btnBase} border-slate-300 text-slate-700 hover:bg-slate-50`;
  const btnResubmit = `${btnBase} border-blue-200 bg-blue-50 text-blue-800 hover:bg-blue-100`;

  return `
    <div class="flex min-w-max flex-nowrap items-center justify-end gap-2">
      <button type="button" class="${btnDetail}" data-job-action="details" data-job-id="${idAttr}">Details</button>
      <button type="button" class="${btnDetail}" data-job-action="rename" data-job-name="${escapeHtml(job.name || '')}" data-job-id="${idAttr}">Rename</button>
      <button type="button" class="${btnLogs}" data-job-action="logs" data-job-id="${idAttr}">Logs</button>
      ${canPause ? `<button type="button" class="${btnPause}" data-job-action="pause" data-job-id="${idAttr}">Pause</button>` : ''}
      ${canCancel ? `<button type="button" class="${btnCancel}" data-job-action="cancel" data-job-id="${idAttr}">Cancel</button>` : ''}
      ${canResubmit ? `<button type="button" class="${btnResubmit}" data-job-action="resubmit" data-job-id="${idAttr}">Resubmit</button>` : ''}
      <button type="button" class="${btnDelete}" data-job-action="delete" data-job-id="${idAttr}">Delete</button>
      ${showErr ? `<button type="button" class="${btnError}" data-job-action="errors" data-job-id="${idAttr}">Errors${errorCount > 0 ? ` (${errorCount})` : ''}</button>` : ''}
    </div>
  `;
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
        <div class="max-w-[34rem] md:min-w-[22rem]">
          <div class="font-semibold leading-6 text-slate-900 break-words">${escapeHtml(display.title)}</div>
          ${displayDetail ? `<div class="mt-1 text-xs leading-5 text-slate-500 break-words">${escapeHtml(displayDetail)}</div>` : ''}
          ${renderJobProgress(job)}
        </div>
      </td>
      <td data-label="Status" class="px-6 py-4">
        ${statusPill(shownStatus)}
      </td>
      <td data-label="Created" class="px-6 py-4 text-slate-600">${tableDateCell(job.created)}</td>
      <td data-label="Updated" class="px-6 py-4 text-slate-600">${tableDateCell(job.updated)}</td>
      <td data-label="Actions" class="cell-actions px-6 py-4 md:min-w-[24rem]">${actionButtons(job)}</td>
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

  if (action === 'rename') renameJob(jobId, jobButton.dataset.jobName || '');
  if (action === 'details') showJobDetails(jobId);
  if (action === 'logs') showLogsForJob(jobId);
  if (action === 'pause') pauseJob(jobId);
  if (action === 'cancel') cancelJob(jobId);
  if (action === 'resubmit') resubmitJob(jobId);
  if (action === 'delete') deleteJob(jobId);
  if (action === 'errors') showErrors(jobId);
}
