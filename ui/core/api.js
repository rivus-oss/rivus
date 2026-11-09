const MODAL_IDS = ['submitModal', 'errorsModal', 'maintenanceMonitorModal'];
const syncFormatter = new Intl.DateTimeFormat(undefined, {
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
});
const integerFormatter = new Intl.NumberFormat(undefined);

let authEnabled = false;
let currentAuthUsername = '';

export { syncFormatter };

export function escapeHtml(s) {
  return (s ?? '').toString()
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

export function fmtWholeNumber(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return '-';
  return integerFormatter.format(Math.trunc(num));
}

export function formatDuration(seconds) {
  const raw = Number(seconds);
  if (!Number.isFinite(raw) || raw < 0) return '-';
  let remaining = Math.trunc(raw);
  const days = Math.floor(remaining / 86400);
  remaining %= 86400;
  const hours = Math.floor(remaining / 3600);
  remaining %= 3600;
  const minutes = Math.floor(remaining / 60);
  const secs = remaining % 60;
  const parts = [];
  if (days > 0) parts.push(`${days}d`);
  if (hours > 0 || parts.length > 0) parts.push(`${hours}h`);
  if (minutes > 0 || parts.length > 0) parts.push(`${minutes}m`);
  parts.push(`${secs}s`);
  return parts.join(' ');
}

export function fmtBytes(n) {
  if (!n && n !== 0) return '-';
  const mb = n / (1024 * 1024);
  if (mb < 1024) return mb.toFixed(1) + ' MB';
  return (mb / 1024).toFixed(2) + ' GB';
}

export function formatDateTime(value) {
  const raw = String(value || '').trim();
  if (!raw) return '-';
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return raw;
  return parsed.toLocaleString();
}

export function tableDateCell(value) {
  const raw = (value ?? '-').toString().trim();
  if (!raw || raw === '-') {
    return `<span class="mono text-xs text-slate-500">-</span>`;
  }

  const normalized = raw.replace('T', ' ');
  const parts = normalized.split(/\s+/);
  const datePart = parts[0] || normalized;
  const timePart = parts.slice(1).join(' ');

  return `
    <div class="flex flex-col gap-0.5">
      <span class="mono text-xs font-medium text-slate-700">${escapeHtml(datePart)}</span>
      ${timePart ? `<span class="mono text-[11px] text-slate-500">${escapeHtml(timePart)}</span>` : ''}
    </div>
  `;
}

function normalizeCommaSpacing(value) {
  const raw = (value ?? '-').toString().trim();
  if (!raw) return '-';
  return raw.replace(/,([^\s])/g, ', $1');
}

export function jobDisplay(value) {
  const normalized = normalizeCommaSpacing(value);
  const match = normalized.match(/^(.*?)\s*\((.+)\)$/);

  if (match) {
    const title = match[1].trim() || normalized;
    const detail = match[2].trim();
    return { title, detail };
  }

  return { title: normalized, detail: '' };
}

export function idChip(value) {
  const safeValue = escapeHtml(value ?? '-');
  return `
    <div class="id-chip" title="${safeValue}">
      <span class="mono text-xs font-semibold text-slate-700">${safeValue}</span>
    </div>
  `;
}

export function statusPill(status) {
  const value = (status ?? '-').toString();
  const base = 'inline-flex items-center gap-2 rounded-md border px-2.5 py-1 text-xs font-semibold uppercase tracking-normal';
  const map = {
    ACTIVE: ['border-emerald-200 bg-emerald-50 text-emerald-800', 'bg-emerald-500'],
    RUNNING: ['border-emerald-200 bg-emerald-50 text-emerald-800', 'bg-emerald-500'],
    PAUSING: ['border-amber-200 bg-amber-50 text-amber-800', 'bg-amber-500'],
    PAUSED: ['border-violet-200 bg-violet-50 text-violet-800', 'bg-violet-500'],
    QUEUED: ['border-sky-200 bg-sky-50 text-sky-800', 'bg-sky-500'],
    PENDING: ['border-amber-200 bg-amber-50 text-amber-800', 'bg-amber-500'],
    DEGRADED: ['border-amber-200 bg-amber-50 text-amber-800', 'bg-amber-500'],
    STALE: ['border-rose-200 bg-rose-50 text-rose-800', 'bg-rose-500'],
    CRITICAL: ['border-rose-200 bg-rose-50 text-rose-800', 'bg-rose-500'],
    FAILED: ['border-rose-200 bg-rose-50 text-rose-800', 'bg-rose-500'],
    STOPPED: ['border-slate-200 bg-slate-100 text-slate-700', 'bg-slate-400'],
    DONE: ['border-blue-200 bg-blue-50 text-blue-800', 'bg-blue-500'],
  };
  const [pillClass, dotClass] = map[value] || ['border-slate-200 bg-slate-100 text-slate-700', 'bg-slate-400'];
  return `<span class="${base} ${pillClass}"><span class="h-2 w-2 rounded-full ${dotClass}"></span>${escapeHtml(value)}</span>`;
}

export function progressPill(progress) {
  const phase = String(progress?.phase || '').trim().toLowerCase();
  const labelMap = {
    queued: 'QUEUED',
    preflight: 'PRE-FLIGHT',
    snapshot: 'SNAPSHOT',
    snapshot_complete: 'SNAPSHOT DONE',
    streaming: 'CDC',
    done: 'DONE',
    stopped: 'STOPPED',
    failed: 'FAILED',
  };
  const classMap = {
    queued: 'border-sky-200 bg-sky-50 text-sky-800',
    preflight: 'border-amber-200 bg-amber-50 text-amber-800',
    snapshot: 'border-blue-200 bg-blue-50 text-blue-800',
    snapshot_complete: 'border-blue-200 bg-blue-50 text-blue-800',
    streaming: 'border-emerald-200 bg-emerald-50 text-emerald-800',
    done: 'border-blue-200 bg-blue-50 text-blue-800',
    stopped: 'border-slate-200 bg-slate-100 text-slate-700',
    failed: 'border-rose-200 bg-rose-50 text-rose-800',
  };
  const label = labelMap[phase] || 'PROGRESS';
  const cls = classMap[phase] || 'border-slate-200 bg-slate-100 text-slate-700';
  return `<span class="inline-flex items-center rounded-md border px-2.5 py-1 text-xs font-semibold uppercase tracking-normal ${cls}">${escapeHtml(label)}</span>`;
}

export function progressMetaParts(progress) {
  const currentTableIndex = Number(progress?.current_table_index);
  const totalTables = Number(progress?.total_tables);
  const currentTableRows = Number(progress?.current_table_rows);
  const metaParts = [];

  if (Number.isFinite(currentTableIndex) && currentTableIndex > 0 && Number.isFinite(totalTables) && totalTables > 0) {
    metaParts.push(`Table ${fmtWholeNumber(currentTableIndex)}/${fmtWholeNumber(totalTables)}`);
  }
  if (Number.isFinite(currentTableRows) && currentTableRows >= 0) {
    metaParts.push(`${fmtWholeNumber(currentTableRows)} rows`);
  }

  return metaParts;
}

export function setBoundText(name, value) {
  document.querySelectorAll(`[data-bind="${name}"]`).forEach((el) => {
    el.textContent = value;
  });
}

export function setInnerHTMLIfChanged(el, html, { preserveScroll = false } = {}) {
  if (!el || el.innerHTML === html) return;
  const ancestors = [];
  const regions = new Map();
  if (preserveScroll) {
    // Existing ancestors survive replacement (including document scrolling).
    // Nested table viewports need stable keys because their DOM nodes do not.
    for (let node = el; node; node = node.parentElement) {
      ancestors.push([node, node.scrollLeft, node.scrollTop]);
    }
    for (const node of el.querySelectorAll('[data-scroll-key]')) {
      regions.set(node.getAttribute('data-scroll-key'), [node.scrollLeft, node.scrollTop]);
    }
  }
  el.innerHTML = html;
  if (!preserveScroll) return;
  const restore = (node, left, top) => {
    if (!Number.isFinite(left) || !Number.isFinite(top)) return;
    if (node.scrollLeft === left && node.scrollTop === top) return;
    // Avoid the document's smooth-scroll styling animating each poll update.
    if (typeof node.scrollTo === 'function') node.scrollTo({ left, top, behavior: 'instant' });
    else { node.scrollLeft = left; node.scrollTop = top; }
  };
  for (const node of el.querySelectorAll('[data-scroll-key]')) {
    const position = regions.get(node.getAttribute('data-scroll-key'));
    if (position) restore(node, ...position);
  }
  for (const position of ancestors) restore(...position);
}

export function setJobsNotice(message, tone = 'info') {
  const el = document.getElementById('jobsNotice');
  if (!el) return;
  if (!message) {
    el.className = 'mt-4 hidden rounded-lg border px-4 py-3 text-sm';
    el.textContent = '';
    return;
  }

  const toneClass = tone === 'error'
    ? 'border-rose-200 bg-rose-50 text-rose-700'
    : 'border-slate-200 bg-white/70 text-slate-600';

  el.className = `mt-4 rounded-lg border px-4 py-3 text-sm ${toneClass}`;
  el.textContent = message;
}

export function setLogsNotice(message, tone = 'info') {
  const el = document.getElementById('logsNotice');
  if (!el) return;
  if (!message) {
    el.className = 'hidden rounded-xl border px-4 py-3 text-sm';
    el.textContent = '';
    return;
  }

  const toneClass = tone === 'error'
    ? 'border-rose-200 bg-rose-50 text-rose-700'
    : 'border-slate-200 bg-white/70 text-slate-600';

  el.className = `rounded-xl border px-4 py-3 text-sm ${toneClass}`;
  el.textContent = message;
}

export function setSubmitStatus(message, tone = 'muted') {
  const el = document.getElementById('submitStatus');
  if (!el) return;
  const toneClass = tone === 'error'
    ? 'text-rose-600'
    : tone === 'success'
      ? 'text-blue-700'
      : 'text-slate-500';
  el.className = `text-sm ${toneClass}`;
  el.textContent = message;
}

export function markSynced(prefix = 'Synced') {
  setBoundText('sync-stamp', `${prefix} ${syncFormatter.format(new Date())}`);
}

export function setHealthState(connected) {
  const dot = document.getElementById('healthDot');
  const badge = document.getElementById('healthBadge');
  if (!dot || !badge) return;

  if (connected) {
    dot.className = 'brand-dot h-2.5 w-2.5 rounded-full';
    badge.className = 'brand-badge inline-flex items-center rounded-md px-2.5 py-1 text-xs font-medium';
    badge.textContent = 'Connected';
    return;
  }

  dot.className = 'h-2.5 w-2.5 rounded-full bg-amber-500 shadow-[0_0_0_4px_rgba(245,158,11,0.12)]';
  badge.className = 'inline-flex items-center rounded-md border border-amber-200 bg-white px-2.5 py-1 text-xs font-medium text-amber-900';
  badge.textContent = 'Degraded';
}

function syncBodyLock() {
  const hasOpenModal = MODAL_IDS.some((id) => !document.getElementById(id)?.classList.contains('hidden'));
  document.body.classList.toggle('modal-open', hasOpenModal);
}

export function isAnyModalOpen() {
  return MODAL_IDS.some((id) => !document.getElementById(id)?.classList.contains('hidden'));
}

export function setModalState(id, open) {
  document.getElementById(id)?.classList.toggle('hidden', !open);
  syncBodyLock();
}

function currentLocationForRedirect() {
  return window.location.pathname + window.location.search;
}

export function redirectToLogin() {
  const next = currentLocationForRedirect() || '/';
  window.location.href = '/login?next=' + encodeURIComponent(next);
}

export async function apiFetch(input, init) {
  return fetch(input, { credentials: 'same-origin', ...(init || {}) });
}

function profileInitials(username) {
  const name = (username || 'User').trim();
  if (!name) return 'U';

  return name
    .split(/\s+/)
    .slice(0, 2)
    .map((part) => part.charAt(0))
    .join('')
    .toUpperCase();
}

export function setProfileMenu(open) {
  const dropdown = document.getElementById('profileDropdown');
  const trigger = document.getElementById('profileButton');
  if (!dropdown || !trigger) return;

  dropdown.classList.toggle('hidden', !open);
  trigger.setAttribute('aria-expanded', open ? 'true' : 'false');
}

export function toggleProfileMenu() {
  const dropdown = document.getElementById('profileDropdown');
  if (!dropdown) return;
  setProfileMenu(dropdown.classList.contains('hidden'));
}

export async function loadAuthStatus() {
  const menu = document.getElementById('profileMenu');
  const name = document.getElementById('profileName');
  const initials = document.getElementById('profileInitials');
  if (!menu || !name || !initials) return;

  try {
    const res = await fetch('/auth/status', { credentials: 'same-origin' });
    if (!res.ok) throw new Error(`auth ${res.status}`);

    const data = await res.json();
    authEnabled = !!data.enabled;
    currentAuthUsername = data.username || '';

    if (authEnabled && !data.authenticated) {
      redirectToLogin();
      return;
    }

    menu.classList.toggle('hidden', !authEnabled);

    if (authEnabled) {
      const displayName = currentAuthUsername || 'User';
      name.textContent = displayName;
      initials.textContent = profileInitials(displayName);
    }
  } catch (err) {
    authEnabled = false;
    currentAuthUsername = '';
    setProfileMenu(false);
    menu.classList.add('hidden');
  }
}

export async function loadAppVersion() {
  const imageTagEl = document.getElementById('appImageTag');
  const commitEl = document.getElementById('appCommitHash');
  const metaEl = document.getElementById('appBuildMeta');
  if (!imageTagEl && !commitEl) return;

  try {
    const res = await fetch('/api/version', { credentials: 'same-origin' });
    if (!res.ok) throw new Error(`version ${res.status}`);
    const data = await res.json();
    const version = data.version || 'dev';
    const imageTag = data.image_tag || version;
    const commit = data.commit ? String(data.commit) : 'dev';
    if (imageTagEl) imageTagEl.textContent = imageTag;
    if (commitEl) commitEl.textContent = commit === 'dev' ? commit : commit.slice(0, 12);
    if (metaEl) {
      const titleParts = [`Image rivus:${imageTag}`, `Commit ${commit}`];
      if (data.build_date) titleParts.push(`Built ${data.build_date}`);
      if (version) titleParts.push(`Version ${version}`);
      metaEl.title = titleParts.join('\n');
    }
  } catch (err) {
    if (imageTagEl) imageTagEl.textContent = 'dev';
    if (commitEl) commitEl.textContent = 'dev';
    if (metaEl) metaEl.title = 'Build metadata unavailable';
  }
}

export async function loadMetrics() {
  try {
    const res = await apiFetch('/api/metrics');
    if (!res.ok) throw new Error(`metrics ${res.status}`);

    const m = await res.json();
    const cpu = (m.cpu_percent ?? 0).toFixed(1) + '%';
    const mem = fmtBytes(m.rss_bytes);

    document.getElementById('m_pid').textContent = m.pid ?? '-';
    document.getElementById('m_cpu').textContent = cpu;
    document.getElementById('m_mem').textContent = mem;
    setHealthState(true);
  } catch (err) {
    document.getElementById('m_cpu').textContent = '-';
    document.getElementById('m_mem').textContent = '-';
    setHealthState(false);
  }
}

export async function operationErrorMessage(res, action) {
  const fallback = `${action} failed: ${res.status}`;
  const contentType = String(res.headers.get('content-type') || '').toLowerCase();
  if (contentType.includes('application/json')) {
    const data = await res.json().catch(() => ({}));
    return data.error || fallback;
  }

  const text = String(await res.text().catch(() => '')).trim();
  if (res.status === 502 || res.status === 503 || res.status === 504 || /<html[\s>]/i.test(text)) {
    return `${action} could not reach Rivus through the gateway (${res.status}). Refresh and retry after the service is available.`;
  }
  return text ? `${fallback}\n${text.slice(0, 300)}` : fallback;
}

export async function logout() {
  try {
    await fetch('/auth/logout', { method: 'POST', credentials: 'same-origin' });
  } finally {
    redirectToLogin();
  }
}

export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Saved progress remains available in the API; lifecycle labels prevent it
// from being mistaken for activity by a stopped or stopping pipeline.
export function jobRuntimeProgress(job) {
 const status=String(job?.status||'').toUpperCase();
 const summaries={STOPPING:'Stopping job',STOPPED:'Job stopped',PAUSED:'Job paused',FAILED:'Job failed',DONE:'Job complete'};
 if (summaries[status]) return {phase:status.toLowerCase(),summary:summaries[status]};
 if(status==='QUEUED') return {phase:'queued',summary:job?.progress?.phase==='queued' ? job.progress.summary : 'Waiting in queue'};
 return job?.progress;
}
