import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

const dataURL = (source) => 'data:text/javascript;base64,' + Buffer.from(source).toString('base64');
const apiURL = dataURL(fs.readFileSync(new URL('./core/api.js', import.meta.url), 'utf8'));
const cleanupURL = dataURL(fs.readFileSync(new URL('./iceberg-cleanup.js', import.meta.url), 'utf8').replace('./core/api.js', apiURL));
const { cleanupContent } = await import(cleanupURL);
const source = fs.readFileSync(new URL('./iceberg-maintenance.js', import.meta.url), 'utf8');
const { renderIcebergMaintenance } = await import(dataURL(source.replace('./core/api.js', apiURL).replace('./iceberg-cleanup.js', cleanupURL)));

function render(table, options = {}) {
  const panel = options.panel || {
    dataset: {},
    classList: { add() {}, remove() {} },
    querySelector: () => null,
    querySelectorAll: () => [],
  };
  renderIcebergMaintenance({
    id: 'test-job', sink_type: 'iceberg_native',
    iceberg_maintenance: {
      enabled: true, state: table.state, data_files_threshold: 50,
      equality_delete_files_threshold: 25, position_delete_files_threshold: 10,
      tables_total: 1, tables_scanned: 1, tables: [table],
    },
  }, { panel, ...options });
  return panel.innerHTML;
}

const partitioned = {
  identifier: 'ga4_bronze.daily_overview', state: 'partitioned',
  active_data_files: 648, eligible_small_files: 648,
  compactable_files: 0, compaction_groups: 0,
  partition_fields: ['date'],
  applicable_equality_delete_files: 0, applicable_position_delete_files: 0,
  checked_at: '2026-10-15T04:00:00Z',
};

test('table totals keep their trigger while explaining why partitioned files are ineligible', () => {
  const html = render(partitioned);
  assert.match(html, /Small:[^\n]*648 \/ 50/);
  assert.match(html, /0 files eligible\. No eligible groups within partitions\./);
  assert.match(html, /Partitioned · date/);
  assert.match(html, /No eligible file groups/);
  assert.doesNotMatch(html, /Ready for maintenance|Eligible small files|Eligible small bytes/);
});

function cleanupPanel(operation = 'remove_orphan_files') {
  return {
    dataset: { maintenancePageOwner: 'test-job', maintenanceView: 'cleanup', cleanupOperation: operation },
    classList: { add() {}, remove() {} }, querySelector: () => null, querySelectorAll: () => [],
  };
}

test('cleanup view uses operation schedules and results independently of stale inventory', () => {
  const html = render({ ...partitioned, state: 'stale', cleanup: [{
    operation: 'remove_orphan_files', status: 'scheduled', reason: 'Waiting until due',
    last_success_at: '2026-11-01T00:00:00Z', next_due_at: '2026-11-08T00:00:00Z',
    last_result: { status: 'succeeded', deleted_files: 12, deleted_bytes: 1048576, created_at: '2026-11-01T00:00:00Z' },
  }] }, { panel: cleanupPanel() });
  assert.match(html, /Last successful cleanup/);
  assert.match(html, /Next due/);
  assert.match(html, /12 files/);
  assert.match(html, /1.0 MB/);
  assert.match(html, /View history/);
  assert.doesNotMatch(html, /Inventory stale|Small-file bytes|Refresh required/);
});

test('snapshot cleanup does not claim zero deleted bytes and displays missing successes honestly', () => {
  const html = render({ ...partitioned, cleanup: [{ operation: 'expire_snapshots', status: 'skipped',
    reason: '<No old snapshots>', last_result: { status: 'skipped', expired_snapshots: 0, deleted_files: null, deleted_bytes: null },
  }] }, { panel: cleanupPanel('expire_snapshots') });
  assert.match(html, /No successful run recorded/);
  assert.match(html, /Deleted files \/ bytes not measured/);
  assert.match(html, /&lt;No old snapshots&gt;/);
  assert.doesNotMatch(html, /0.0 MB/);
});

test('cleanup operation, search, pagination, and unavailable data keep their meaning', () => {
  const panel = cleanupPanel();
  panel.dataset.cleanupSearch = 'not-present';
  assert.match(render(partitioned, { panel }), /No tables match these cleanup filters/);
  panel.dataset.cleanupSearch = '';
  assert.match(render(partitioned, { panel }), /Cleanup monitoring is unavailable/);
  panel.dataset.cleanupOperation = 'expire_snapshots';
  const html = render({ ...partitioned, cleanup: [
    { operation: 'expire_snapshots', status: 'running', reason: 'Expiring', last_result: { status: 'running' } },
    { operation: 'remove_orphan_files', status: 'failed', reason: 'Orphan failure' },
  ] }, { panel });
  assert.match(html, /In progress/);
  assert.doesNotMatch(html, /Orphan failure/);
});

test('switching owners resets cleanup filters and selected tab', () => {
  const panel = cleanupPanel();
  panel.dataset.maintenancePageOwner = 'another-owner';
  panel.dataset.cleanupSearch = 'old filter';
  const html = render(partitioned, { panel });
  assert.equal(panel.dataset.maintenanceView, 'compaction');
  assert.equal(panel.dataset.cleanupSearch, '');
  assert.match(html, /Small-file bytes/);
});

test('cleanup pagination follows filtered rows and clamps after filtering', () => {
  const panel = cleanupPanel();
  panel.dataset.cleanupPage = '2';
  const maintenance = { tables_total: 3, tables: ['alpha', 'beta', 'gamma'].map((name) => ({
    identifier: `sample.ds.${name}`, cleanup: [{ operation: 'remove_orphan_files', status: 'scheduled' }],
  })) };
  const metric = () => '';
  let html = cleanupContent(maintenance, panel, 'test-job', metric, 2);
  assert.match(html, /sample.ds.gamma/);
  assert.doesNotMatch(html, /sample.ds.alpha/);
  assert.match(html, /Rows 3–3 of 3/);
  panel.dataset.cleanupSearch = 'alpha';
  html = cleanupContent(maintenance, panel, 'test-job', metric, 2);
  assert.equal(panel.dataset.cleanupPage, '1');
  assert.match(html, /Rows 1–1 of 1/);
  assert.match(html, /sample.ds.alpha/);
});

test('the same file ratio distinguishes partitioned and unpartitioned tables from metadata', () => {
  const partitionedHTML = render({ ...partitioned, state: 'stale', eligible_small_files: 657 });
  const unpartitionedHTML = render({ ...partitioned, state: 'ready', eligible_small_files: 657, partition_fields: [], compactable_files: 657, compaction_groups: 1 });
  assert.match(partitionedHTML, /657 \/ 50/);
  assert.match(unpartitionedHTML, /657 \/ 50/);
  assert.match(partitionedHTML, /Partitioned · date/);
  assert.match(partitionedHTML, /0 files eligible at last scan/);
  assert.match(unpartitionedHTML, /Unpartitioned/);
  assert.match(unpartitionedHTML, /657 files in 1 eligible group/);
  assert.match(unpartitionedHTML, /Ready for maintenance/);
  assert.doesNotMatch(unpartitionedHTML, /within partitions/);
});

test('partitioning is not guessed from file counts or the worker state', () => {
  for (const partition_fields of [undefined, null, [''], [42]]) {
    const html = render({ ...partitioned, partition_fields });
    assert.match(html, /Partitioning pending scan/);
    assert.doesNotMatch(html, /Partitioned ·|Unpartitioned/);
  }
  assert.match(render({ ...partitioned, partition_fields: ['day(created_at)', '<region>'] }), /Partitioned · day\(created_at\), &lt;region&gt;/);
});

test('cached eligibility remains explicitly historical when inventory is stale or refreshing', () => {
  for (const state of ['stale', 'inventory_pending', 'scanning']) {
    const html = render({ ...partitioned, state });
    assert.match(html, /0 files eligible at last scan/);
    assert.doesNotMatch(html, /Ready for maintenance/);
  }
});

test('missing planner counts or a missing inventory are not presented as zero eligibility', () => {
  for (const table of [
    { ...partitioned, compactable_files: undefined, compaction_groups: undefined },
    { ...partitioned, checked_at: undefined },
  ]) {
    const html = render(table);
    assert.match(html, /Eligibility pending scan/);
    assert.doesNotMatch(html, /0 files eligible/);
  }
});

test('eligible groups and delete triggers preserve the worker status for both panel owners', () => {
  for (const historyOwnerID of ['job-1', 'monitor:maintenance-catalogs']) {
    const html = render({ ...partitioned, state: 'ready', compactable_files: 20, compaction_groups: 2, applicable_equality_delete_files: 30 }, { historyOwnerID });
    assert.match(html, /20 files in 2 eligible groups/);
    assert.match(html, /Ready for maintenance/);
    assert.match(html, /Equality:[^\n]*30 \/ 25/);
  }
});

test('worker errors and routing reasons retain precedence and are escaped', () => {
  const errorHTML = render({ ...partitioned, state: 'error', error: '<failed scan>' });
  assert.match(errorHTML, /&lt;failed scan&gt;/);
  assert.doesNotMatch(errorHTML, /0 files eligible/);
  const routeHTML = render({ ...partitioned, operations: ['compact', 'route:Compaction skipped: no eligible group'] });
  assert.match(routeHTML, /Compaction skipped: no eligible group/);
});
