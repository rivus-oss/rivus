import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

const source = fs.readFileSync(new URL('./core/api.js', import.meta.url), 'utf8');
const { setInnerHTMLIfChanged } = await import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'));

function fixture(oldKey = 'owner:compaction:1', newKey = oldKey) {
  const document = { scrollLeft: 0, scrollTop: 900, parentElement: null };
  const wrapper = { scrollLeft: 180, scrollTop: 20, parentElement: document };
  const viewport = (key, left = 0, top = 0) => ({
    scrollLeft: left, scrollTop: top, getAttribute: () => key,
    scrollTo({ left, top, behavior }) {
      assert.equal(behavior, 'instant');
      this.scrollLeft = left; this.scrollTop = top;
    },
  });
  let html = 'before';
  let region = viewport(oldKey, 240, 380);
  let replacements = 0;
  const panel = {
    parentElement: wrapper, scrollLeft: 0, scrollTop: 0,
    get innerHTML() { return html; },
    set innerHTML(value) {
      html = value; replacements++;
      region = viewport(newKey);
      // Simulate layout/anchoring moving surviving scroll ancestors too.
      wrapper.scrollLeft = 0; wrapper.scrollTop = 0; document.scrollTop = 100;
    },
    querySelectorAll: () => [region],
  };
  return { panel, wrapper, document, region: () => region, replacements: () => replacements };
}

test('polling updates retain table vertical/horizontal scroll and outer viewport', () => {
  const f = fixture();
  setInnerHTMLIfChanged(f.panel, 'changed file counts', { preserveScroll: true });
  assert.equal(f.region().scrollTop, 380);
  assert.equal(f.region().scrollLeft, 240);
  assert.equal(f.wrapper.scrollLeft, 180);
  assert.equal(f.wrapper.scrollTop, 20);
  assert.equal(f.document.scrollTop, 900);
  assert.equal(f.panel.innerHTML, 'changed file counts');
});

test('changing owner, page, operation or filters starts a new table viewport', () => {
  for (const scope of ['another-owner:compaction:1', 'owner:compaction:2', 'owner:cleanup:1', 'owner:cleanup:filtered']) {
    const f = fixture('owner:compaction:1', scope);
    setInnerHTMLIfChanged(f.panel, 'new selection', { preserveScroll: true });
    assert.equal(f.region().scrollTop, 0, scope);
    assert.equal(f.region().scrollLeft, 0, scope);
    assert.equal(f.document.scrollTop, 900);
  }
});

test('identical markup does not replace nodes or adjust scroll', () => {
  const f = fixture();
  const original = f.region();
  setInnerHTMLIfChanged(f.panel, 'before', { preserveScroll: true });
  assert.equal(f.replacements(), 0);
  assert.equal(f.region(), original);
  assert.equal(f.region().scrollTop, 380);
});

test('callers without scroll preservation retain their existing behavior', () => {
  const f = fixture();
  setInnerHTMLIfChanged(f.panel, 'ordinary replacement');
  assert.equal(f.region().scrollTop, 0);
  assert.equal(f.document.scrollTop, 100);
});
