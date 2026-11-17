import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { JSDOM, VirtualConsole } from 'jsdom';

const flush = () => new Promise(resolve => setTimeout(resolve, 35));
function card(name, storedMode) {
  const sent = [], errors = [], storage = [];
  const console = new VirtualConsole();
  console.on('jsdomError', error => { if (error.type === 'unhandled-exception') errors.push(error); });
  const dom = new JSDOM(readFileSync(new URL(`../../src/mcp_${name}_app.html`, import.meta.url), 'utf8'), {
    url: 'https://card.example/', runScripts: 'dangerously', pretendToBeVisual: true, virtualConsole: console,
    beforeParse(win) {
      const get = win.Storage.prototype.getItem;
      const set = win.Storage.prototype.setItem;
      const allowed = new Set(['webcodex.ui.selection.v1', 'webcodex.ui.accent.v1']);
      win.Storage.prototype.getItem = function (key) {
        storage.push({ key });
        assert.ok(allowed.has(key), `card reads only local appearance: ${key}`);
        return get.call(this, key);
      };
      win.Storage.prototype.setItem = function (key, value) {
        storage.push({ key, value });
        assert.ok(allowed.has(key), `card stores only local appearance: ${key}`);
        assert.ok(key.endsWith('selection.v1') ? ['accent', 'monochrome'].includes(value) : /^#[0-9a-f]{6}$/i.test(value));
        return set.call(this, key, value);
      };
      if (storedMode) win.localStorage.setItem('webcodex.ui.selection.v1', storedMode);
      win.ResizeObserver = class { observe() {} unobserve() {} disconnect() {} };
      win.DOMMatrix = class {};
      win.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });
      win.postMessage = message => sent.push(message);
      win.HTMLElement.prototype.getBoundingClientRect = function () {
        const index = [...(this.parentElement?.children || [])].indexOf(this);
        return { x: 0, y: 0, left: index * 95, top: 0, right: index * 95 + 90, bottom: 36, width: 90, height: 36 };
      };
    },
  });
  const close = dom.window.close.bind(dom.window);
  dom.window.close = () => {
    try {
      for (const entry of storage) {
        assert.ok(['webcodex.ui.selection.v1', 'webcodex.ui.accent.v1'].includes(entry.key), 'even caught storage failures must stay appearance-only');
        if ('value' in entry) assert.ok(entry.key.endsWith('selection.v1') ? ['accent', 'monochrome'].includes(entry.value) : /^#[0-9a-f]{6}$/i.test(entry.value));
      }
    } finally { close(); }
  };
  return { dom, win: dom.window, doc: dom.window.document, sent, errors };
}

for (const [name, groups] of [
  ['workbench', [['.primary-navigation', 'filled'], ['#projectTabs', 'underline'], ['#projectNavigationMotion', 'filled']]],
  ['work_result', [['#viewTabs', 'underline']]],
  ['spreadsheet', [['#sheetsMotion', 'underline']]],
]) {
  test(`${name} shipped card wires local appearance and semantic selection motion`, async () => {
    const { dom, win, doc, sent, errors } = card(name);
    try {
      await flush();
      assert.deepEqual(errors, []);
      for (const [selector, variant] of groups) assert.equal(doc.querySelector(selector).dataset.selectionMotionVariant, variant);
      assert.equal(doc.documentElement.dataset.selectionColor, 'accent');
      const before = sent.length;
      const preference = doc.getElementById('selectionPreference');
      preference.closest('details').open = true;
      const trigger = preference.parentElement.querySelector('.ui-select-trigger');
      assert.equal(trigger.getAttribute('role'), 'combobox');
      trigger.click();
      assert.equal(trigger.getAttribute('aria-expanded'), 'true');
      assert.equal(doc.getElementById(trigger.getAttribute('aria-controls')).parentElement, doc.body, 'the popup escapes card clipping');
      trigger.dispatchEvent(new win.KeyboardEvent('keydown', { key: 'End', bubbles: true }));
      trigger.dispatchEvent(new win.KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));
      await flush();
      assert.equal(doc.documentElement.dataset.selectionColor, 'monochrome');
      assert.equal(win.localStorage.getItem('webcodex.ui.selection.v1'), 'monochrome');
      assert.equal(sent.length, before, 'changing local appearance must not submit or request Host work');
      win.dispatchEvent(new win.MessageEvent('message', { source: win.parent, data: { jsonrpc: '2.0', method: 'ui/notifications/host-context-changed', params: { theme: 'dark' } } }));
      assert.equal(doc.documentElement.style.colorScheme, 'dark', 'Host theme remains authoritative');
      win.dispatchEvent(new win.PageTransitionEvent('pagehide'));
      await flush();
      assert.equal(doc.querySelectorAll('[data-selection-motion]').length, 0, 'card teardown releases selection controllers');
    } finally { dom.window.close(); }
  });
}

test('Workbench retains the saved local mode and flows Home/Goals without submitting work', async () => {
  const { dom, doc, sent, errors } = card('workbench', 'monochrome');
  try {
    await flush(); assert.deepEqual(errors, []);
    assert.equal(doc.getElementById('selectionPreference').value, 'monochrome');
    doc.getElementById('goalsNav').click(); await flush();
    assert.equal(doc.getElementById('goalsNav').getAttribute('aria-pressed'), 'true');
    assert.equal(doc.querySelector('.primary-navigation [data-selection-motion-selected]').id, 'goalsNav');
    doc.getElementById('homeNav').click(); await flush();
    assert.equal(doc.querySelector('.primary-navigation [data-selection-motion-selected]').id, 'homeNav');
    assert.equal(sent.filter(request => request.method === 'tools/call').length, 0);
  } finally { dom.window.close(); }
});

for (const name of ['work_result', 'spreadsheet']) {
  test(`${name} appearance panel bounds, nested selection, dismissal and teardown`, async () => {
    const { dom, win, doc, errors } = card(name);
    try {
      await flush();
      const preference = doc.getElementById('selectionPreference');
      const details = preference.closest('details');
      const panel = details.querySelector('.card-selection-preference');
      const summary = details.querySelector('summary');
      const trigger = details.querySelector('.ui-select-trigger');
      Object.defineProperty(win, 'innerWidth', { configurable: true, value: 320 });
      summary.getBoundingClientRect = () => ({ left: 10, right: 90, top: 10, bottom: 42, width: 80, height: 32 });
      details.open = true;
      await flush();
      assert.equal(panel.style.left, '8px');
      assert.equal(panel.style.top, '48px');
      trigger.click();
      const menu = doc.getElementById(trigger.getAttribute('aria-controls'));
      menu.dispatchEvent(new win.MouseEvent('pointerdown', { bubbles: true }));
      assert.equal(details.open, true, 'nested portaled list belongs to Appearance');
      doc.body.dispatchEvent(new win.MouseEvent('pointerdown', { bubbles: true }));
      assert.equal(details.open, false);
      trigger.dispatchEvent(new win.Event('blur'));
      details.open = true;
      summary.focus();
      summary.dispatchEvent(new win.KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
      assert.equal(details.open, false);
      assert.equal(doc.activeElement, summary);
      details.open = true;
      summary.getBoundingClientRect = () => ({ left: 300, right: 380, top: 10, bottom: 42, width: 80, height: 32 });
      win.dispatchEvent(new win.Event('resize'));
      assert.equal(panel.style.left, '112px', 'right edge clamps after resize');
      win.dispatchEvent(new win.PageTransitionEvent('pagehide'));
      assert.equal(details.open, false);
      const previous = panel.style.left;
      details.open = true;
      summary.getBoundingClientRect = () => ({ left: 10, right: 90, top: 10, bottom: 42, width: 80, height: 32 });
      win.dispatchEvent(new win.Event('resize'));
      await flush();
      assert.equal(panel.style.left, previous, 'disposed controls no longer reposition');
      assert.deepEqual(errors, []);
    } finally { dom.window.close(); }
  });
}
