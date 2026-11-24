// Self-contained MCP card presentation. No Host requests or authority state.
export { createSelectionMotion } from '../ui/selectionMotion';
import { initializeCardAppearance } from '../ui/cardAppearance';
import { enhanceSelects } from '../ui/enhancedSelect';
import { loadSelectionPreference, persistSelectionPreference, applySelectionPreference, SELECTION_CHANGE_EVENT } from '../ui/selectionPreference';

export function initializeCardControls(preferenceSelect) {
  const root = preferenceSelect?.ownerDocument?.documentElement;
  const win = root?.ownerDocument?.defaultView;
  // Protocol fixtures intentionally have no browser document.
  if (!win) return { destroy() {}, updateTheme() {}, update() {} };
  const appearance = initializeCardAppearance(root);
  const selects = enhanceSelects(root.ownerDocument.body);
  const details = preferenceSelect.closest('.card-appearance');
  const panel = details?.querySelector('.card-selection-preference');
  const summary = details?.querySelector('summary');
  const position = () => {
    if (!details?.open || !panel || !summary) return;
    const box = summary.getBoundingClientRect();
    const width = Math.min(200, Math.max(0, win.innerWidth - 16));
    const below = win.innerHeight - box.bottom - 14;
    const above = box.top - 14;
    const upwards = below < panel.scrollHeight && above > below;
    Object.assign(panel.style, {
      width: `${width}px`, left: `${Math.max(8, Math.min(box.right - width, win.innerWidth - width - 8))}px`,
      top: upwards ? 'auto' : `${Math.max(8, box.bottom + 6)}px`,
      bottom: upwards ? `${Math.max(8, win.innerHeight - box.top + 6)}px` : 'auto',
      maxHeight: `${Math.max(0, upwards ? above : below)}px`,
    });
  };
  const contains = target => details?.contains(target) || Array.from(details?.querySelectorAll('[aria-controls]') || [])
    .some(trigger => root.ownerDocument.getElementById(trigger.getAttribute('aria-controls'))?.contains(target));
  const outside = event => { if (details?.open && !contains(event.target)) details.open = false; };
  const escape = event => {
    if (event.key !== 'Escape' || !details?.open) return;
    event.preventDefault();
    details.open = false;
    summary?.focus();
  };
  details?.addEventListener('toggle', position);
  root.ownerDocument.addEventListener('pointerdown', outside);
  root.ownerDocument.addEventListener('focusin', outside);
  root.ownerDocument.addEventListener('keydown', escape);
  root.ownerDocument.addEventListener('scroll', position, true);
  win.addEventListener('resize', position);
  const sync = event => {
    preferenceSelect.value = event?.detail === 'accent' || event?.detail === 'monochrome' ? event.detail : loadSelectionPreference();
    selects.update();
  };
  const change = () => {
    const mode = preferenceSelect.value === 'monochrome' ? 'monochrome' : 'accent';
    persistSelectionPreference(mode);
    applySelectionPreference(mode, root);
    selects.update();
  };
  preferenceSelect.addEventListener('change', change);
  win.addEventListener(SELECTION_CHANGE_EVENT, sync);
  win.addEventListener('storage', sync);
  sync();
  return {
    updateTheme: theme => appearance.updateTheme(theme),
    update: () => selects.update(),
    destroy() {
      if (details) details.open = false;
      details?.removeEventListener('toggle', position);
      root.ownerDocument.removeEventListener('pointerdown', outside);
      root.ownerDocument.removeEventListener('focusin', outside);
      root.ownerDocument.removeEventListener('keydown', escape);
      root.ownerDocument.removeEventListener('scroll', position, true);
      win.removeEventListener('resize', position);
      preferenceSelect.removeEventListener('change', change);
      win.removeEventListener(SELECTION_CHANGE_EVENT, sync);
      win.removeEventListener('storage', sync);
      selects.destroy(); appearance.destroy();
    },
  };
}
