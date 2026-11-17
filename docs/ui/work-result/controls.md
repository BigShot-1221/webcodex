# Work Result control corrections

[简体中文](controls.zh-CN.md)

Follow-up to the completed Work Result redesign, tracked in
[Issue #1253](https://github.com/yyjeqhc/webcodex/issues/1253). The user reported
an Appearance panel outside the embedded viewport, uneven header controls,
confusing tab focus and oversized empty-results actions.

The Appearance panel now uses viewport positioning, clamps to either edge and
repositions on resize or scroll. Escape restores focus to its summary; clicking
or focusing outside closes it. The portaled select menu still belongs to the
panel, so choosing a local preference does not dismiss its parent prematurely.
Disposal closes the panel and removes its listeners. This shared behavior also
applies to the Spreadsheet Appearance control; Workbench retains its existing
Instance options menu.

Work Result uses aligned 32px header controls, with 44px touch targets. Narrow
cards place the controls on one toolbar row when space permits. Refresh reserves
room for its loading label. Tabs retain their moving selection underline;
keyboard focus uses a separate dashed inset outline. Pointer selection does not
leave a keyboard outline.

Results actions use their content width and wrap together. Collapse appears only
when a file is expanded and returns focus to the file when invoked. An empty
following workspace hides Pin and the tracking explanation. A pinned snapshot
retains Follow changes even if the current workspace becomes clean. Snapshot,
identity and submission semantics are unchanged.

## Evidence and reproduction

These captures show shipped HTML with public synthetic data in a disposable
loopback MCP Host fixture. They do not establish native ChatGPT/Codex Host,
live-runtime or deployed-build acceptance. No stable/test service or credential
was changed.

![Narrow Appearance dropdown](controls-mobile.png)

![Results actions sized to content](controls-results.png)

```sh
node frontend/scripts/build-selection-motion.mjs --check
npm --prefix frontend run typecheck
node --test frontend/test/mcp-card-selection.test.mjs frontend/test/selection-motion-build.test.mjs src/mcp_tests/*.test.mjs
node scripts/ui-smoke/work-result-controls.mjs
node scripts/ui-smoke/mcp-workspaces.mjs
cargo test --locked -p webcodex --lib mcp::app_registry
cargo test --locked -p webcodex --lib work_result
cargo test --locked -p webcodex --lib retired_app_resources
```

The controls script writes screenshots and `report.json` beneath
`artifacts/work-result-controls/`. It covers light/dark themes, 320/390/800/1440px,
inline/fullscreen display, pointer/keyboard focus, nested dropdown selection,
resize, dismissal, empty/populated Results and coarse touch. Node tests cover
shared panel disposal and pinned-snapshot preservation; the existing MCP browser
matrix covers normal motion, reduced motion and forced colors.

Changed embedded assets advance cache identities to Work Result v36, Workbench
v9 and Spreadsheet v5. Previous identities remain retired. These cache versions
do not change the product version or deploy an updated Host resource.
