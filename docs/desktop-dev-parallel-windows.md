# WebCodex Desktop Dev — parallel Windows install

This document describes the **opt-in Dev edition** built with `WEBCODEX_DESKTOP_DEV_EDITION=1`. It is intended for side-by-side testing with the normal **WebCodex Desktop** product on one Windows user account. Production installers and builds that do **not** set that variable are unchanged.

## What you should see after install

| | Production | Dev test edition |
|---|------------|------------------|
| Start menu / uninstall name | WebCodex Desktop | WebCodex Desktop Dev |
| Window title | WebCodex | WebCodex Dev |
| Tray tooltip | WebCodex Desktop | WebCodex Desktop Dev |
| Tauri identifier | `dev.webcodex.desktop` | `dev.webcodex.desktop.dev` |
| Main executable name | `WebCodex.exe` (unchanged) | `WebCodexDev.exe` (bundled `webcodex-runtime` still adjacent) |

Both apps can run at the same time: single-instance locks are per identifier, not global. The Dev NSIS installer targets only `WebCodexDev.exe` for “app already running” checks, so it must not prompt to close production `WebCodex.exe`.

## Data and configuration paths (Windows)

| Purpose | Production | Dev |
|---------|------------|-----|
| CLI profiles, credentials, environment store | `%APPDATA%\webcodex` | `%APPDATA%\webcodex-dev` |
| Runner state / logs | `%LOCALAPPDATA%\webcodex` | `%LOCALAPPDATA%\webcodex-dev` |
| Desktop app-local data (Tauri) | `%LOCALAPPDATA%\dev.webcodex.desktop` | `%LOCALAPPDATA%\dev.webcodex.desktop.dev` |
| Updater cache default (CLI) | `%LOCALAPPDATA%\dev.webcodex.desktop` | `%LOCALAPPDATA%\dev.webcodex.desktop.dev` |

Fallback when `APPDATA` / `LOCALAPPDATA` are missing uses `%USERPROFILE%\.config\webcodex-dev` and `%USERPROFILE%\.local\state\webcodex-dev` for Dev.

### `WEBCODEX_DESKTOP_DATA_DIR`

Production Desktop may honor this absolute-path override. **Dev edition refuses startup** if the variable is set, so a global override cannot point Dev at production app data or credentials.

## Windows services and tasks

Dev uses distinct service IDs (same hash suffix rules as production):

- `WebCodexDevServer`
- `WebCodexDevRunner-<account-hash>`
- `WebCodexDevTunnel` / `WebCodexDevTunnel-<profile-id>`

Production keeps `WebCodexServer`, `WebCodexRunner-*`, and `WebCodexTunnel*`. Stopping or configuring services from Dev should only touch Dev-owned IDs because specs are derived from the Dev environment store.

Each edition allocates its own loopback ports during setup; Dev does not read production’s `%APPDATA%\webcodex\environment` tree.

## Updates

Dev **does not** check, download, or install production release updates, and cannot open the production release page from the in-app update flow. Upgrade Dev by building and running a new Dev NSIS package (`scripts/build_desktop_windows_dev.ps1`).

### Upgrading from an older Dev package (`WebCodex.exe`)

Early Dev NSIS builds used the same `WebCodex.exe` basename as production, which could make the installer ask to close the wrong process. New Dev builds use `WebCodexDev.exe` instead.

1. **Quit only the Dev app** (tray title **WebCodex Desktop Dev** / window **WebCodex Dev**). Leave production **WebCodex Desktop** running if you use both.
2. Run the new Dev NSIS installer. It should only detect `WebCodexDev.exe` under the Dev install directory.
3. After upgrade, start Dev from **WebCodex Desktop Dev** in the Start menu. The new shortcut launches `WebCodexDev.exe`.
4. **Optional cleanup**: An old `WebCodex.exe` left inside the Dev install folder (for example `...\WebCodex Desktop Dev\WebCodex.exe`) is no longer used. You may delete that file manually after confirming `WebCodexDev.exe` starts correctly. **Do not** delete `WebCodex.exe` from the production install path (`WebCodex Desktop` without “Dev”).

Dev `productName`, Tauri `identifier`, `%APPDATA%\webcodex-dev`, and Dev Windows service names are unchanged. Production paths and `WebCodex.exe` are unchanged.

## Build (operator)

From the repository root, with Rust **1.92.0**, Node **22**, and **no Git** invoked by the script:

```powershell
.\scripts\build_desktop_windows_dev.ps1 -SourceSha <40-hex-commit> -BuiltAt <unix-epoch-seconds>
```

Outputs:

- Runtime artifacts: `target/desktop-dev-runtime/`
- Tauri build tree: `target/desktop-dev-tauri/`
- Installer: `target/desktop-dev-dist/webcodex-desktop-dev-<short-sha>-v<version>-win32-x64-setup.exe` (+ `.sha256`)

The script always records `dirty=true` in embedded build identity. Use a **fresh** Dev runtime build; do not mix binaries from `target/dogfood` or `target/desktop-local-tauri`.

## Manual verification checklist

1. Install production Desktop (if not already) and Dev NSIS; confirm two entries under Settings → Apps.
2. Start both; confirm two tray icons and titles **WebCodex** vs **WebCodex Dev**.
3. Confirm `%APPDATA%\webcodex` and `%APPDATA%\webcodex-dev` are separate after each completes first-time setup.
4. In `services.msc`, confirm `WebCodexServer` vs `WebCodexDevServer` when each edition runs a local server.
5. With `WEBCODEX_DESKTOP_DATA_DIR` set globally, production may still start; **Dev must fail closed** with a clear error.
6. Dev Settings → Updates should not download or install the production NSIS.

## Limits and risks

- **Icons**: Dev uses the same icon assets as production unless you add a distinct asset later; naming and window title carry the Dev label.
- **PATH / global CLI**: A `webcodex` on `PATH` from a production install is unrelated to Dev’s bundled runtime; Dev Desktop uses its staged `webcodex-runtime` next to `WebCodexDev.exe`.
- **Elevation / policy**: Corporate policy blocking multiple installers or services is outside this edition split.
- **Cross-edition CLI**: Running `webcodex` from a production install still uses `%APPDATA%\webcodex`; only binaries built with `WEBCODEX_DESKTOP_DEV_EDITION=1` use `webcodex-dev` paths.
