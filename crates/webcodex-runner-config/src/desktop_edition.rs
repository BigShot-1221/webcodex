//! Compile-time opt-in for the isolated Desktop Dev test edition.
//!
//! Set `WEBCODEX_DESKTOP_DEV_EDITION=1` when building Desktop, CLI, Server, and
//! Runner together. Default release and local product builds omit the variable
//! entirely so production paths and Windows service IDs stay unchanged.

/// True when this binary was built with `WEBCODEX_DESKTOP_DEV_EDITION=1`.
#[inline]
pub fn desktop_dev_edition_enabled() -> bool {
    matches!(option_env!("WEBCODEX_DESKTOP_DEV_EDITION"), Some("1"))
}

#[cfg(windows)]
pub fn windows_client_config_dir_name() -> &'static str {
    if desktop_dev_edition_enabled() {
        "webcodex-dev"
    } else {
        "webcodex"
    }
}

/// Final component under `%LOCALAPPDATA%` for Desktop updater cache roots.
pub fn desktop_app_local_data_component() -> &'static str {
    if desktop_dev_edition_enabled() {
        "dev.webcodex.desktop.dev"
    } else {
        "dev.webcodex.desktop"
    }
}

#[cfg(windows)]
pub fn windows_server_service_id() -> &'static str {
    if desktop_dev_edition_enabled() {
        "WebCodexDevServer"
    } else {
        "WebCodexServer"
    }
}

#[cfg(windows)]
pub fn windows_tunnel_service_id() -> &'static str {
    if desktop_dev_edition_enabled() {
        "WebCodexDevTunnel"
    } else {
        "WebCodexTunnel"
    }
}

#[cfg(windows)]
pub fn windows_runner_service_id_prefix() -> &'static str {
    if desktop_dev_edition_enabled() {
        "WebCodexDevRunner-"
    } else {
        "WebCodexRunner-"
    }
}

#[cfg(windows)]
pub fn windows_profile_tunnel_service_id(profile_id: &str) -> String {
    if desktop_dev_edition_enabled() {
        format!("WebCodexDevTunnel-{profile_id}")
    } else {
        format!("WebCodexTunnel-{profile_id}")
    }
}
