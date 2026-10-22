use crate::error::{DesktopError, DesktopResult};
use crate::updates::UpdateStatus;

pub fn dev_edition_enabled() -> bool {
    webcodex_runner_config::desktop_edition::desktop_dev_edition_enabled()
}

pub fn dev_edition_label() -> Option<&'static str> {
    dev_edition_enabled().then_some("Dev")
}

fn dev_update_error() -> DesktopError {
    DesktopError::new(
        "desktop_dev_update_unavailable",
        "WebCodex Desktop Dev cannot download or install production release updates",
        "Rebuild and install a new WebCodex Desktop Dev NSIS package to upgrade this test edition.",
    )
}

pub fn dev_update_status(_installed: &str, now: u64) -> UpdateStatus {
    UpdateStatus {
        state: "dev_edition".into(),
        latest: None,
        update_available: false,
        show_banner: false,
        cached: true,
        last_check_at_ms: Some(now),
        manual_error: None,
        automatic_download: false,
        download: crate::updates::DownloadStatus::default(),
    }
}

pub fn guard_production_update_action() -> DesktopResult<()> {
    if dev_edition_enabled() {
        Err(dev_update_error())
    } else {
        Ok(())
    }
}
