use std::time::Duration;

use tokio::spawn;
use tracing::{debug, info};

use crate::{
    config::CLEWDR_CONFIG,
    services::cookie_actor::CookieActorHandle,
};


/// Auto refresh service that periodically checks cookie validity
pub struct AutoRefreshService {
    cookie_handle: CookieActorHandle,
}

impl AutoRefreshService {
    pub fn new(cookie_handle: CookieActorHandle) -> Self {
        Self {
            cookie_handle,
        }
    }

    /// Start the auto refresh service
    pub async fn start(self) {
        let config = CLEWDR_CONFIG.load();
        
        if !config.auto_refresh_cookie {
            debug!("Auto refresh is disabled, not starting service");
            return;
        }

        let interval_hours = config.check_interval_hours;
        info!("Starting auto refresh service with {} hour interval", interval_hours);
        
        spawn(async move {
            let mut last_interval_hours = interval_hours;
            let mut interval = tokio::time::interval(Duration::from_secs(last_interval_hours * 3600));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;
                
                let current_config = CLEWDR_CONFIG.load();
                
                // Check if auto refresh is still enabled
                if !current_config.auto_refresh_cookie {
                    debug!("Auto refresh disabled, stopping service");
                    break;
                }

                // Check if interval has changed and update if needed
                if current_config.check_interval_hours != last_interval_hours {
                    last_interval_hours = current_config.check_interval_hours;
                    interval = tokio::time::interval(Duration::from_secs(last_interval_hours * 3600));
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    info!("Auto refresh interval updated to {} hours", last_interval_hours);
                }

                info!("Auto refresh cycle triggered - feature will be implemented in future version");
                // TODO: Implement actual cookie testing functionality
            }
        });
    }
}