use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use moka::sync::Cache;
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use serde::Serialize;
use serde_json::json;
use snafu::{GenerateImplicitData, Location};
use tokio::{spawn, sync::Semaphore};
use tracing::{debug, error, info, warn};

use crate::{
    config::{CLEWDR_CONFIG, ClewdrConfig, CookieStatus, Reason, UselessCookie, CONFIG_PATH},
    error::ClewdrError,
};

const INTERVAL: u64 = 300;

#[derive(Debug, Serialize, Clone)]
pub struct CookieStatusInfo {
    pub valid: Vec<CookieStatus>,
    pub exhausted: Vec<CookieStatus>,
    pub invalid: Vec<UselessCookie>,
}

/// Messages that the CookieActor can handle
#[derive(Debug)]
enum CookieActorMessage {
    /// Return a Cookie
    Return(CookieStatus, Option<Reason>),
    /// Submit a new Cookie
    Submit(CookieStatus),
    /// Check for timed out Cookies
    CheckReset,
    /// Request to get a Cookie
    Request(Option<u64>, RpcReplyPort<Result<CookieStatus, ClewdrError>>),
    /// Get all Cookie status information
    GetStatus(RpcReplyPort<CookieStatusInfo>),
    /// Delete a Cookie
    Delete(CookieStatus, RpcReplyPort<Result<(), ClewdrError>>),
    /// Scheduled check for cookie validity (auto refresh)
    ScheduledCheck,
}

/// CookieActor state - manages collections of cookies
#[derive(Debug)]
struct CookieActorState {
    valid: VecDeque<CookieStatus>,
    exhausted: HashSet<CookieStatus>,
    invalid: HashSet<UselessCookie>,
    moka: Cache<u64, CookieStatus>,
}

/// Cookie actor that handles cookie distribution, collection, and status tracking using Ractor
struct CookieActor;

impl CookieActor {
    /// Gets API password from global configuration
    fn get_api_password() -> String {
        CLEWDR_CONFIG.load().password.clone()
    }

    /// Performs scheduled cookie validity check
    async fn perform_scheduled_check() {
        let config = CLEWDR_CONFIG.load();
        if !config.auto_refresh_cookie {
            debug!("Auto refresh disabled, skipping scheduled check");
            return;
        }

        info!("Spawning scheduled cookie validity check task");
        let addr = config.address();
        let test_url = format!("http://{}/v1/messages", addr);
        
        tokio::spawn(async move {
            // Get API password from global configuration
            let api_password = CookieActor::get_api_password();
            
            if api_password.is_empty() {
                warn!("API password is empty. Cookie testing may fail.");
                return;
            }
            
            // Get cookie count from current state - we'll estimate based on config
            let current_config = CLEWDR_CONFIG.load();
            let valid_count = current_config.cookie_array.len();
            if valid_count == 0 {
                info!("No cookies configured, skipping scheduled check");
                return;
            }
            
            info!("Testing {} cookies using round-robin mechanism at endpoint: {}", valid_count, test_url);
            
            // Create test message payload
            let test_payload = json!({
                "model": "claude-sonnet-4-20250514",
                "messages": [{"role": "user", "content": "hi"}],
                "max_tokens": 10
            });

            // Send requests equal to cookie count to ensure complete coverage
            let num_requests = valid_count;
            let batch_size = 10;
            let total_batches = (num_requests + batch_size - 1) / batch_size;
            
            info!("Sending {} requests in {} batches to test all cookies", num_requests, total_batches);

            // Create shared HTTP client for reuse
            let client = Arc::new(wreq::Client::new());

            // Process requests in batches to avoid overwhelming the service
            for batch_num in 0..total_batches {
                let batch_start = batch_num * batch_size;
                let batch_end = std::cmp::min(batch_start + batch_size, num_requests);
                let current_batch_size = batch_end - batch_start;
                
                info!("Processing batch {}/{}: {} requests (testing cookies {}-{})", 
                      batch_num + 1, total_batches, current_batch_size, batch_start + 1, batch_end);

                // Create futures for concurrent requests
                let futures: Vec<_> = (0..current_batch_size).map(|i| {
                    let client = client.clone();
                    let test_url = test_url.clone();
                    let test_payload = test_payload.clone();
                    let api_password = api_password.clone();
                    let request_num = batch_start + i + 1;
                    
                    async move {
                        debug!("Sending test request {} (will test next cookie in round-robin)", request_num);
                        
                        // Send test request - let service automatically select next cookie
                        let result = client
                            .post(&test_url)
                            .header("x-api-key", &api_password)
                            .json(&test_payload)
                            // No Cookie header - let service use round-robin selection
                            .send()
                            .await;
                        
                        match result {
                            Ok(response) => {
                                let status = response.status();
                                // Get response body for debugging
                                let body_text = response.text().await.unwrap_or_else(|_| "Failed to read response body".to_string());
                                if status.is_success() {
                                    debug!("Request {} SUCCESS ({}): response body: {}", 
                                          request_num, status, &body_text[..body_text.len().min(200)]);
                                } else {
                                    warn!("Request {} FAILED ({}): response body: {}", 
                                         request_num, status, &body_text[..body_text.len().min(500)]);
                                }
                            }
                            Err(e) => {
                                warn!("Request {} ERROR: {} (service will handle cookie state)", request_num, e);
                            }
                        }
                    }
                }).collect();

                // Use buffer_unordered for controlled concurrency
                use futures::stream::{self, StreamExt};
                let mut stream = stream::iter(futures).buffer_unordered(current_batch_size);
                while stream.next().await.is_some() {}
                
                info!("Batch {}/{} completed", batch_num + 1, total_batches);
                
                // Add delay between batches to avoid overwhelming the service
                if batch_num < total_batches - 1 {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
            
            info!("Cookie validity check completed! Sent {} requests to test all cookies. Service's round-robin mechanism ensured complete coverage and automatic state updates.", num_requests);
        });
    }
    /// Spawns a scheduled checker task that periodically sends ScheduledCheck messages
    fn spawn_scheduled_checker(actor_ref: ActorRef<CookieActorMessage>) {
        spawn(async move {
            let config = CLEWDR_CONFIG.load();
            let mut last_interval_hours = config.check_interval_hours;
            info!("Starting scheduled checker with {} hour interval", last_interval_hours);
            
            let mut interval = tokio::time::interval(Duration::from_secs(last_interval_hours * 3600));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;
                
                let current_config = CLEWDR_CONFIG.load();
                
                // Check if auto refresh is still enabled
                if !current_config.auto_refresh_cookie {
                    debug!("Auto refresh disabled, stopping scheduled checker");
                    break;
                }

                // Check if interval has changed and update if needed
                if current_config.check_interval_hours != last_interval_hours {
                    last_interval_hours = current_config.check_interval_hours;
                    interval = tokio::time::interval(Duration::from_secs(last_interval_hours * 3600));
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    info!("Scheduled checker interval updated to {} hours", last_interval_hours);
                }

                // Send ScheduledCheck message to the actor
                if let Err(e) = ractor::cast!(actor_ref, CookieActorMessage::ScheduledCheck) {
                    debug!("Failed to send ScheduledCheck message: {}, stopping scheduled checker", e);
                    break;
                }
            }
        });
    }

    /// Saves the current state of cookies to the configuration
    fn save(state: &CookieActorState) {
        CLEWDR_CONFIG.rcu(|config| {
            let mut config = ClewdrConfig::clone(config);
            config.cookie_array = state
                .valid
                .iter()
                .chain(state.exhausted.iter())
                .cloned()
                .collect();
            config.wasted_cookie = state.invalid.clone();
            config
        });

        tokio::spawn(async move {
            let result = CLEWDR_CONFIG.load().save().await;
            match result {
                Ok(_) => info!("Configuration saved successfully"),
                Err(e) => error!("Save task panicked: {}", e),
            }
        });
    }

    /// Logs the current state of cookie collections
    fn log(state: &CookieActorState) {
        info!(
            "Valid: {}, Exhausted: {}, Invalid: {}",
            state.valid.len().to_string().as_str(),
            state.exhausted.len().to_string().as_str(),
            state.invalid.len().to_string().as_str(),
        );
    }

    /// Checks and resets cookies that have passed their reset time
    fn reset(state: &mut CookieActorState) {
        let mut reset_cookies = Vec::new();
        state.exhausted.retain(|cookie| {
            let reset_cookie = cookie.clone().reset();
            if reset_cookie.reset_time.is_none() {
                reset_cookies.push(reset_cookie);
                false
            } else {
                true
            }
        });
        if reset_cookies.is_empty() {
            return;
        }
        state.valid.extend(reset_cookies);
        Self::log(state);
        Self::save(state);
    }

    /// Dispatches a cookie for use
    fn dispatch(
        state: &mut CookieActorState,
        hash: Option<u64>,
    ) -> Result<CookieStatus, ClewdrError> {
        Self::reset(state);
        if let Some(hash) = hash
            && let Some(cookie) = state.moka.get(&hash)
            && let Some(cookie) = state.valid.iter().find(|&c| c == &cookie)
        {
            // renew moka cache
            state.moka.insert(hash, cookie.clone());
            return Ok(cookie.clone());
        }
        let cookie = state
            .valid
            .pop_front()
            .ok_or(ClewdrError::NoCookieAvailable)?;
        state.valid.push_back(cookie.clone());
        if let Some(hash) = hash {
            state.moka.insert(hash, cookie.clone());
        }
        Ok(cookie)
    }

    /// Collects a returned cookie and processes it based on the return reason
    fn collect(state: &mut CookieActorState, mut cookie: CookieStatus, reason: Option<Reason>) {
        let Some(reason) = reason else {
            // replace the cookie in valid collection
            if cookie.token.is_some()
                && let Some(c) = state.valid.iter_mut().find(|c| **c == cookie)
            {
                *c = cookie;
                Self::save(state);
            }
            return;
        };
        let mut find_remove = |cookie: &CookieStatus| {
            state.valid.retain(|c| c != cookie);
        };
        match reason {
            Reason::NormalPro => {
                return;
            }
            Reason::TooManyRequest(i) => {
                find_remove(&cookie);
                cookie.reset_time = Some(i);
                if !state.exhausted.insert(cookie) {
                    return;
                }
            }
            Reason::Restricted(i) => {
                find_remove(&cookie);
                cookie.reset_time = Some(i);
                if !state.exhausted.insert(cookie) {
                    return;
                }
            }
            Reason::NonPro => {
                find_remove(&cookie);
                if !state
                    .invalid
                    .insert(UselessCookie::new(cookie.cookie, reason))
                {
                    return;
                }
            }
            _ => {
                find_remove(&cookie);
                if !state
                    .invalid
                    .insert(UselessCookie::new(cookie.cookie, reason))
                {
                    return;
                }
            }
        }
        Self::save(state);
        Self::log(state);
    }

    /// Accepts a new cookie into the valid collection
    fn accept(state: &mut CookieActorState, cookie: CookieStatus) {
        if CLEWDR_CONFIG.load().cookie_array.contains(&cookie)
            || CLEWDR_CONFIG
                .load()
                .wasted_cookie
                .iter()
                .any(|c| *c == cookie)
        {
            warn!("Cookie already exists");
            return;
        }
        state.valid.push_back(cookie);
        Self::save(state);
        Self::log(state);
    }

    /// Creates a report of all cookie statuses
    fn report(state: &CookieActorState) -> CookieStatusInfo {
        CookieStatusInfo {
            valid: state.valid.clone().into(),
            exhausted: state.exhausted.iter().cloned().collect(),
            invalid: state.invalid.iter().cloned().collect(),
        }
    }

    /// Deletes a cookie from all collections
    fn delete(state: &mut CookieActorState, cookie: CookieStatus) -> Result<(), ClewdrError> {
        let mut found = false;
        state.valid.retain(|c| {
            found |= *c == cookie;
            *c != cookie
        });
        let useless = UselessCookie::new(cookie.cookie.clone(), Reason::Null);
        found |= state.exhausted.remove(&cookie) | state.invalid.remove(&useless);

        if found {
            Self::save(state);
            Self::log(state);
            Ok(())
        } else {
            Err(ClewdrError::UnexpectedNone {
                msg: "Delete operation did not find the cookie",
            })
        }
    }
}

impl Actor for CookieActor {
    type Msg = CookieActorMessage;
    type State = CookieActorState;
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _arguments: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let valid = VecDeque::from_iter(
            CLEWDR_CONFIG
                .load()
                .cookie_array
                .iter()
                .filter(|c| c.reset_time.is_none())
                .cloned(),
        );
        let exhausted = HashSet::from_iter(
            CLEWDR_CONFIG
                .load()
                .cookie_array
                .iter()
                .filter(|c| c.reset_time.is_some())
                .cloned(),
        );
        let invalid = HashSet::from_iter(CLEWDR_CONFIG.load().wasted_cookie.iter().cloned());

        let moka = Cache::builder()
            .max_capacity(1000)
            .time_to_idle(std::time::Duration::from_secs(60 * 60))
            .build();

        let state = CookieActorState {
            valid,
            exhausted,
            invalid,
            moka,
        };

        CookieActor::log(&state);
        
        // Start scheduled check if auto refresh is enabled
        Self::spawn_scheduled_checker(myself);
        
        Ok(state)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            CookieActorMessage::Return(cookie, reason) => {
                Self::collect(state, cookie, reason);
            }
            CookieActorMessage::Submit(cookie) => {
                Self::accept(state, cookie);
            }
            CookieActorMessage::CheckReset => {
                Self::reset(state);
            }
            CookieActorMessage::Request(cache_hash, reply_port) => {
                let result = Self::dispatch(state, cache_hash);
                reply_port.send(result)?;
            }
            CookieActorMessage::GetStatus(reply_port) => {
                let status_info = Self::report(state);
                reply_port.send(status_info)?;
            }
            CookieActorMessage::Delete(cookie, reply_port) => {
                let result = Self::delete(state, cookie);
                reply_port.send(result)?;
            }
            CookieActorMessage::ScheduledCheck => {
                Self::perform_scheduled_check().await;
            }
        }
        Ok(())
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        CookieActor::save(state);
        Ok(())
    }
}

/// Handle for interacting with the CookieActor
#[derive(Clone)]
pub struct CookieActorHandle {
    actor_ref: ActorRef<CookieActorMessage>,
}

impl CookieActorHandle {
    /// Create a new CookieActor and return a handle to it
    pub async fn start() -> Result<Self, ractor::SpawnErr> {
        let (actor_ref, _join_handle) = Actor::spawn(None, CookieActor, ()).await?;

        // Start the timeout checker
        let handle = Self {
            actor_ref: actor_ref.clone(),
        };
        handle.spawn_timeout_checker().await;

        Ok(handle)
    }

    /// Spawns a timeout checker task
    async fn spawn_timeout_checker(&self) {
        let actor_ref = self.actor_ref.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(INTERVAL));
            loop {
                interval.tick().await;
                if let Err(e) = ractor::cast!(actor_ref, CookieActorMessage::CheckReset) {
                    debug!("Failed to send CheckReset message: {}, stopping timeout checker", e);
                    break;
                }
            }
        });
    }

    /// Request a cookie from the cookie actor
    pub async fn request(&self, cache_hash: Option<u64>) -> Result<CookieStatus, ClewdrError> {
        ractor::call!(self.actor_ref, CookieActorMessage::Request, cache_hash).map_err(|e| {
            ClewdrError::RactorError {
                loc: Location::generate(),
                msg: format!("Failed to communicate with CookieActor for request operation: {e}"),
            }
        })?
    }

    /// Return a cookie to the cookie actor
    pub async fn return_cookie(
        &self,
        cookie: CookieStatus,
        reason: Option<Reason>,
    ) -> Result<(), ClewdrError> {
        ractor::cast!(self.actor_ref, CookieActorMessage::Return(cookie, reason)).map_err(|e| {
            ClewdrError::RactorError {
                loc: Location::generate(),
                msg: format!("Failed to communicate with CookieActor for return operation: {e}"),
            }
        })
    }

    /// Submit a new cookie to the cookie actor
    pub async fn submit(&self, cookie: CookieStatus) -> Result<(), ClewdrError> {
        ractor::cast!(self.actor_ref, CookieActorMessage::Submit(cookie)).map_err(|e| {
            ClewdrError::RactorError {
                loc: Location::generate(),
                msg: format!("Failed to communicate with CookieActor for submit operation: {e}"),
            }
        })
    }

    /// Get status information about all cookies
    pub async fn get_status(&self) -> Result<CookieStatusInfo, ClewdrError> {
        ractor::call!(self.actor_ref, CookieActorMessage::GetStatus).map_err(|e| {
            ClewdrError::RactorError {
                loc: Location::generate(),
                msg: format!(
                    "Failed to communicate with CookieActor for get status operation: {e}"
                ),
            }
        })
    }

    /// Delete a cookie from the cookie actor
    pub async fn delete_cookie(&self, cookie: CookieStatus) -> Result<(), ClewdrError> {
        ractor::call!(self.actor_ref, CookieActorMessage::Delete, cookie).map_err(|e| {
            ClewdrError::RactorError {
                loc: Location::generate(),
                msg: format!("Failed to communicate with CookieActor for delete operation: {e}"),
            }
        })?
    }
}
