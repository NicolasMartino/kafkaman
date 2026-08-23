use std::time::Duration;

use tokio_util::sync::CancellationToken;

/// Sleep for `poll_interval`, waking early if shutdown is requested.
///
/// Returns `false` when the caller's loop should stop.
///
/// This is the one piece every run loop shares, and the one that is easy to get
/// wrong: a plain `sleep` that does not race the token makes a shutdown take a
/// full poll interval to be noticed, which on a 60-second retention interval is
/// a minute of a deploy waiting on nothing.
pub(crate) async fn sleep_or_shutdown(
    poll_interval: Duration,
    shutdown: &CancellationToken,
) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(poll_interval) => true,
        _ = shutdown.cancelled() => false,
    }
}
