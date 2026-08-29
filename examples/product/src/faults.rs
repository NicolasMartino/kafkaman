//! A switch for making this service's dispatch handler fail on purpose.
//!
//! The example stack had never produced a single error: no span carried a
//! failure status, no log record was above `INFO`, and every DLQ was empty. So
//! the half of kafkaman that exists for when things go wrong — retry backoff,
//! the attempt budget, dead-lettering, redrive — was demonstrated by nothing and
//! proven by nothing. This is what lets `examples/faults.sh` drive it.
//!
//! # Why a `static` rather than state threaded through `AppState`
//!
//! Because the thing genuinely is process-global. It is one debug switch for one
//! process, read by the dispatch handler and written by an HTTP route, and both
//! boot paths (`service` and `service_manual`) have to see the same one. Passing
//! a handle instead would mean a new field on `AppState`, a new parameter on
//! `dispatch_router`, and a clone captured by each of the two handler closures —
//! three places to keep in step for a value that has exactly one instance.
//!
//! The state is one mutex-protected value. The handler only holds it long enough
//! to claim one budget unit, and the single lock makes each arming visible as
//! one coherent state rather than three independent atomic writes.
//!
//! # Why it is not in the library
//!
//! Fault injection is a property of this demo, not of kafkaman. Nothing here is
//! reachable from a published crate.

use std::sync::{Mutex, MutexGuard};

use kafkaman::sqlx::Error as KafkamanError;
use serde::{Deserialize, Serialize};
use sqlx::PgConnection;

/// The one switch, for this process.
static FAULTS: Mutex<FaultState> = Mutex::new(FaultState::new());

/// How the handler should misbehave.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum FaultMode {
    /// Behave normally.
    #[default]
    Off,
    /// Return `Err`, the way a well-behaved handler reports a failure. Retried
    /// on the row's budget and dead-lettered when it runs out.
    Error,
    /// Panic, the way a handler misbehaves. Caught at the handler boundary and
    /// treated as an error — the point being that the service stays up.
    Panic,
    /// Fail on a constraint the database refused to break, the way a handler
    /// writing a duplicate or a negative quantity does. Classified
    /// `urn:kafkaman:problem:constraint`.
    Constraint,
    /// Fail on a serialization failure or a deadlock, the way two handlers
    /// touching the same rows at once do. Classified
    /// `urn:kafkaman:problem:contention`.
    Contention,
    /// Fail on a statement the database would not run at all — bad data for the
    /// column, a relation the role cannot see. Classified
    /// `urn:kafkaman:problem:statement`.
    Statement,
}

impl FaultMode {
    /// The SQLSTATE this mode makes PostgreSQL raise, if it raises one.
    ///
    /// One code per class rather than per cause: `kafkaman-sqlx` classifies on
    /// the first two characters, so these three are exactly the three branches
    /// that lead anywhere other than `infrastructure`. A fourth code from class
    /// 23 would demonstrate nothing the first does not.
    const fn sqlstate(self) -> Option<&'static str> {
        match self {
            Self::Off | Self::Error | Self::Panic => None,
            // check_violation — a value the table refused.
            Self::Constraint => Some("23514"),
            // deadlock_detected — the one class that is nobody's fault and is
            // worth retrying unchanged.
            Self::Contention => Some("40P01"),
            // division_by_zero — a statement that could never have succeeded.
            Self::Statement => Some("22012"),
        }
    }
}

/// Arm the fault.
#[derive(Clone, Copy, Debug, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ArmRequest {
    pub mode: FaultMode,
    /// How many handler calls should fail. Omit to fail until disarmed.
    ///
    /// This is the whole difference between "transient failure, absorbed by the
    /// retry budget" and "permanent failure, dead-lettered" — one number.
    #[schema(example = 2)]
    pub remaining: Option<u32>,
}

/// What the fault is doing now.
#[derive(Clone, Copy, Debug, Serialize, utoipa::ToSchema)]
pub struct FaultStatus {
    pub mode: FaultMode,
    /// `null` when the mode is `off`, or when it fires until disarmed.
    pub remaining: Option<u32>,
    /// Handler calls this fault has failed since it was armed.
    pub fired: u64,
}

struct FaultState {
    mode: FaultMode,
    remaining: Option<u32>,
    fired: u64,
}

impl FaultState {
    const fn new() -> Self {
        Self {
            mode: FaultMode::Off,
            remaining: None,
            fired: 0,
        }
    }
}

/// Arm or re-arm the fault, resetting the fired count.
pub fn arm(request: ArmRequest) -> Result<FaultStatus, String> {
    if request.mode != FaultMode::Off && request.remaining == Some(0) {
        return Err("remaining must be greater than zero when mode is not off".to_owned());
    }

    let mut state = fault_state();
    *state = FaultState {
        mode: request.mode,
        remaining: match request.mode {
            FaultMode::Off => None,
            // Spelled out rather than a catch-all: a mode added later should
            // have to say whether it honours a budget, not inherit an answer.
            FaultMode::Error
            | FaultMode::Panic
            | FaultMode::Constraint
            | FaultMode::Contention
            | FaultMode::Statement => request.remaining,
        },
        fired: 0,
    };
    Ok(status_from(&state))
}

/// Disarm, leaving the fired count readable.
pub fn disarm() -> FaultStatus {
    let mut state = fault_state();
    state.mode = FaultMode::Off;
    state.remaining = None;
    status_from(&state)
}

/// What is armed right now.
pub fn status() -> FaultStatus {
    let state = fault_state();
    status_from(&state)
}

/// Consume one unit of the armed fault, if any.
///
/// Called at the top of the dispatch handler, before it touches anything, so a
/// fault is a clean failure rather than a half-applied one — the handler's own
/// writes are what the savepoint rollback is there to unwind, and this stays out
/// of that story.
///
/// Takes the dispatch transaction's own connection because three of the modes
/// fail *in the database*, and they have to fail on that connection to be the
/// thing they are imitating: a statement error aborts the enclosing transaction,
/// and unwinding that is the handler savepoint's entire job.
///
/// `#[allow(clippy::panic)]` because panicking on demand is the feature. The
/// workspace lint exists to stop panics reaching library paths, and this is an
/// example binary whose whole purpose here is to show what happens when one
/// does.
#[allow(clippy::panic)]
pub async fn check(conn: &mut PgConnection) -> Result<(), KafkamanError> {
    let Some((mode, fired)) = claim_fault() else {
        return Ok(());
    };
    tracing::warn!(
        fired,
        mode = ?mode,
        "injected fault firing in the product dispatch handler"
    );
    if mode == FaultMode::Panic {
        panic!("injected fault: the product handler panicked on call {fired}");
    }
    if let Some(sqlstate) = mode.sqlstate() {
        return Err(raise(conn, sqlstate, fired).await);
    }
    Err(KafkamanError::Handler(format!(
        "injected fault: the product handler failed on call {fired}"
    )))
}

/// Make PostgreSQL raise one specific SQLSTATE, and return what sqlx made of it.
///
/// `RAISE … USING ERRCODE` rather than a statement that genuinely violates
/// something, for two reasons. sqlx classifies on the five-character code alone,
/// so a real check violation and this one are the same value by the time
/// `kafkaman-sqlx` reads them — there is nothing more faithful to be had. And
/// two of these classes cannot be produced on demand from one handler at all: a
/// deadlock needs a second transaction to deadlock against.
///
/// Both interpolated values are program-controlled — a `&'static str` from
/// [`FaultMode::sqlstate`] and a counter — which is what makes the `format!`
/// safe. A `DO` block takes no bind parameters, so there is no alternative.
async fn raise(conn: &mut PgConnection, sqlstate: &str, fired: u64) -> KafkamanError {
    let sql = format!(
        "DO $$ BEGIN \
           RAISE EXCEPTION 'injected fault: the product handler hit a database \
                            error on call {fired}' \
           USING ERRCODE = '{sqlstate}'; \
         END $$"
    );
    match sqlx::query(&sql).execute(conn).await {
        Err(error) => error.into(),
        // Unreachable — the statement's only job is to fail. Reported anyway,
        // because the alternative is a fault that reads as armed and does
        // nothing, which is worse than one that fails loudly.
        Ok(_) => KafkamanError::Handler(format!(
            "injected fault: RAISE {sqlstate} did not fail on call {fired}"
        )),
    }
}

fn claim_fault() -> Option<(FaultMode, u64)> {
    let mut state = fault_state();
    if state.mode == FaultMode::Off {
        return None;
    }

    // `arm` rejects a budget of zero, so a `Some` budget here is always at least
    // one: the fault fires, and disarms itself if that spent the last of it.
    // There is deliberately no "budget already empty" branch — it would be
    // unreachable, and an unreachable branch in a state machine invites the next
    // reader to add the state that makes it reachable.
    if let Some(remaining) = state.remaining.as_mut() {
        *remaining -= 1;
    }

    let mode = state.mode;
    state.fired += 1;
    let fired = state.fired;
    if state.remaining == Some(0) {
        // Disarm on exhaustion, so `GET /faults` reports `off` rather than a
        // mode that can no longer fire.
        state.mode = FaultMode::Off;
        state.remaining = None;
    }
    Some((mode, fired))
}

fn status_from(state: &FaultState) -> FaultStatus {
    FaultStatus {
        mode: state.mode,
        remaining: if state.mode == FaultMode::Off {
            None
        } else {
            state.remaining
        },
        fired: state.fired,
    }
}

fn fault_state() -> MutexGuard<'static, FaultState> {
    FAULTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_lock() -> &'static Mutex<()> {
        static LOCK: std::sync::OnceLock<Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn arm_request_rejects_unknown_fields() {
        let body = serde_json::json!({
            "mode": "error",
            "remaning": 1,
        });

        assert!(serde_json::from_value::<ArmRequest>(body).is_err());
    }

    #[test]
    fn active_fault_rejects_zero_remaining() {
        let _guard = test_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        disarm();

        let err = arm(ArmRequest {
            mode: FaultMode::Error,
            remaining: Some(0),
        });

        assert!(matches!(err, Err(message) if message.contains("greater than zero")));
        assert_eq!(status().mode, FaultMode::Off);
    }

    /// Driven through `claim_fault` rather than `check`, which is the whole of
    /// the state machine and the only half that has no database in it. `check`
    /// adds the effect — an error, a panic, or a raised SQLSTATE — and the
    /// effect is what `examples/faults.sh` exercises against a real stack.
    #[test]
    fn bounded_fault_disarms_after_its_last_firing() {
        let _guard = test_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        disarm();

        let armed = arm(ArmRequest {
            mode: FaultMode::Error,
            remaining: Some(1),
        });
        assert!(armed.is_ok(), "arming should accept a positive budget");
        assert_eq!(
            claim_fault(),
            Some((FaultMode::Error, 1)),
            "the first call consumes the only unit"
        );

        let current = status();
        assert_eq!(current.mode, FaultMode::Off);
        assert_eq!(current.remaining, None);
        assert_eq!(current.fired, 1);
    }

    /// The three database modes are the only ones that name a SQLSTATE, and each
    /// names a distinct class — which is the whole point of having three of them
    /// rather than one. A second code from a class already covered would add a
    /// mode and demonstrate nothing.
    #[test]
    fn each_database_mode_raises_a_distinct_sqlstate_class() {
        let classes: Vec<&str> = [
            FaultMode::Constraint,
            FaultMode::Contention,
            FaultMode::Statement,
        ]
        .into_iter()
        .map(|mode| &mode.sqlstate().expect("a database mode raises a SQLSTATE")[..2])
        .collect();

        assert_eq!(classes, ["23", "40", "22"]);
        for mode in [FaultMode::Off, FaultMode::Error, FaultMode::Panic] {
            assert_eq!(
                mode.sqlstate(),
                None,
                "{mode:?} does not touch the database"
            );
        }
    }
}
