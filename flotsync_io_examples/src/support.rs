use kompact::prelude::{
    Component,
    ComponentDefinition,
    Fulfillable,
    KFuture,
    KPromise,
    block_until,
    promise,
};
use snafu::{FromString, Whatever};
use std::{sync::Arc, time::Duration};

const OUTCOME_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Terminal result promise held by one example component.
pub type OutcomePromise = KPromise<std::result::Result<(), Whatever>>;

/// Terminal result future held by the synchronous example runner.
pub type OutcomeFuture = KFuture<std::result::Result<(), Whatever>>;

/// Creates a fresh one-shot outcome pair for one example component run.
pub fn new_outcome_promise() -> (OutcomePromise, OutcomeFuture) {
    promise()
}

/// Completes the terminal outcome if it has not already been completed.
pub fn complete_outcome(
    outcome: &mut Option<OutcomePromise>,
    result: std::result::Result<(), Whatever>,
) {
    let Some(outcome) = outcome.take() else {
        return;
    };
    let _ = outcome.fulfil(result);
}

/// Waits for the terminal outcome while still surfacing component faults promptly.
pub fn wait_for_component_outcome<C>(
    component: &Arc<Component<C>>,
    mut outcome: OutcomeFuture,
    component_label: &str,
) -> std::result::Result<(), Whatever>
where
    C: ComponentDefinition + Sized + 'static,
{
    loop {
        match block_until(OUTCOME_POLL_INTERVAL, outcome) {
            Ok(Ok(result)) => return result,
            Ok(Err(_)) => {
                return Err(Whatever::without_source(format!(
                    "{component_label} outcome promise dropped"
                )));
            }
            Err(next_outcome) => outcome = next_outcome,
        }

        if component.is_faulty() {
            return Err(Whatever::without_source(format!(
                "{component_label} component faulted"
            )));
        }
        if component.is_destroyed() {
            return Err(Whatever::without_source(format!(
                "{component_label} component exited without recording an outcome"
            )));
        }
    }
}
