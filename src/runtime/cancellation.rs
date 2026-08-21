use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};

use tokio::sync::Notify;

use super::SdkError;

const ACTIVE: u8 = 0;
const CANCELLED: u8 = 1;
const FINISHED: u8 = 2;

struct CancellationState {
    value: AtomicU8,
    changed: Notify,
}

/// Cooperative cancellation state shared by one action and its derived work.
#[derive(Clone)]
pub struct Cancellation(Arc<CancellationState>);

impl Cancellation {
    /// Returns whether oll requested cancellation of the owning job.
    pub fn is_cancelled(&self) -> bool {
        self.0.value.load(Ordering::Acquire) == CANCELLED
    }

    /// Waits until the owning job is cancelled or its action has already ended.
    pub async fn cancelled(&self) {
        loop {
            let notified = self.0.changed.notified();
            tokio::pin!(notified);
            // Register before checking the atomic state. `notify_waiters` does
            // not retain a permit for a future that has not been polled yet.
            notified.as_mut().enable();
            if self.0.value.load(Ordering::Acquire) != ACTIVE {
                return;
            }
            notified.as_mut().await;
        }
    }

    pub(super) fn ensure_active(&self) -> Result<(), SdkError> {
        (self.0.value.load(Ordering::Acquire) == ACTIVE)
            .then_some(())
            .ok_or(SdkError::Cancelled)
    }
}

#[derive(Clone)]
pub(super) struct CancellationController(Cancellation);

impl CancellationController {
    pub(super) fn new() -> (Self, Cancellation) {
        let cancellation = Cancellation(Arc::new(CancellationState {
            value: AtomicU8::new(ACTIVE),
            changed: Notify::new(),
        }));
        (Self(cancellation.clone()), cancellation)
    }

    pub(super) fn cancel(&self) {
        if self
            .0
            .0
            .value
            .compare_exchange(ACTIVE, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.0.0.changed.notify_waiters();
        }
    }

    pub(super) fn finish(&self) {
        if self
            .0
            .0
            .value
            .compare_exchange(ACTIVE, FINISHED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.0.0.changed.notify_waiters();
        }
    }
}

pub(super) struct FinishGuard(CancellationController);

impl FinishGuard {
    pub(super) fn new(controller: CancellationController) -> Self {
        Self(controller)
    }
}

impl Drop for FinishGuard {
    fn drop(&mut self) {
        self.0.finish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancellation_waiters_observe_the_request_before_action_cleanup() {
        let (controller, cancellation) = CancellationController::new();
        controller.cancel();
        cancellation.cancelled().await;
        assert!(cancellation.is_cancelled());
        assert!(cancellation.ensure_active().is_err());
    }

    #[tokio::test]
    async fn finished_contexts_cannot_emit_more_work() {
        let (controller, cancellation) = CancellationController::new();
        controller.finish();
        cancellation.cancelled().await;
        assert!(!cancellation.is_cancelled());
        assert!(cancellation.ensure_active().is_err());
    }

    #[tokio::test]
    async fn every_cloned_waiter_is_released() {
        let (controller, cancellation) = CancellationController::new();
        let mut waiters = Vec::new();
        for _ in 0..16 {
            let cancellation = cancellation.clone();
            waiters.push(tokio::spawn(async move {
                cancellation.cancelled().await;
            }));
        }
        tokio::task::yield_now().await;
        controller.cancel();
        for waiter in waiters {
            waiter.await.unwrap();
        }
    }
}
