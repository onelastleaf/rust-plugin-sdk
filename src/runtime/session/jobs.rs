use std::{
    any::Any,
    collections::HashMap,
    panic::{AssertUnwindSafe, catch_unwind},
    task::Poll,
};

use tokio::task::JoinSet;

use crate::protocol::{self as oll, plugin_envelope};

use super::super::{
    ActionContext, ActionResult, SdkError,
    cancellation::{CancellationController, FinishGuard},
    host::HostClient,
    plugin::RegisteredAction,
    validation,
};

const CAPACITY_METADATA_KEY: &str = "maximum_concurrent_jobs";

pub(super) struct JobManager {
    actions: HashMap<String, RegisteredAction>,
    maximum_concurrent_jobs: usize,
    host: HostClient,
    active: HashMap<String, ActiveJob>,
    tasks: JoinSet<(String, Result<ActionResult, SdkError>)>,
}

impl JobManager {
    pub(super) fn new(
        actions: HashMap<String, RegisteredAction>,
        maximum_concurrent_jobs: usize,
        host: HostClient,
    ) -> Self {
        Self {
            actions,
            maximum_concurrent_jobs,
            host,
            active: HashMap::new(),
            tasks: JoinSet::new(),
        }
    }

    pub(super) fn has_active_jobs(&self) -> bool {
        !self.active.is_empty()
    }

    pub(super) async fn start(
        &mut self,
        message_id: u64,
        trace: oll::TraceContext,
        request: oll::StartJobRequest,
    ) -> Result<(), SdkError> {
        let job_id = validation::job_id(request.job_id.as_ref())?.to_owned();
        validation::optional_timestamp(request.deadline.as_ref(), "StartJobRequest deadline")?;
        if self.active.contains_key(&job_id) {
            return Err(SdkError::Protocol("duplicate active job ID".to_owned()));
        }
        let Some(oll::start_job_request::Invocation::Action(invocation)) = request.invocation
        else {
            return Err(SdkError::Protocol("unsupported job invocation".to_owned()));
        };
        let action = self
            .actions
            .get(&invocation.action)
            .cloned()
            .ok_or_else(|| SdkError::Protocol(format!("unknown action `{}`", invocation.action)))?;

        if self.active.len() >= self.maximum_concurrent_jobs {
            self.host
                .sender()
                .send(
                    Some(message_id),
                    trace,
                    plugin_envelope::Payload::ProtocolError(capacity_error(
                        self.maximum_concurrent_jobs,
                    )),
                )
                .await?;
            return Ok(());
        }

        self.host
            .sender()
            .send(
                Some(message_id),
                trace.clone(),
                plugin_envelope::Payload::JobAccepted(oll::JobAccepted {
                    job_id: Some(oll::PluginJobId {
                        value: job_id.clone(),
                    }),
                }),
            )
            .await?;

        let (controller, cancellation) = CancellationController::new();
        let context = ActionContext::new(
            job_id.clone(),
            request.deadline,
            trace.clone(),
            cancellation,
            message_id,
            self.host.clone(),
        );
        let task_controller = controller.clone();
        let task_job_id = job_id.clone();
        self.tasks.spawn(async move {
            let _finished = FinishGuard::new(task_controller);
            let output = run_action(action, context, invocation.arguments).await;
            (task_job_id, output)
        });
        let replaced = self.active.insert(
            job_id,
            ActiveJob {
                controller,
                trace,
                cancellations: Vec::new(),
            },
        );
        debug_assert!(replaced.is_none(), "validated active job was replaced");
        Ok(())
    }

    pub(super) async fn cancel(
        &mut self,
        message_id: u64,
        trace: oll::TraceContext,
        request: oll::CancelJobRequest,
    ) -> Result<(), SdkError> {
        let job_id = validation::job_id(request.job_id.as_ref())?.to_owned();
        validation::cancellation_reason(request.reason)?;
        let acknowledgement = PendingCancellation {
            reply_to: message_id,
            trace,
            job_id: oll::PluginJobId {
                value: job_id.clone(),
            },
        };
        if let Some(job) = self.active.get_mut(&job_id) {
            job.controller.cancel();
            job.cancellations.push(acknowledgement);
            return Ok(());
        }
        self.send_cancellation_acknowledgement(acknowledgement)
            .await
    }

    pub(super) async fn join_next(&mut self) -> Result<FinishedAction, SdkError> {
        let joined = self
            .tasks
            .join_next()
            .await
            .expect("active job count matches JoinSet tasks");
        let (job_id, output) = joined.map_err(|error| {
            SdkError::Action(format!(
                "action supervisor task ended without a result: {error}"
            ))
        })?;
        Ok(FinishedAction { job_id, output })
    }

    pub(super) async fn settle(&mut self, finished: FinishedAction) -> Result<(), SdkError> {
        let job = self
            .active
            .get_mut(&finished.job_id)
            .expect("completed action belongs to an active job");
        let cancellations = std::mem::take(&mut job.cancellations);
        let trace = job.trace.clone();

        if cancellations.is_empty() {
            // The job remains in `active` until the terminal update has entered
            // the ordered sender. A crossing CancelJobRequest therefore either
            // wins before this point or observes an inactive job afterward.
            self.host
                .sender()
                .send(
                    None,
                    trace,
                    plugin_envelope::Payload::JobUpdate(terminal_update(
                        &finished.job_id,
                        finished.output,
                    )),
                )
                .await?;
        } else {
            for cancellation in cancellations {
                self.send_cancellation_acknowledgement(cancellation).await?;
            }
        }

        self.active
            .remove(&finished.job_id)
            .expect("settled action remained active until its output was queued");
        Ok(())
    }

    pub(super) async fn shutdown(&mut self) -> Result<(), SdkError> {
        for job in self.active.values() {
            job.controller.cancel();
        }
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
        let cancellations = self
            .active
            .values_mut()
            .flat_map(|job| std::mem::take(&mut job.cancellations))
            .collect::<Vec<_>>();
        for cancellation in cancellations {
            self.send_cancellation_acknowledgement(cancellation).await?;
        }
        self.active.clear();
        Ok(())
    }

    async fn send_cancellation_acknowledgement(
        &self,
        cancellation: PendingCancellation,
    ) -> Result<(), SdkError> {
        self.host
            .sender()
            .send(
                Some(cancellation.reply_to),
                cancellation.trace,
                plugin_envelope::Payload::CancelJobAcknowledged(oll::CancelJobAcknowledged {
                    job_id: Some(cancellation.job_id),
                }),
            )
            .await
            .map(|_| ())
    }
}

struct ActiveJob {
    controller: CancellationController,
    trace: oll::TraceContext,
    cancellations: Vec<PendingCancellation>,
}

struct PendingCancellation {
    reply_to: u64,
    trace: oll::TraceContext,
    job_id: oll::PluginJobId,
}

pub(super) struct FinishedAction {
    job_id: String,
    output: Result<ActionResult, SdkError>,
}

fn terminal_update(job_id: &str, output: Result<ActionResult, SdkError>) -> oll::JobUpdate {
    let (state, result, error, artifacts) = match output {
        Ok(result) => match result.into_wire(job_id) {
            Ok((result, artifacts)) => (oll::JobState::Succeeded, result, None, artifacts),
            Err(error) => (
                oll::JobState::Failed,
                None,
                Some(outbound_error(&error)),
                Vec::new(),
            ),
        },
        Err(error) => (
            oll::JobState::Failed,
            None,
            Some(outbound_error(&error)),
            Vec::new(),
        ),
    };
    oll::JobUpdate {
        job_id: Some(oll::PluginJobId {
            value: job_id.to_owned(),
        }),
        state: state as i32,
        progress: Some(1.0),
        status_message: None,
        result,
        error,
        artifacts,
    }
}

async fn run_action(
    action: RegisteredAction,
    context: ActionContext,
    arguments: Vec<String>,
) -> Result<ActionResult, SdkError> {
    let mut future = catch_unwind(AssertUnwindSafe(|| (action.handler)(context, arguments)))
        .map_err(action_panic)?;
    std::future::poll_fn(move |context| {
        match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(context))) {
            Ok(output) => output,
            Err(payload) => Poll::Ready(Err(action_panic(payload))),
        }
    })
    .await
}

fn action_panic(payload: Box<dyn Any + Send>) -> SdkError {
    let message = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload");
    SdkError::Action(format!("action task panicked: {message}"))
}

fn outbound_error(error: &SdkError) -> oll::ProtocolError {
    let error = error.protocol_error();
    if validation::protocol_error(&error).is_ok() {
        error
    } else {
        oll::ProtocolError {
            code: oll::ErrorCode::Internal as i32,
            message: "action returned an invalid protocol error".to_owned(),
            retryable: false,
            metadata: HashMap::new(),
            details: Vec::new(),
        }
    }
}

fn capacity_error(maximum: usize) -> oll::ProtocolError {
    oll::ProtocolError {
        code: oll::ErrorCode::Unavailable as i32,
        message: "plugin action capacity is currently exhausted".to_owned(),
        retryable: true,
        metadata: HashMap::from([(CAPACITY_METADATA_KEY.to_owned(), maximum.to_string())]),
        details: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use tokio::sync::mpsc;

    use super::*;
    use crate::runtime::{
        plugin::Plugin,
        sender::{OutboundEnvelope, SessionIdentity, SessionSender},
    };

    const FIRST_JOB: &str = "00000000-0000-4000-8000-000000000001";
    const SECOND_JOB: &str = "00000000-0000-4000-8000-000000000002";

    fn trace(correlation_id: &str) -> oll::TraceContext {
        oll::TraceContext {
            correlation_id: correlation_id.to_owned(),
            parent_call_id: None,
            call_depth: 0,
            causal_depth: 0,
            task_id: Some("task".to_owned()),
            task_group_id: Some("group".to_owned()),
        }
    }

    fn start(job_id: &str, action: &str) -> oll::StartJobRequest {
        oll::StartJobRequest {
            job_id: Some(oll::PluginJobId {
                value: job_id.to_owned(),
            }),
            deadline: None,
            invocation: Some(oll::start_job_request::Invocation::Action(
                oll::ActionInvocation {
                    action: action.to_owned(),
                    arguments: Vec::new(),
                },
            )),
        }
    }

    fn cancellation(job_id: &str) -> oll::CancelJobRequest {
        oll::CancelJobRequest {
            job_id: Some(oll::PluginJobId {
                value: job_id.to_owned(),
            }),
            reason: oll::JobCancellationReason::UserRequest as i32,
        }
    }

    fn harness(
        plugin: Plugin,
        maximum_concurrent_jobs: usize,
    ) -> (JobManager, mpsc::Receiver<OutboundEnvelope>) {
        let (wire, receiver) = mpsc::channel(32);
        let sender = SessionSender::new(
            wire,
            SessionIdentity {
                session_id: "session".to_owned(),
                instance_id: "instance".to_owned(),
            },
        );
        let host = HostClient::new(sender, "session".to_owned(), 64 * 1024, 10);
        (
            JobManager::new(plugin.actions, maximum_concurrent_jobs, host),
            receiver,
        )
    }

    async fn receive(wire: &mut mpsc::Receiver<OutboundEnvelope>) -> oll::PluginEnvelope {
        wire.recv().await.unwrap().consume()
    }

    async fn assert_panicking_action_fails(plugin: Plugin) {
        let (mut jobs, mut wire) = harness(plugin, 1);
        jobs.start(1, trace("start"), start(FIRST_JOB, "panic"))
            .await
            .unwrap();
        let _accepted = receive(&mut wire).await;
        let finished = jobs.join_next().await.unwrap();
        jobs.settle(finished).await.unwrap();
        let update = receive(&mut wire).await;
        let Some(plugin_envelope::Payload::JobUpdate(update)) = update.payload else {
            panic!("panic did not produce a JobUpdate");
        };
        assert_eq!(update.state, oll::JobState::Failed as i32);
        assert_eq!(update.error.unwrap().code, oll::ErrorCode::Internal as i32);
    }

    #[tokio::test]
    async fn cancellation_waits_for_cooperative_action_cleanup() {
        let cleaned = Arc::new(AtomicBool::new(false));
        let action_cleaned = cleaned.clone();
        let plugin = Plugin::builder("org.example.test", "0.1.0")
            .action("wait", "wait", move |context, _| {
                let cleaned = action_cleaned.clone();
                async move {
                    context.cancellation().cancelled().await;
                    tokio::task::yield_now().await;
                    cleaned.store(true, Ordering::Release);
                    Ok(ActionResult::empty())
                }
            })
            .unwrap()
            .build()
            .unwrap();
        let (mut jobs, mut wire) = harness(plugin, 1);
        jobs.start(1, trace("start"), start(FIRST_JOB, "wait"))
            .await
            .unwrap();
        assert!(matches!(
            receive(&mut wire).await.payload,
            Some(plugin_envelope::Payload::JobAccepted(_))
        ));

        jobs.cancel(2, trace("cancel"), cancellation(FIRST_JOB))
            .await
            .unwrap();
        assert!(wire.try_recv().is_err());
        let finished = jobs.join_next().await.unwrap();
        assert!(cleaned.load(Ordering::Acquire));
        jobs.settle(finished).await.unwrap();
        let acknowledgement = receive(&mut wire).await;
        assert_eq!(acknowledgement.reply_to, Some(2));
        assert_eq!(acknowledgement.trace.as_ref(), Some(&trace("cancel")));
        assert!(matches!(
            acknowledgement.payload,
            Some(plugin_envelope::Payload::CancelJobAcknowledged(_))
        ));
        assert!(wire.try_recv().is_err());
    }

    #[tokio::test]
    async fn inactive_job_cancellation_is_idempotently_acknowledged() {
        let plugin = Plugin::builder("org.example.test", "0.1.0")
            .build()
            .unwrap();
        let (mut jobs, mut wire) = harness(plugin, 1);
        for message_id in [7, 8] {
            jobs.cancel(message_id, trace("cancel"), cancellation(FIRST_JOB))
                .await
                .unwrap();
            let acknowledgement = receive(&mut wire).await;
            assert_eq!(acknowledgement.reply_to, Some(message_id));
            assert!(matches!(
                acknowledgement.payload,
                Some(plugin_envelope::Payload::CancelJobAcknowledged(_))
            ));
        }
    }

    #[tokio::test]
    async fn cancellation_wins_if_completion_has_not_been_queued() {
        let plugin = Plugin::builder("org.example.test", "0.1.0")
            .action("done", "done", |_, _| async { Ok(ActionResult::empty()) })
            .unwrap()
            .build()
            .unwrap();
        let (mut jobs, mut wire) = harness(plugin, 1);
        jobs.start(1, trace("start"), start(FIRST_JOB, "done"))
            .await
            .unwrap();
        let _accepted = receive(&mut wire).await;
        let finished = jobs.join_next().await.unwrap();

        jobs.cancel(2, trace("cancel"), cancellation(FIRST_JOB))
            .await
            .unwrap();
        jobs.settle(finished).await.unwrap();
        let output = receive(&mut wire).await;
        assert!(matches!(
            output.payload,
            Some(plugin_envelope::Payload::CancelJobAcknowledged(_))
        ));
        assert!(wire.try_recv().is_err());
    }

    #[tokio::test]
    async fn late_cancellation_does_not_replace_a_queued_terminal_update() {
        let plugin = Plugin::builder("org.example.test", "0.1.0")
            .action("done", "done", |_, _| async { Ok(ActionResult::empty()) })
            .unwrap()
            .build()
            .unwrap();
        let (mut jobs, mut wire) = harness(plugin, 1);
        jobs.start(1, trace("start"), start(FIRST_JOB, "done"))
            .await
            .unwrap();
        let _accepted = receive(&mut wire).await;
        let finished = jobs.join_next().await.unwrap();
        jobs.settle(finished).await.unwrap();
        assert!(matches!(
            receive(&mut wire).await.payload,
            Some(plugin_envelope::Payload::JobUpdate(_))
        ));

        jobs.cancel(2, trace("cancel"), cancellation(FIRST_JOB))
            .await
            .unwrap();
        assert!(matches!(
            receive(&mut wire).await.payload,
            Some(plugin_envelope::Payload::CancelJobAcknowledged(_))
        ));
        assert!(wire.try_recv().is_err());
    }

    #[tokio::test]
    async fn action_future_panics_become_failed_terminal_updates() {
        let plugin = Plugin::builder("org.example.test", "0.1.0")
            .action("panic", "panic", |_, _| async {
                panic!("action failure");
                #[allow(unreachable_code)]
                Ok(ActionResult::empty())
            })
            .unwrap()
            .build()
            .unwrap();
        assert_panicking_action_fails(plugin).await;
    }

    #[tokio::test]
    async fn action_handler_panics_become_failed_terminal_updates() {
        let plugin = Plugin::builder("org.example.test", "0.1.0")
            .action("panic", "panic", |_, _| {
                panic!("handler failure");
                #[allow(unreachable_code)]
                std::future::ready(Ok(ActionResult::empty()))
            })
            .unwrap()
            .build()
            .unwrap();
        assert_panicking_action_fails(plugin).await;
    }

    #[test]
    fn malformed_action_errors_are_not_emitted_on_the_wire() {
        let error = outbound_error(&SdkError::Host(oll::ProtocolError::default()));
        assert_eq!(error.code, oll::ErrorCode::Internal as i32);
        assert!(!error.message.is_empty());
    }

    #[tokio::test]
    async fn configured_capacity_rejects_only_excess_admission() {
        let plugin = Plugin::builder("org.example.test", "0.1.0")
            .action("wait", "wait", |context, _| async move {
                context.cancellation().cancelled().await;
                Ok(ActionResult::empty())
            })
            .unwrap()
            .build()
            .unwrap();
        let (mut jobs, mut wire) = harness(plugin, 1);
        jobs.start(1, trace("first"), start(FIRST_JOB, "wait"))
            .await
            .unwrap();
        let _accepted = receive(&mut wire).await;
        jobs.start(2, trace("second"), start(SECOND_JOB, "wait"))
            .await
            .unwrap();
        let rejected = receive(&mut wire).await;
        assert_eq!(rejected.reply_to, Some(2));
        let Some(plugin_envelope::Payload::ProtocolError(error)) = rejected.payload else {
            panic!("capacity exhaustion did not reject StartJobRequest");
        };
        assert_eq!(error.code, oll::ErrorCode::Unavailable as i32);
        assert!(error.retryable);

        jobs.cancel(3, trace("cancel"), cancellation(FIRST_JOB))
            .await
            .unwrap();
        let finished = jobs.join_next().await.unwrap();
        jobs.settle(finished).await.unwrap();
        let _acknowledgement = receive(&mut wire).await;
    }

    #[tokio::test]
    async fn dropping_the_manager_aborts_owned_action_tasks() {
        struct DropSignal(Arc<AtomicBool>);

        impl Drop for DropSignal {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let action_dropped = dropped.clone();
        let plugin = Plugin::builder("org.example.test", "0.1.0")
            .action("wait", "wait", move |_, _| {
                let dropped = action_dropped.clone();
                async move {
                    let _drop = DropSignal(dropped);
                    std::future::pending::<()>().await;
                    Ok(ActionResult::empty())
                }
            })
            .unwrap()
            .build()
            .unwrap();
        let (mut jobs, mut wire) = harness(plugin, 1);
        jobs.start(1, trace("start"), start(FIRST_JOB, "wait"))
            .await
            .unwrap();
        let _accepted = receive(&mut wire).await;
        tokio::task::yield_now().await;
        drop(jobs);
        for _ in 0..10 {
            if dropped.load(Ordering::Acquire) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn shutdown_aborts_actions_and_settles_pending_job_cancellation() {
        let plugin = Plugin::builder("org.example.test", "0.1.0")
            .action("wait", "wait", |_, _| async {
                std::future::pending::<()>().await;
                Ok(ActionResult::empty())
            })
            .unwrap()
            .build()
            .unwrap();
        let (mut jobs, mut wire) = harness(plugin, 1);
        jobs.start(1, trace("start"), start(FIRST_JOB, "wait"))
            .await
            .unwrap();
        let _accepted = receive(&mut wire).await;
        jobs.cancel(2, trace("cancel"), cancellation(FIRST_JOB))
            .await
            .unwrap();

        jobs.shutdown().await.unwrap();
        assert!(!jobs.has_active_jobs());
        let output = receive(&mut wire).await;
        assert_eq!(output.reply_to, Some(2));
        assert!(matches!(
            output.payload,
            Some(plugin_envelope::Payload::CancelJobAcknowledged(_))
        ));
        assert!(wire.try_recv().is_err());
    }
}
