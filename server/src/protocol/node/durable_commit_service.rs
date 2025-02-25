use std::{collections::VecDeque, sync::Arc, time::Duration};

use asteroid_mq_model::TopicCode;
use chrono::Utc;
use tokio_util::sync::CancellationToken;

use crate::prelude::{DurableMessage, DurableService};

use super::{durable_message::DurableCommand, raft::MaybeLoadingRaft};
#[derive(Debug, Clone)]
pub struct DurableCommitService {
    ct: CancellationToken,
    pub(crate) durable_commands_queue: Arc<std::sync::RwLock<VecDeque<(TopicCode, DurableCommand)>>>,
    raft: MaybeLoadingRaft,
    service: DurableService,
    duration: Duration,
}

impl DurableCommitService {
    pub fn new(
        raft: MaybeLoadingRaft,
        durable_service: DurableService,
        ct: CancellationToken,
    ) -> Self {
        const DURABLE_COMMAND_COMMIT_DURATION: Duration = Duration::from_millis(100);
        Self {
            ct,
            durable_commands_queue: Arc::new(std::sync::RwLock::new(VecDeque::new())),
            raft,
            service: durable_service,
            duration: DURABLE_COMMAND_COMMIT_DURATION
        }
    }
    pub fn spawn(self) {
        tokio::spawn(self.run());
    }
    pub async fn run(self) -> crate::Result<()> {
        let mut commands = VecDeque::new();
        loop {
            tokio::select! {
                _ = self.ct.cancelled() => {
                    break;
                }
                _ = tokio::time::sleep(self.duration) => {

                }
            }

            // only one execute task at a time for each topic
            {
                let mut outer_commands = self.durable_commands_queue.write().unwrap();
                if outer_commands.is_empty() {
                    continue;
                }
                std::mem::swap(&mut commands, &mut outer_commands);
            }
            if self.raft.get().await.ensure_linearizable().await.is_err() {
                tracing::trace!("raft not leader, skip durable commands");
                continue;
            } else {
                tracing::trace!(?commands, "raft is leader, commit durable commands");
            };
            for (topic_code, command) in commands.drain(..) {
                let topic = topic_code.clone();
                let result = match command {
                    DurableCommand::Create(command) => {
                        self.service
                            .save(
                                topic,
                                DurableMessage {
                                    message: command,
                                    status: Default::default(),
                                    time: Utc::now(),
                                },
                            )
                            .await
                    }
                    DurableCommand::UpdateStatus(command) => {
                        if !command.status.is_empty() {
                            self.service.update_status(topic, command).await
                        } else {
                            Ok(())
                        }
                    }
                    DurableCommand::Archive(command) => self.service.archive(topic, command).await,
                };
                if let Err(err) = result {
                    tracing::error!(?err, "durable command failed");
                }
            }
        }
        Ok(())
    }
}
