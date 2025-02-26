use std::{collections::VecDeque, sync::Arc};

use asteroid_mq_model::TopicCode;
use chrono::Utc;
use tokio_util::sync::CancellationToken;

use crate::prelude::{DurableMessage, DurableService};

use super::{durable_message::DurableCommand, raft::MaybeLoadingRaft};
#[derive(Debug, Clone)]
pub struct DurableCommitService {
    ct: CancellationToken,
    pub(crate) durable_commands_queue:
        Arc<std::sync::RwLock<VecDeque<(TopicCode, DurableCommand)>>>,
    raft: MaybeLoadingRaft,
    service: DurableService,
}

impl DurableCommitService {
    pub fn new(
        raft: MaybeLoadingRaft,
        durable_service: DurableService,
        ct: CancellationToken,
    ) -> Self {
        Self {
            ct,
            durable_commands_queue: Arc::new(std::sync::RwLock::new(VecDeque::new())),
            raft,
            service: durable_service,
        }
    }
    pub fn spawn(self) {
        tokio::spawn(self.run());
    }
    pub async fn run(self) -> crate::Result<()> {
        let mut commands = VecDeque::new();
        loop {
            if self.ct.is_cancelled() {
                break;
            }
            let is_empty = { self.durable_commands_queue.read().unwrap().is_empty() };
            if is_empty {
                tokio::task::yield_now().await;
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
