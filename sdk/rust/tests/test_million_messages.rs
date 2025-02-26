use asteroid_mq_model::{
    codec::{CodecKind, DynCodec},
    EdgeMessage, Interest, MessageAckExpectKind, MessageDurableConfig, Subject, TopicCode,
};
use asteroid_mq_sdk::ClientNode;
use chrono::{TimeDelta, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct HelloMessage {
    pub message: String,
}
const CODEC: &str = "bincode";
const SIZE: usize = 100_000;
fn get_codec() -> DynCodec {
    let codec_kind = CODEC.parse::<CodecKind>().unwrap();
    DynCodec::form_kind(codec_kind).unwrap()
}

async fn get_ws_url() -> Result<String, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    const NODE_ID_API: &str = "http://localhost:8080/node_id";
    let node_id = client.put(NODE_ID_API).send().await?.text().await?;
    let url = format!("ws://localhost:8080/connect?node_id={node_id}&codec={CODEC}");
    Ok(url)
}

#[tokio::test]
async fn test_million_messages() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    if std::env::var("K8S_PROVIDER").is_err() {
        // let build_serve_result = tokio::process::Command::new("cargo")
        //     .arg("build")
        //     .arg("-p")
        //     .arg("asteroid-mq")
        //     .arg("--example")
        //     .arg("axum-server")
        //     .arg("--features")
        //     .arg("cluster-k8s")
        //     .arg("--release")
        //     .spawn()?
        //     .wait()
        //     .await?;
        // if !build_serve_result.success() {
        //     return Err("Failed to build axum-server example".into());
        // }
        // let _server_process = tokio::process::Command::new("cargo")
        //     .arg("run")
        //     .arg("-p")
        //     .arg("asteroid-mq")
        //     .arg("--example")
        //     .arg("axum-server")
        //     .arg("--features")
        //     .arg("cluster-k8s")
        //     .arg("--release")
        //     .spawn()?;
        // tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
    let url_a = get_ws_url().await?;
    let url_b = get_ws_url().await?;
    let node_a = ClientNode::connect_ws2(url_a, get_codec()).await?;
    let node_b = ClientNode::connect_ws2(url_b, get_codec()).await?;
    const TOPIC_CODE: TopicCode = TopicCode::const_new("test");
    const SUBJECT: Subject = Subject::const_new("event/millions-must-send");

    let mut endpoint_b = node_b
        .create_endpoint(TOPIC_CODE, [Interest::new("event/*")])
        .await?;

    endpoint_b
        .update_interests([Interest::new("event/*")])
        .await?;
    let recv_task = tokio::spawn(async move {
        let mut ack_join_set = tokio::task::JoinSet::new();
        let mut recv_count = 0;
        while let Some(message) = endpoint_b.next_message().await {
            recv_count += 1;
            if recv_count % (SIZE / 10) == 0 {
                tracing::info!("recv {recv_count} message");
            }
            ack_join_set.spawn(async move {
                let result = message.ack_processed().await;
                if result.is_err() {
                    tracing::error!(?result);
                }
            });
            if recv_count == SIZE {
                break;
            }
        }
        tracing::info!("message sent finished, wait for ack finished");
        let mut join_count = 0;
        while let Some(_result) = ack_join_set.join_next().await {
            join_count += 1;
            if join_count % (SIZE / 10) == 0 {
                tracing::info!("join {join_count} message");
            }
        }
        tracing::info!("waiting ack finished")
    });
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    let mut send_task_set = tokio::task::JoinSet::new();
    for _ in 0..SIZE {
        let node_a = node_a.clone();
        send_task_set.spawn(async move {
            node_a
                .send_message_and_wait(
                    EdgeMessage::builder(TOPIC_CODE, [SUBJECT], "durable message")
                        .mode_durable(
                            MessageDurableConfig::new(Utc::now() + TimeDelta::minutes(10))
                                .with_max_receiver(1),
                        )
                        .ack_kind(MessageAckExpectKind::Received)
                        .build(),
                )
                .await
        });
    }
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    send_task_set.join_all().await;

    recv_task.await?;
    drop(node_b);
    Ok(())
}
