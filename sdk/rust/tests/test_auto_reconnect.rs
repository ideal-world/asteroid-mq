use std::time::Duration;

use asteroid_mq_model::{
    codec::{CodecKind, DynCodec},
    EdgeMessage, Interest, Subject, TopicCode,
};
use asteroid_mq_sdk::ClientNode;

const CODEC: &str = "bincode";

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
async fn test_auto_reconnect() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
    .with_max_level(tracing::Level::DEBUG)
    .init();
    fn run_server() -> Result<tokio::process::Child, tokio::io::Error>{
        tokio::process::Command::new("cargo")
        .arg("run")
        .arg("-p")
        .arg("asteroid-mq")
        .arg("--example")
        .arg("axum-server")
        .arg("--features")
        .arg("cluster-k8s")
        .spawn()
    }
    let build_serve_result = tokio::process::Command::new("cargo")
        .arg("build")
        .arg("-p")
        .arg("asteroid-mq")
        .arg("--example")
        .arg("axum-server")
        .arg("--features")
        .arg("cluster-k8s")
        .spawn()?
        .wait()
        .await?;
    if !build_serve_result.success() {
        return Err("Failed to build axum-server example".into());
    }
    let mut server_process = run_server()?;
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    let url_a = get_ws_url().await?;
    let url_b = get_ws_url().await?;
    let node_a = ClientNode::connect_ws2(url_a, get_codec()).await?;
    let node_b = ClientNode::connect_ws2(url_b, get_codec()).await?;

    const TOPIC_CODE: TopicCode = TopicCode::const_new("test");
    fn message(message: &'static str) -> EdgeMessage {
        EdgeMessage::builder(
            TOPIC_CODE,
            [
                Subject::new("event/hello"),
                Subject::new("event/hello/avatar/b2"),
            ],
            message,
        )
        .build()
    }
    let mut endpoint_b1 = node_b
        .create_endpoint(TOPIC_CODE, [Interest::new("event/*")])
        .await?;
    let mut endpoint_b2 = node_b
        .create_endpoint(TOPIC_CODE, [Interest::new("event/**/b2")])
        .await?;
    endpoint_b1
        .update_interests([Interest::new("event/hello")])
        .await?;
    let task_b1 = tokio::spawn(async move {
        while let Ok(message) = endpoint_b1.next_message_and_auto_respawn().await {
            tracing::info!("Received message in b1: {:?}", &message.payload.0);
            let _result = message.ack_processed().await;
        }
    });
    let task_b2 = tokio::spawn(async move {
        while let Ok(message) = endpoint_b2.next_message_and_auto_respawn().await {
            tracing::info!("Received message in b2: {:?}", &message.payload.0);
            let _result = message.ack_processed().await;
        }

    });
    node_a.send_message(message("before-server-error-0")).await?;
    node_a.send_message(message("before-server-error-1")).await?;
    node_a.send_message(message("before-server-error-2")).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    server_process.kill().await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut server_process = run_server()?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let handle_0= node_a.send_message(message("after-server-error-0")).await?;
    tracing::info!("message send out");
    let wait_result = handle_0.wait().await;
    tracing::info!(?wait_result, "message 0 result");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let handle_1= node_a.send_message(message("after-server-error-1")).await?;
    let handle_2= node_a.send_message(message("after-server-error-2")).await?;
    let _wait_result = handle_1.wait().await?;
    let _wait_result = handle_2.wait().await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    server_process.kill().await?;
    Ok(())
}