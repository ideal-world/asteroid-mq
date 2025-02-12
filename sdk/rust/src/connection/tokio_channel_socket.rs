use crate::{ClientNode, ClientNodeError};

use asteroid_mq::{
    prelude::Node, protocol::node::edge::connection::tokio_channel::TokioChannelSocket,
};
use asteroid_mq_model::{EdgeAuth, EdgeConfig, MaybeBase64Bytes, NodeId};

use tokio_util::bytes;
impl ClientNode {
    pub async fn connect_local(
        node: Node,
        auth: bytes::Bytes,
    ) -> Result<ClientNode, ClientNodeError> {
        let (socket_client, socket_server) = TokioChannelSocket::pair();
        let _node_id = node
            .create_edge_connection(
                socket_server,
                EdgeConfig {
                    peer_id: NodeId::snowflake(),
                    peer_auth: EdgeAuth {
                        payload: MaybeBase64Bytes(auth),
                    },
                },
            )
            .await?;
        let node = ClientNode::connect(socket_client).await?;
        Ok(node)
    }
    pub async fn connect_local_without_auth(node: Node) -> Result<ClientNode, ClientNodeError> {
        Self::connect_local(node, Default::default()).await
    }
}
