use std::{
    borrow::Cow,
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::OnceLock,
};

use k8s_openapi::api::core::v1::Pod;

use crate::protocol::node::{Node, NodeId, NodeInfo};

/// The node id is created by sha256 hash of the pod uid. Use [`NodeId::sha256`] to create a node id.
///
/// You may get the uid from the pod metadata through environment variables or other ways.
pub struct K8sClusterProvider {
    pub namespace: Cow<'static, str>,
    pub param: ListParams,
    pub client: kube::Client,
    next_update: tokio::time::Instant,
    pub poll_interval: std::time::Duration,
    pub port: u16,
}

fn namespace_from_file() -> &'static str {
    static NAMESPACE: OnceLock<&'static str> = OnceLock::new();
    NAMESPACE.get_or_init(|| {
        let ns = std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
            .expect("failed to read namespace from file, is this program running in a k8s pod?")
            .trim()
            .to_string()
            .leak();
        ns
    })
}

impl K8sClusterProvider {
    pub async fn new(label: impl Into<String>, port: u16) -> Self {
        let client = kube::Client::try_default()
            .await
            .expect("failed to create k8s client, is this program running in a k8s pod?");

        let param = ListParams {
            label_selector: Some(label.into()),
            ..Default::default()
        };
        K8sClusterProvider {
            namespace: namespace_from_file().into(),
            param,
            client,
            next_update: tokio::time::Instant::now(),
            poll_interval: std::time::Duration::from_secs(1),
            port,
        }
    }
    pub async fn run(self, uid: String) -> Result<(), crate::Error> {
        let node = Node::new(NodeInfo::new_cluster_by_id(NodeId::sha256(uid.as_bytes())));
        let port = self.port;
        node.create_cluster(self, SocketAddr::new(crate::DEFAULT_TCP_ADDR, port))
            .await
            .map_err(crate::Error::contextual("create k8s cluster"))?;
        Ok(())
    }
}

impl NodeId {
    pub fn from_pod(pod: &Pod) -> Self {
        let uid = pod
            .metadata
            .uid
            .as_ref()
            .expect("pod are expected to have a uid");
        NodeId::sha256(uid.as_bytes())
    }
}

use kube::{api::ListParams, Api};

use super::TcpClusterProvider;
impl TcpClusterProvider for K8sClusterProvider {
    async fn next_update(&mut self) -> super::TcpClusterInfo {
        tokio::time::sleep_until(self.next_update).await;
        self.next_update += self.poll_interval;
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        let pod_list = pods.list(&self.param).await.unwrap();
        let mut nodes = HashMap::new();
        for pod in pod_list.items {
            let node_id = NodeId::from_pod(&pod);
            let addr = format!(
                "{}:{}",
                pod.status
                    .expect("pod are expected to have a status")
                    .host_ip
                    .expect("pod are expected to have a pod_ip"),
                self.port
            );
            nodes.insert(node_id, addr.parse().unwrap());
        }

        super::TcpClusterInfo {
            size: nodes.len() as u64,
            nodes,
        }
    }
}
