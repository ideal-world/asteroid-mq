use std::{borrow::Cow, collections::BTreeMap, sync::OnceLock};

use crate::protocol::node::NodeId;

/// The node id is created by sha256 hash of the pod uid. Use [`NodeId::sha256`] to create a node id.
///
/// You may get the uid from the pod metadata through environment variables or other ways.
pub struct K8sClusterProvider {
    pub namespace: Cow<'static, str>,
    pub host_name: Cow<'static, str>,
    pub client: kube::Client,
    next_update: tokio::time::Instant,
    pub poll_interval: std::time::Duration,
    pub port: u16,
}

pub fn this_pod_id() -> NodeId {
    NodeId::sha256(hostname_from_file().as_bytes())
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

fn hostname_from_file() -> &'static str {
    static NAMESPACE: OnceLock<&'static str> = OnceLock::new();
    NAMESPACE.get_or_init(|| {
        let ns = std::fs::read_to_string("/etc/hostname")
            .expect("failed to read namespace from file, is this program running in a k8s pod?")
            .trim()
            .to_string()
            .leak();
        ns
    })
}

impl K8sClusterProvider {
    pub async fn new(port: u16) -> Self {
        let client = kube::Client::try_default()
            .await
            .expect("failed to create k8s client, is this program running in a k8s pod?");
        K8sClusterProvider {
            namespace: namespace_from_file().into(),
            host_name: hostname_from_file().into(),
            client,
            next_update: tokio::time::Instant::now(),
            poll_interval: std::time::Duration::from_secs(1),
            port,
        }
    }
}

use kube::Api;

use super::ClusterProvider;
impl ClusterProvider for K8sClusterProvider {
    fn name(&self) -> Cow<'static, str> {
        Cow::Owned(format!("k8s/{}/{}", self.namespace, self.host_name,))
    }
    async fn pristine_nodes(&mut self) -> crate::Result<BTreeMap<NodeId, String>> {
        self.next_update().await
    }
    async fn next_update(&mut self) -> crate::Result<BTreeMap<NodeId, String>> {
        use k8s_openapi::api::apps::v1::StatefulSet;

        tokio::time::sleep_until(self.next_update).await;
        self.next_update += self.poll_interval;
        let ss_api: Api<StatefulSet> = Api::namespaced(self.client.clone(), &self.namespace);
        let ss_name = self.host_name.rsplit_once('-').expect("invalid pod name").0;
        let ss = ss_api.get(ss_name).await.expect("service not found");
        let sepc = ss.spec.expect("service are expected to have a spec");
        let service_name = sepc.service_name;
        let replicas = sepc
            .replicas
            .expect("service are expected to have replicas");
        let mut nodes = BTreeMap::new();
        for idx in 0..replicas {
            // StatefulSet pod 名称格式: <statefulset-name>-<ordinal>
            let pod_name = format!("{}-{}", ss_name, idx);

            // 构造 pod 的 hostname
            // StatefulSet pod 的 hostname 格式: <pod-name>.<service-name>.<namespace>.svc
            let host = format!("{}.{}.{}.svc", pod_name, service_name, self.namespace);

            // 使用 pod name 作为节点 ID
            let node_id = NodeId::sha256(pod_name.as_bytes());

            let addr = format!("{}:{}", host, self.port);

            nodes.insert(node_id, addr);
        }
        Ok(nodes)
    }
}
