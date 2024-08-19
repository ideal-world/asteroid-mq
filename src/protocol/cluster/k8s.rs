use k8s_openapi::api::core::v1::Pod;

use crate::protocol::node::NodeId;

pub struct K8sClusterProvider {
    namespace: String,
    client: kube::Client,
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
// impl K8sClusterProvider {
//     pub fn new(namespace: String, client: Client) -> Self {
//         K8sClusterProvider { namespace, client }
//     }

//     pub async fn watch_pods(&self, tx: mpsc::Sender<Pod>) {
//         let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
//         let lp = ListParams::default();

//         let pod_list = pods.list(&lp).await.unwrap();
//         for pod in pod_list.items {
//             let uid = pod.metadata.uid.expect("pod are expected to have a uid");

//         }
//     }
// }

// use futures_util::StreamExt;
// use k8s_openapi::api::core::v1::Pod;
// use kube::{api::ListParams, runtime::watcher::{self, Config, Event}, Api};

// use super::TcpClusterProvider;
// impl TcpClusterProvider for K8sClusterProvider {

//     async fn next_update(&mut self) -> super::TcpClusterInfo {
//         k8s_openapi::api::resource
//     }
// }
