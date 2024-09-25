use std::{borrow::Cow, collections::BTreeMap, net::SocketAddr, sync::OnceLock};

use k8s_openapi::api::core::v1::{Endpoints, Pod, Service};

use crate::protocol::node::NodeId;

/// The node id is created by sha256 hash of the pod uid. Use [`NodeId::sha256`] to create a node id.
///
/// You may get the uid from the pod metadata through environment variables or other ways.
pub struct K8sClusterProvider {
    pub namespace: Cow<'static, str>,
    pub service: Cow<'static, str>,
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
    pub async fn new(service: impl Into<Cow<'static, str>>, port: u16) -> Self {
        let client = kube::Client::try_default()
            .await
            .expect("failed to create k8s client, is this program running in a k8s pod?");
        K8sClusterProvider {
            namespace: namespace_from_file().into(),
            service: service.into(),
            client,
            next_update: tokio::time::Instant::now(),
            poll_interval: std::time::Duration::from_secs(1),
            port,
        }
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

use kube::Api;

use super::ClusterProvider;
impl ClusterProvider for K8sClusterProvider {
    async fn next_update(&mut self) -> crate::Result<BTreeMap<NodeId, SocketAddr>> {
        tokio::time::sleep_until(self.next_update).await;
        self.next_update += self.poll_interval;
        let service_api: Api<Service> = Api::namespaced(self.client.clone(), &self.namespace);
        let service = service_api
            .get(&self.service)
            .await
            .expect("service not found");
        let port_mapped = service
            .spec
            .expect("service are expected to have a spec")
            .ports
            .expect("service are expected to have a port")
            .iter()
            .find(|p| {
                p.target_port.as_ref().is_some_and(|target| match target {
                    k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(target_port) => {
                        *target_port == self.port as i32
                    }
                    k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(
                        target_port,
                    ) => target_port.parse::<u16>() == Ok(self.port),
                })
            })
            .map(|p| p.port)
            .expect("we should find one") as u16;
        let endpoint_api: Api<Endpoints> = Api::namespaced(self.client.clone(), &self.namespace);
        let ep_list: Endpoints = endpoint_api
            .get(&self.service)
            .await
            .expect("endpoints not found");
        tracing::trace!(?ep_list, "k8s endpoints list");
        let subsets = ep_list.subsets.unwrap_or_default();
        let socket_addr_list = subsets.into_iter().flat_map(|subset| {
            subset
                .not_ready_addresses
                .into_iter()
                .flatten()
                .chain(subset.addresses.into_iter().flatten())
                .map(|address| {
                    let target = address.target_ref.expect("should have target ref");
                    let addr: SocketAddr = format!("{}:{}", address.ip, port_mapped)
                        .parse()
                        .expect("should be valid socket addr");
                    let node_id = NodeId::sha256(target.uid.expect("should have uid").as_bytes());
                    (addr, node_id)
                })
        });
        let mut nodes = BTreeMap::new();
        for (addr, node_id) in socket_addr_list {
            nodes.insert(node_id, addr);
        }
        tracing::debug!(?nodes, "update k8s cluster");
        Ok(nodes)
    }
}
