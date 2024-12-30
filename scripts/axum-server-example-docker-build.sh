cargo build --example axum-server --features="cluster-k8s"
docker build -t asteroid-axum-server-example -f server/examples/axum_server/DockerFile .