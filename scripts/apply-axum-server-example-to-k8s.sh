kind load docker-image asteroid-axum-server-example:latest
kubectl apply -f server/examples/axum_server/resource.yaml
kubectl get pods
kubectl get svc