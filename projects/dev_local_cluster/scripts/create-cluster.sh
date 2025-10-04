#!/usr/bin/env bash
#
# k3d Cluster Setup Script for ma114tsdb Local Development
#
# Creates a local Kubernetes cluster with:
# - 2 agent nodes for multi-node testing
# - Port forwarding for HTTP (8080) and Prometheus (9090)
# - Registry for local container images
# - Prometheus and Grafana pre-installed
#
# Usage:
#   ./create-cluster.sh          # Create cluster
#   ./create-cluster.sh --delete # Delete cluster

set -euo pipefail

CLUSTER_NAME="ma114tsdb-dev"
AGENTS=2
HTTP_PORT=8080
PROMETHEUS_PORT=9090
REGISTRY_PORT=5000

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_prerequisites() {
    log_info "Checking prerequisites..."

    if ! command -v k3d &> /dev/null; then
        log_error "k3d not found. Install from: https://k3d.io/v5.8.0/#installation"
        exit 1
    fi

    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl not found. Install from: https://kubernetes.io/docs/tasks/tools/"
        exit 1
    fi

    if ! command -v docker &> /dev/null; then
        log_error "docker not found. Install Docker Desktop or Docker Engine."
        exit 1
    fi

    log_info "All prerequisites satisfied"
}

delete_cluster() {
    log_info "Deleting cluster: $CLUSTER_NAME"

    if k3d cluster list | grep -q "$CLUSTER_NAME"; then
        k3d cluster delete "$CLUSTER_NAME"
        log_info "Cluster deleted successfully"
    else
        log_warn "Cluster $CLUSTER_NAME does not exist"
    fi
}

create_cluster() {
    log_info "Creating k3d cluster: $CLUSTER_NAME"

    # Check if cluster already exists
    if k3d cluster list | grep -q "$CLUSTER_NAME"; then
        log_warn "Cluster $CLUSTER_NAME already exists. Deleting first..."
        delete_cluster
    fi

    # Create cluster with registry and port forwarding
    k3d cluster create "$CLUSTER_NAME" \
        --agents "$AGENTS" \
        --port "${HTTP_PORT}:80@loadbalancer" \
        --port "${PROMETHEUS_PORT}:9090@loadbalancer" \
        --registry-create "ma114tsdb-registry:0.0.0.0:${REGISTRY_PORT}" \
        --wait

    log_info "Cluster created successfully"

    # Verify cluster
    kubectl cluster-info
    kubectl get nodes
}

install_prometheus() {
    log_info "Installing Prometheus and Grafana..."

    # Create monitoring namespace
    kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -

    # Create Prometheus deployment (simple deployment for local dev)
    cat <<EOF | kubectl apply -f -
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-config
  namespace: monitoring
data:
  prometheus.yml: |
    global:
      scrape_interval: 15s
    scrape_configs:
      - job_name: 'kubernetes-pods'
        kubernetes_sd_configs:
          - role: pod
        relabel_configs:
          - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
            action: keep
            regex: true
          - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_path]
            action: replace
            target_label: __metrics_path__
            regex: (.+)
          - source_labels: [__address__, __meta_kubernetes_pod_annotation_prometheus_io_port]
            action: replace
            regex: ([^:]+)(?::\d+)?;(\d+)
            replacement: \$1:\$2
            target_label: __address__
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prometheus
  namespace: monitoring
spec:
  replicas: 1
  selector:
    matchLabels:
      app: prometheus
  template:
    metadata:
      labels:
        app: prometheus
    spec:
      containers:
      - name: prometheus
        image: prom/prometheus:latest
        args:
          - '--config.file=/etc/prometheus/prometheus.yml'
          - '--storage.tsdb.path=/prometheus'
        ports:
        - containerPort: 9090
        volumeMounts:
        - name: config
          mountPath: /etc/prometheus
      volumes:
      - name: config
        configMap:
          name: prometheus-config
---
apiVersion: v1
kind: Service
metadata:
  name: prometheus
  namespace: monitoring
spec:
  selector:
    app: prometheus
  ports:
    - port: 9090
      targetPort: 9090
  type: LoadBalancer
EOF

    log_info "Waiting for Prometheus to be ready..."
    kubectl wait --for=condition=available --timeout=120s deployment/prometheus -n monitoring

    log_info "Prometheus installed successfully"
    log_info "Access Prometheus at: http://localhost:${PROMETHEUS_PORT}"
}

print_summary() {
    log_info "Cluster setup complete!"
    echo ""
    echo "Cluster Name:       $CLUSTER_NAME"
    echo "Nodes:              1 server + $AGENTS agents"
    echo "HTTP Port:          http://localhost:${HTTP_PORT}"
    echo "Prometheus:         http://localhost:${PROMETHEUS_PORT}"
    echo "Registry:           localhost:${REGISTRY_PORT}"
    echo ""
    echo "Next steps:"
    echo "  1. Verify cluster:     kubectl get nodes"
    echo "  2. Deploy ma114tsdb:   nx run ma114tsdb-runtime:deploy"
    echo "  3. View Prometheus:    open http://localhost:${PROMETHEUS_PORT}"
    echo ""
}

main() {
    # Parse arguments
    if [[ $# -gt 0 ]] && [[ "$1" == "--delete" ]]; then
        check_prerequisites
        delete_cluster
        exit 0
    fi

    check_prerequisites
    create_cluster
    install_prometheus
    print_summary
}

main "$@"
