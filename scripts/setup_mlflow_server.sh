#!/bin/bash
# MLflow Tracking Server Setup for AWS

set -e

echo "=== MLflow Tracking Server Setup ==="
echo ""
echo "Choose your deployment option:"
echo "1. Local MLflow UI (dev/testing)"
echo "2. EC2-based MLflow Server (production)"
echo "3. Docker-based MLflow Server (local)"
echo ""

# Option 1: Local MLflow UI
setup_local_ui() {
    echo "Setting up local MLflow UI..."
    
    # Create local mlruns directory synced with S3
    mkdir -p mlruns
    
    # Sync from S3 (if any data exists)
    aws s3 sync s3://contexq-dev-mlflow-artifacts-119287772129/mlruns/ ./mlruns/ || true
    
    echo ""
    echo "Starting MLflow UI..."
    echo "Access at: http://localhost:5000"
    echo ""
    mlflow ui --backend-store-uri ./mlruns --default-artifact-root s3://contexq-dev-mlflow-artifacts-119287772129/mlruns --host 0.0.0.0 --port 5000
}

# Option 2: EC2 MLflow Server Setup Script
generate_ec2_setup() {
    cat > mlflow_ec2_setup.sh <<'EOF'
#!/bin/bash
# Run this on an EC2 instance (Amazon Linux 2023)

sudo yum update -y
sudo yum install -y python3.11 python3.11-pip postgresql15

# Install MLflow and dependencies
pip3.11 install mlflow==2.9.2 boto3 psycopg2-binary

# Create MLflow user and directory
sudo useradd -m mlflow
sudo mkdir -p /opt/mlflow
sudo chown mlflow:mlflow /opt/mlflow

# Create systemd service
sudo tee /etc/systemd/system/mlflow.service > /dev/null <<'SERVICE'
[Unit]
Description=MLflow Tracking Server
After=network.target

[Service]
Type=simple
User=mlflow
WorkingDirectory=/opt/mlflow
Environment="PATH=/usr/local/bin:/usr/bin:/bin"
ExecStart=/usr/local/bin/mlflow server \
    --backend-store-uri postgresql://mlflow:PASSWORD@RDS_ENDPOINT:5432/mlflow \
    --default-artifact-root s3://contexq-dev-mlflow-artifacts-119287772129/mlruns \
    --host 0.0.0.0 \
    --port 5000
Restart=always

[Install]
WantedBy=multi-user.target
SERVICE

# Start service
sudo systemctl daemon-reload
sudo systemctl enable mlflow
sudo systemctl start mlflow

echo "MLflow server started on port 5000"
echo "Configure security group to allow port 5000"
EOF
    
    echo "Generated mlflow_ec2_setup.sh"
    echo "Upload to EC2 and run to set up tracking server"
}

# Option 3: Docker-based MLflow Server
setup_docker() {
    cat > docker-compose.mlflow.yml <<'EOF'
version: '3.8'

services:
  mlflow:
    image: ghcr.io/mlflow/mlflow:v2.9.2
    container_name: mlflow-server
    ports:
      - "5000:5000"
    environment:
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
      - AWS_DEFAULT_REGION=us-east-1
    command: >
      mlflow server
      --backend-store-uri sqlite:///mlflow/mlflow.db
      --default-artifact-root s3://contexq-dev-mlflow-artifacts-119287772129/mlruns
      --host 0.0.0.0
      --port 5000
    volumes:
      - mlflow-data:/mlflow

volumes:
  mlflow-data:
EOF
    
    echo "Generated docker-compose.mlflow.yml"
    echo ""
    echo "Start with:"
    echo "  docker-compose -f docker-compose.mlflow.yml up -d"
    echo ""
    echo "Access at: http://localhost:5000"
}

# Main menu
read -p "Enter option (1-3): " option

case $option in
    1)
        setup_local_ui
        ;;
    2)
        generate_ec2_setup
        ;;
    3)
        setup_docker
        ;;
    *)
        echo "Invalid option"
        exit 1
        ;;
esac
