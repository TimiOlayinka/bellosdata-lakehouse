provider "aws" {
  region  = "eu-west-2"
  profile = "playEngineer"
}

# ═══════════════════════════════════════════════════════════════
# IAM ROLES — Managed via AWS CLI (PowerUserAccess cannot list/read IAM policies)
# ═══════════════════════════════════════════════════════════════
#
# Role: ecs_execution_role
#   ARN: arn:aws:iam::332779204498:role/ecs_execution_role
#   Attached: AmazonECSTaskExecutionRolePolicy
#
# Role: ledger-cloud-api-role
#   ARN: arn:aws:iam::332779204498:role/ledger-cloud-api-role
#   Attached: AWSLambdaBasicExecutionRole, AmazonS3ReadOnlyAccess
#
# IAM User: bellosdata-platform (created manually in console)
#   Attached: AmazonS3FullAccess
#   Access keys stored on data-platform Lightsail instance
#

locals {
  ecs_execution_role_arn = "arn:aws:iam::332779204498:role/ecs_execution_role"
  ledger_api_role_arn    = "arn:aws:iam::332779204498:role/ledger-cloud-api-role"
}

# ═══════════════════════════════════════════════════════════════
# Lightsail — Data Platform (Airflow + Unity Catalog)
# Always-on cloud instance running Docker Compose
# ═══════════════════════════════════════════════════════════════

resource "aws_lightsail_instance" "data_platform" {
  name              = "bellosdata-platform"
  availability_zone = "eu-west-2a"
  blueprint_id      = "amazon_linux_2023"
  bundle_id         = "medium_3_0" # 4 GB RAM, 2 vCPU, 80 GB, $24/mo
  key_pair_name     = "LightsailDefaultKeyPair"

  tags = {
    Project = "BellosData"
    Role    = "data-platform"
    Stack   = "airflow-glue-redshift"
  }

  user_data = <<-EOF
    #!/bin/bash
    set -e

    # ── System updates ──
    yum update -y
    yum install -y docker git

    # ── Docker setup ──
    systemctl enable docker
    systemctl start docker
    usermod -aG docker ec2-user

    # ── Docker Compose v2 ──
    mkdir -p /usr/local/lib/docker/cli-plugins
    curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" \
      -o /usr/local/lib/docker/cli-plugins/docker-compose
    chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

    # ── Clone repository ──
    cd /home/ec2-user
    git clone https://github.com/TimiOlayinka/bellosdata-lakehouse.git platform
    chown -R ec2-user:ec2-user platform

    # ── Create .env file (credentials injected via deploy script) ──
    cat > /home/ec2-user/platform/.env <<'ENVFILE'
    AWS_ACCESS_KEY_ID=REPLACE_ME
    AWS_SECRET_ACCESS_KEY=REPLACE_ME
    AWS_DEFAULT_REGION=eu-west-2
    ENVFILE

    echo "Bootstrap complete. Run deploy-platform.ps1 to configure and start services."
  EOF

  lifecycle {
    ignore_changes = [user_data]
  }
}

resource "aws_lightsail_static_ip" "data_platform_ip" {
  name = "bellosdata-platform-ip"
}

resource "aws_lightsail_static_ip_attachment" "data_platform_ip_attach" {
  static_ip_name = aws_lightsail_static_ip.data_platform_ip.name
  instance_name  = aws_lightsail_instance.data_platform.name
}

resource "aws_lightsail_instance_public_ports" "data_platform_ports" {
  instance_name = aws_lightsail_instance.data_platform.name

  port_info {
    protocol  = "tcp"
    from_port = 22
    to_port   = 22
  }

  port_info {
    protocol  = "tcp"
    from_port = 8081
    to_port   = 8081 # Airflow UI
  }
}

# ═══════════════════════════════════════════════════════════════
# ECR + ECS Fargate (Pipeline Containers — future use)
# ═══════════════════════════════════════════════════════════════

resource "aws_ecr_repository" "data_pipelines" {
  name                 = "bellosdata-pipelines"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecs_cluster" "bellosdata_cluster" {
  name = "bellosdata-cluster"
}

resource "aws_ecs_task_definition" "pipeline_task" {
  family                   = "bellosdata-pipeline-task"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = local.ecs_execution_role_arn

  container_definitions = jsonencode([
    {
      name      = "pipeline-container"
      image     = aws_ecr_repository.data_pipelines.repository_url
      essential = true
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = "/ecs/bellosdata-pipeline"
          "awslogs-region"        = "eu-west-2"
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])
}

resource "aws_cloudwatch_log_group" "ecs_log_group" {
  name              = "/ecs/bellosdata-pipeline"
  retention_in_days = 7
}

# ═══════════════════════════════════════════════════════════════
# AWS Glue Data Catalog — Lakehouse Metadata
# Replaces Unity Catalog OSS for table governance
# ═══════════════════════════════════════════════════════════════

resource "aws_glue_catalog_database" "bellosdata" {
  name        = "bellosdata"
  description = "BellosData Lakehouse — Delta Lake tables on S3 (Bronze + Silver + Gold)"
}

# ═══════════════════════════════════════════════════════════════
# Redshift Serverless — Query Engine (compute-only, S3 native)
# Replaces DuckDB for production analytics
# Queries S3 Delta Lake directly — no Spectrum fees
# ═══════════════════════════════════════════════════════════════

variable "redshift_admin_password" {
  description = "Admin password for Redshift Serverless namespace"
  type        = string
  sensitive   = true
}

resource "aws_iam_role" "redshift_s3_glue" {
  name = "bellosdata-redshift-s3-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = {
    Project = "BellosData"
    Role    = "redshift-data-access"
  }
}

resource "aws_iam_role_policy_attachment" "redshift_s3_read" {
  role       = aws_iam_role.redshift_s3_glue.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

resource "aws_iam_role_policy_attachment" "redshift_glue_access" {
  role       = aws_iam_role.redshift_s3_glue.name
  policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"
}

resource "aws_redshiftserverless_namespace" "bellosdata" {
  namespace_name      = "bellosdata"
  db_name             = "bellosdata"
  admin_username      = "admin"
  admin_user_password = var.redshift_admin_password
  iam_roles           = [aws_iam_role.redshift_s3_glue.arn]

  tags = {
    Project = "BellosData"
    Role    = "query-engine"
  }
}

resource "aws_redshiftserverless_workgroup" "bellosdata" {
  workgroup_name = "bellosdata-workgroup"
  namespace_name = aws_redshiftserverless_namespace.bellosdata.namespace_name
  base_capacity  = 8 # Minimum RPUs — auto-pauses when idle

  tags = {
    Project = "BellosData"
    Role    = "query-engine"
  }
}

# ═══════════════════════════════════════════════════════════════
# Lambda Cloud API — The Light That Never Goes Out
# ═══════════════════════════════════════════════════════════════

resource "aws_cloudwatch_log_group" "ledger_api_logs" {
  name              = "/aws/lambda/ledger-cloud-api"
  retention_in_days = 7
}

resource "aws_apigatewayv2_api" "ledger_api" {
  name          = "ledger-cloud-api"
  protocol_type = "HTTP"
  description   = "Merchant Ledger Cloud API — The Light That Never Goes Out"
}

resource "aws_apigatewayv2_stage" "ledger_default" {
  api_id      = aws_apigatewayv2_api.ledger_api.id
  name        = "$default"
  auto_deploy = true
}

# ═══════════════════════════════════════════════════════════════
# Outputs
# ═══════════════════════════════════════════════════════════════

output "ecr_repository_url" {
  value       = aws_ecr_repository.data_pipelines.repository_url
  description = "ECR repository URL for pipeline containers"
}

output "ledger_api_url" {
  value       = aws_apigatewayv2_api.ledger_api.api_endpoint
  description = "Cloud API URL — The Light That Never Goes Out"
}

output "ledger_api_id" {
  value       = aws_apigatewayv2_api.ledger_api.id
  description = "API Gateway ID"
}

output "data_platform_ip" {
  value       = aws_lightsail_static_ip.data_platform_ip.ip_address
  description = "Data Platform static IP (Airflow)"
}

output "airflow_url" {
  value       = "http://${aws_lightsail_static_ip.data_platform_ip.ip_address}:8081"
  description = "Airflow UI URL"
}

output "glue_catalog_database" {
  value       = aws_glue_catalog_database.bellosdata.name
  description = "Glue Data Catalog database for lakehouse tables"
}

output "redshift_workgroup" {
  value       = aws_redshiftserverless_workgroup.bellosdata.workgroup_name
  description = "Redshift Serverless workgroup name"
}

output "redshift_endpoint" {
  value       = aws_redshiftserverless_workgroup.bellosdata.endpoint
  description = "Redshift Serverless endpoint for SQL connections"
}
