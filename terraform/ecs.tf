# ECS Task Definition for Delete Task (invoked after successful dataset publish)

resource "aws_ecs_task_definition" "s3_storage_cleanup_task" {
  family                   = "${var.environment_name}-${var.service_name}-s3-storage-cleanup-task"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.ecs_s3_storage_cleanup_task_cpu
  memory                   = var.ecs_s3_storage_cleanup_task_memory
  execution_role_arn       = aws_iam_role.s3_storage_cleanup_task_execution_role.arn
  task_role_arn            = aws_iam_role.s3_storage_cleanup_task_role.arn

  container_definitions = jsonencode([{
    name      = var.ecs_s3_storage_cleanup_task_container_name
    image     = var.ecs_s3_storage_cleanup_task_image
    essential = true
    environment = [
      { name = "ENVIRONMENT", value = var.environment_name }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.s3_storage_cleanup_task.name
        "awslogs-region"        = data.aws_region.current_region.name
        "awslogs-stream-prefix" = "s3-storage-cleanup-task"
      }
    }
  }])

  tags = {
    Name        = "${var.environment_name}-${var.service_name}-s3-storage-cleanup-task"
    Environment = var.environment_name
    Service     = var.service_name
  }
}

# CloudWatch Log Group for Delete Task
resource "aws_cloudwatch_log_group" "s3_storage_cleanup_task" {
  name              = "/aws/ecs/${var.environment_name}-${var.service_name}-s3-storage-cleanup-task"
  retention_in_days = 30

  tags = {
    Name        = "${var.environment_name}-${var.service_name}-s3-storage-cleanup-task-logs"
    Environment = var.environment_name
    Service     = var.service_name
  }
}

# IAM Role for Task Execution (pulling images, writing logs)
resource "aws_iam_role" "s3_storage_cleanup_task_execution_role" {
  name = "${var.environment_name}-${var.service_name}-s3-storage-cleanup-task-exec-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })

  tags = {
    Name        = "${var.environment_name}-${var.service_name}-s3-storage-cleanup-task-exec-role"
    Environment = var.environment_name
    Service     = var.service_name
  }
}

resource "aws_iam_role_policy_attachment" "s3_storage_cleanup_task_execution_role_policy" {
  role       = aws_iam_role.s3_storage_cleanup_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# IAM Role for Task (permissions the container needs at runtime)
resource "aws_iam_role" "s3_storage_cleanup_task_role" {
  name = "${var.environment_name}-${var.service_name}-s3-storage-cleanup-task-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })

  tags = {
    Name        = "${var.environment_name}-${var.service_name}-s3-storage-cleanup-task-role"
    Environment = var.environment_name
    Service     = var.service_name
  }
}

# Permissions for the s3 storage cleanup task
resource "aws_iam_role_policy" "s3_storage_cleanup_task_policy" {
  name = "${var.environment_name}-${var.service_name}-s3-storage-cleanup-task-policy"
  role = aws_iam_role.s3_storage_cleanup_task_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ]
      Resource = [
        data.terraform_remote_state.platform_infrastructure.outputs.discover_publish_bucket_arn,
        "${data.terraform_remote_state.platform_infrastructure.outputs.discover_publish_bucket_arn}/*"
      ]
    }]
  })
}

# Security Group for Delete Fargate Task
resource "aws_security_group" "s3_storage_cleanup_fargate_task_security_group" {
  name        = "${var.environment_name}-s3-storage-cleanup-fargate-sg-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  description = "Security Group for ${var.environment_name}-s3-storage-cleanup-fargate-task-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  vpc_id      = data.terraform_remote_state.vpc.outputs.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    "Name"         = "${var.environment_name}-s3-storage-cleanup-fargate-task-sg-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
    "name"         = "${var.environment_name}-s3-storage-cleanup-fargate-task-sg-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
    "service_name" = var.service_name
    "Environment"  = var.environment_name
  }
}
