# Storage Cleanup Task
# The ECS task definition, IAM roles, and log group are defined in the publish-storage-sync repo.
# This repo references that task via terraform_remote_state.publish_storage_sync.

# Security Group for Storage Cleanup Fargate Task
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
