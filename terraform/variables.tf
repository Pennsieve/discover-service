variable "aws_account" {}

variable "environment_name" {}

variable "service_name" {}

variable "vpc_name" {}

variable "ecs_task_iam_role_id" {}

variable "discover_postgres_host" {}

variable "discover_bucket_dataset_assets_key_prefix" {
  default = "dataset-assets"
}

variable "discover_elasticsearch_port" {
  default = 443
}

variable "download_max_size" {
  default = "10 GB"
}

# New Relic
variable "newrelic_agent_enabled" {
  default = "true"
}

locals {
  java_opts = [
    "-javaagent:/app/newrelic.jar",
    "-Dnewrelic.config.agent_enabled=${var.newrelic_agent_enabled}",
    "-XX:+UseContainerSupport",
    "-XshowSettings:vm",
  ]

  service = element(split("-", var.service_name), 0)
  tier    = element(split("-", var.service_name), 1)

  hosted_zone = var.environment_name == "prod" ? data.terraform_remote_state.account.outputs.pennsieve_org_route53_zone_id : data.terraform_remote_state.account.outputs.public_hosted_zone_id
  domain_name = data.terraform_remote_state.account.outputs.domain_name

  common_tags = {
    aws_account      = var.aws_account
    aws_region       = data.aws_region.current_region.name
    environment_name = var.environment_name
    service_name     = var.service_name
  }
}
