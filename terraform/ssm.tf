// DATABASE (POSTGRES) CONFIGURATION

resource "aws_ssm_parameter" "discover_postgres_host" {
  name = "/${var.environment_name}/${var.service_name}/discover-postgres-host"
  type = "String"

  # share postgres instance with model-schema service in dev
  value = var.discover_postgres_host
}

resource "aws_ssm_parameter" "discover_postgres_user" {
  name  = "/${var.environment_name}/${var.service_name}/discover-postgres-user"
  type  = "String"
  value = "${var.environment_name}_${replace(var.service_name, "-", "_")}_user"
}

resource "aws_ssm_parameter" "discover_postgres_password" {
  name      = "/${var.environment_name}/${var.service_name}/discover-postgres-password"
  overwrite = false
  type      = "SecureString"
  value     = "dummy"

  lifecycle {
    ignore_changes = [value]
  }
}

resource "aws_ssm_parameter" "discover_postgres_num_connections" {
  name  = "/${var.environment_name}/${var.service_name}/discover-postgres-num-connections"
  type  = "String"
  value = "10"
}

resource "aws_ssm_parameter" "discover_postgres_queue_size" {
  name  = "/${var.environment_name}/${var.service_name}/discover-postgres-queue-size"
  type  = "String"
  value = "1000"
}

// JWT CONFIGURATION

resource "aws_ssm_parameter" "discover_jwt_secret_key" {
  name      = "/${var.environment_name}/${var.service_name}/discover-jwt-secret-key"
  overwrite = false
  type      = "SecureString"
  value     = "dummy"

  lifecycle {
    ignore_changes = [value]
  }
}

// DOI CLIENT CONFIGURATION

resource "aws_ssm_parameter" "doi_service_host" {
  name  = "/${var.environment_name}/${var.service_name}/doi-service-host"
  type  = "String"
  value = "https://${data.terraform_remote_state.doi_service.outputs.internal_fqdn}"
}

// NEW RELIC CONFIGURATION

resource "aws_ssm_parameter" "java_opts" {
  name  = "/${var.environment_name}/${var.service_name}/java-opts"
  type  = "String"
  value = join(" ", local.java_opts)
}

resource "aws_ssm_parameter" "new_relic_app_name" {
  name  = "/${var.environment_name}/${var.service_name}/new-relic-app-name"
  type  = "String"
  value = "${var.environment_name}-${var.service_name}"
}

resource "aws_ssm_parameter" "new_relic_labels" {
  name  = "/${var.environment_name}/${var.service_name}/new-relic-labels"
  type  = "String"
  value = "Environment:${var.environment_name};Service:${local.service};Tier:${local.tier}"
}

resource "aws_ssm_parameter" "new_relic_license_key" {
  name      = "/${var.environment_name}/${var.service_name}/new-relic-license-key"
  overwrite = false
  type      = "SecureString"
  value     = "dummy"

  lifecycle {
    ignore_changes = [value]
  }
}

// S3 CONFIGURATION
resource "aws_ssm_parameter" "s3_publish_bucket" {
  name  = "/${var.environment_name}/${var.service_name}/s3-publish-bucket"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.discover_publish_bucket_id
}

resource "aws_ssm_parameter" "s3_embargo_bucket" {
  name  = "/${var.environment_name}/${var.service_name}/s3-embargo-bucket"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.discover_embargo_bucket_id
}

resource "aws_ssm_parameter" "s3_frontend_bucket" {
  name  = "/${var.environment_name}/${var.service_name}/s3-frontend-bucket"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.discover_publish_bucket_id
}

resource "aws_ssm_parameter" "s3_assets_key_prefix" {
  name  = "/${var.environment_name}/${var.service_name}/s3-assets-key-prefix"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.discover_bucket_dataset_assets_key_prefix
}

resource "aws_ssm_parameter" "s3_publish_bucket_logs" {
  name  = "/${var.environment_name}/${var.service_name}/s3-publish-bucket-logs"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.discover_publish_logs_s3_bucket_id
}

resource "aws_ssm_parameter" "s3_access_logs_path" {
  name  = "/${var.environment_name}/${var.service_name}/s3-access-logs-path"
  type  = "String"
  value = "${var.environment_name}/discover-publish/s3"
}

resource "aws_ssm_parameter" "athena_output_path" {
  name  = "/${var.environment_name}/${var.service_name}/athena-output-path"
  type  = "String"
  value = "${aws_ssm_parameter.s3_publish_bucket_logs.value}/${var.environment_name}/discover-publish/athena-query-results"
}

resource "aws_ssm_parameter" "s3_athena_output_location" {
  name  = "/${var.environment_name}/${var.service_name}/s3-athena-output-location"
  type  = "String"
  value = "s3://${aws_ssm_parameter.athena_output_path.value}"
}

// STEP FUNCTIONS CONFIGURATION

resource "aws_ssm_parameter" "publish_state_machine_arn" {
  name  = "/${var.environment_name}/${var.service_name}/publish-state-machine-arn"
  type  = "String"
  value = data.terraform_remote_state.discover_publish.outputs.state_machine_id
}

resource "aws_ssm_parameter" "release_state_machine_arn" {
  name  = "/${var.environment_name}/${var.service_name}/release-state-machine-arn"
  type  = "String"
  value = data.terraform_remote_state.discover_release.outputs.state_machine_id
}

resource "aws_ssm_parameter" "region" {
  name  = "/${var.environment_name}/${var.service_name}/region"
  type  = "String"
  value = data.aws_region.current_region.name
}

// SQS CONFIGURATION

resource "aws_ssm_parameter" "queue_url" {
  name  = "/${var.environment_name}/${var.service_name}/queue-url"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.discover_publish_queue_id
}

// PENNSIEVE API CONFIGURATION

resource "aws_ssm_parameter" "api_host" {
  name = "/${var.environment_name}/${var.service_name}/api-host"
  type = "String"

  # API depends on discover service data, preventing cyclical dependency by using naming standard
  value = "https://${var.environment_name}-api-${data.terraform_remote_state.region.outputs.aws_region_shortname}.${data.terraform_remote_state.account.outputs.domain_name}"
}


// AUTHORIZATION SERVICE CONFIGURATION

resource "aws_ssm_parameter" "authorization_service_host" {
  name = "/${var.environment_name}/${var.service_name}/authorization-service-host"
  type = "String"

  value = "https://${data.terraform_remote_state.authorization_service.outputs.internal_fqdn}"
}


// MAX DOWNLOAD SIZE CONFIGURATION

resource "aws_ssm_parameter" "download_max_size" {
  name  = "/${var.environment_name}/${var.service_name}/download-max-size"
  type  = "String"
  value = var.download_max_size
}

// PUBLIC URL

resource "aws_ssm_parameter" "discover_public_url" {
  name  = "/${var.environment_name}/${var.service_name}/discover-public-url"
  type  = "String"
  value = "https://${local.service}.${local.domain_name}"
}

// ASSETS URL

resource "aws_ssm_parameter" "discover_assets_url" {
  name  = "/${var.environment_name}/${var.service_name}/discover-assets-url"
  type  = "String"
  value = "https://${data.terraform_remote_state.platform_infrastructure.outputs.discover_assets_route53_record}"
}

// LAMBDA CONFIGURATION

resource "aws_ssm_parameter" "discover_s3clean_lambda" {
  name  = "/${var.environment_name}/${var.service_name}/discover-s3clean-lambda"
  type  = "String"
  value = data.terraform_remote_state.discover_s3clean_lambda.outputs.lambda_function_arn
}

// ELASTICSEARCH CONFIGURATION

resource "aws_ssm_parameter" "discover_elasticsearch_host" {
  name  = "/${var.environment_name}/${var.service_name}/discover-elasticsearch-host"
  type  = "String"
  value = "https://${data.terraform_remote_state.discover_elasticsearch.outputs.domain_endpoint}:${var.discover_elasticsearch_port}?ssl=true"
}

resource "aws_ssm_parameter" "discover_elasticsearch_port" {
  name  = "/${var.environment_name}/${var.service_name}/discover-elasticsearch-port"
  type  = "String"
  value = var.discover_elasticsearch_port
}

// SNS CONFIGURATION

# resource "aws_ssm_parameter" "sns_alert_topic" {
#   name  = "/${var.environment_name}/${var.service_name}/alert-topic"
#   type  = "String"
#   value = data.terraform_remote_state.account.outputs.data_management_victor_ops_sns_topic_id
# }
