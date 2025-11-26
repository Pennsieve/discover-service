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
  name = "/${var.environment_name}/${var.service_name}/java-opts"
  type = "String"
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

resource "aws_ssm_parameter" "s3_publish_50_bucket" {
  name  = "/${var.environment_name}/${var.service_name}/s3-publish-50-bucket"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.discover_publish50_bucket_id
}

resource "aws_ssm_parameter" "s3_embargo_50_bucket" {
  name  = "/${var.environment_name}/${var.service_name}/s3-embargo-50-bucket"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.discover_embargo50_bucket_id
}

resource "aws_ssm_parameter" "s3_frontend_bucket" {
  name  = "/${var.environment_name}/${var.service_name}/s3-frontend-bucket"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.discover_s3_bucket_id
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

// EXTERNAL PUBLISH BUCKETS CONFIGURATION
//    SPARC
resource "aws_ssm_parameter" "sparc_publish_bucket" {
  name  = "/${var.environment_name}/${var.service_name}/sparc-publish-bucket"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.sparc_publish_bucket_id
}

resource "aws_ssm_parameter" "sparc_embargo_bucket" {
  name  = "/${var.environment_name}/${var.service_name}/sparc-embargo-bucket"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.sparc_embargo_bucket_id
}

resource "aws_ssm_parameter" "sparc_publish_50_bucket" {
  name  = "/${var.environment_name}/${var.service_name}/sparc-publish-50-bucket"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.sparc_publish50_bucket_id
}

resource "aws_ssm_parameter" "sparc_embargo_50_bucket" {
  name  = "/${var.environment_name}/${var.service_name}/sparc-embargo-50-bucket"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.sparc_embargo50_bucket_id
}

resource "aws_ssm_parameter" "sparc_bucket_role_arn" {
  name  = "/${var.environment_name}/${var.service_name}/sparc-bucket-role-arn"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.sparc_bucket_role_arn
}

resource "aws_ssm_parameter" "awsod_sparc_publish_50_bucket" {
  name  = "/${var.environment_name}/${var.service_name}/awsod-sparc-publish-50-bucket"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.awsod_sparc_publish50_bucket_id
}

resource "aws_ssm_parameter" "awsod_sparc_bucket_role_arn" {
  name  = "/${var.environment_name}/${var.service_name}/awsod-sparc-bucket-role-arn"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.awsod_sparc_bucket_role_arn
}

resource "aws_ssm_parameter" "awsod_edots_publish_50_bucket" {
  name  = "/${var.environment_name}/${var.service_name}/awsod-edots-publish-50-bucket"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.awsod_edots_publish50_bucket_id
}

// Note: we use the "SPARC" Role here as the buckets are in the same AWS Open Data account
// Technically this parameter is probably not needed, but it is here in case this needs to change.
resource "aws_ssm_parameter" "awsod_edots_bucket_role_arn" {
  name  = "/${var.environment_name}/${var.service_name}/awsod-edots-bucket-role-arn"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.awsod_sparc_bucket_role_arn
}

//    RE-JOIN
resource "aws_ssm_parameter" "rejoin_publish_50_bucket" {
  name  = "/${var.environment_name}/${var.service_name}/rejoin-publish-50-bucket"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.rejoin_publish50_bucket_id
}

resource "aws_ssm_parameter" "rejoin_embargo_50_bucket" {
  name  = "/${var.environment_name}/${var.service_name}/rejoin-embargo-50-bucket"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.rejoin_embargo50_bucket_id
}

resource "aws_ssm_parameter" "rejoin_bucket_role_arn" {
  name  = "/${var.environment_name}/${var.service_name}/rejoin-bucket-role-arn"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.rejoin_bucket_role_arn
}
//    Precision
resource "aws_ssm_parameter" "precision_publish_50_bucket" {
  name  = "/${var.environment_name}/${var.service_name}/precision-publish-50-bucket"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.precision_publish50_bucket_id
}

resource "aws_ssm_parameter" "precision_embargo_50_bucket" {
  name  = "/${var.environment_name}/${var.service_name}/precision-embargo-50-bucket"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.precision_embargo50_bucket_id
}


// ATHENA CONFIGURATION
resource "aws_ssm_parameter" "pennsieve_bucket_access_glue_table" {
  name  = "/${var.environment_name}/${var.service_name}/pennsieve-bucket-access-glue-table"
  type  = "String"
  value = "${var.glue_db_name}.${aws_glue_catalog_table.glue_catalog_table_s3_logs.name}"
}

resource "aws_ssm_parameter" "sparc_bucket_access_glue_table" {
  name  = "/${var.environment_name}/${var.service_name}/sparc-bucket-access-glue-table"
  type  = "String"
  value = "${var.sparc_glue_catalog}.${data.terraform_remote_state.platform_infrastructure.outputs.sparc_s3_access_logs_glue_db}.${data.terraform_remote_state.platform_infrastructure.outputs.sparc_s3_access_logs_glue_table}"
}

resource "aws_ssm_parameter" "rejoin_bucket_access_glue_table" {
  name  = "/${var.environment_name}/${var.service_name}/rejoin-bucket-access-glue-table"
  type  = "String"
  value = "${var.rejoin_glue_catalog}.${data.terraform_remote_state.platform_infrastructure.outputs.rejoin_s3_access_logs_glue_db}.${data.terraform_remote_state.platform_infrastructure.outputs.rejoin_s3_access_logs_glue_table}"
}

resource "aws_ssm_parameter" "sparc_aod_bucket_access_glue_table" {
  name  = "/${var.environment_name}/${var.service_name}/sparc-aod-bucket-access-glue-table"
  type  = "String"
  value = "${var.sparc_aod_glue_catalog}.${local.sparc_aod.glue_db}.${local.sparc_aod.glue_table}"
}

// SNS CONFIGURATION

# resource "aws_ssm_parameter" "sns_alert_topic" {
#   name  = "/${var.environment_name}/${var.service_name}/alert-topic"
#   type  = "String"
#   value = data.terraform_remote_state.account.outputs.data_management_victor_ops_sns_topic_id
# }

// Runtime settings
resource "aws_ssm_parameter" "delete_release_intermediate_file" {
  name  = "/${var.environment_name}/${var.service_name}/delete-release-intermediate-file"
  type  = "String"
  value = "false"
}

// DOI Collection settings
resource "aws_ssm_parameter" "pennsieve_doi_prefix" {
  name  = "/${var.environment_name}/${var.service_name}/pennsieve-doi-prefix"
  type  = "String"
  value = local.pennsieve_doi_prefix
}

resource "aws_ssm_parameter" "doi_collections_id_space_id" {
  name  = "/${var.environment_name}/${var.service_name}/doi-collections-id-space-id"
  type  = "String"
  value = var.doi_collections_id_space_id
}

resource "aws_ssm_parameter" "doi_collections_id_space_name" {
  name  = "/${var.environment_name}/${var.service_name}/doi-collections-id-space-name"
  type  = "String"
  value = var.doi_collections_id_space_name
}
