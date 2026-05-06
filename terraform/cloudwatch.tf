# CREATE EMBARGO RELEASE EVENT RULE
resource "aws_cloudwatch_event_rule" "embargo_release_event_rule" {
  name        = "${var.environment_name}-${var.service_name}-embargo-release-rule-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  description = "Notify Discover (via SQS) to scan for embargoed datasets to release"

  // Run every hour
  // See https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/ScheduledEvents.html#CronExpressions
  schedule_expression = "cron(0 * * * ? *)"
}

# CREATE EMBARGO RELEASE SQS EVENT TARGET
resource "aws_cloudwatch_event_target" "embargo_release_event_target" {
  rule      = aws_cloudwatch_event_rule.embargo_release_event_rule.name
  target_id = aws_cloudwatch_event_rule.embargo_release_event_rule.name
  arn       = data.terraform_remote_state.platform_infrastructure.outputs.discover_publish_queue_arn

  input = "{\"job_type\": \"SCAN_FOR_RELEASE\"}"
}

# Create filters and alarms for failure to enqueue publish-storage-sync messages
resource "aws_cloudwatch_log_metric_filter" "publish_storage_sync_enqueue_failed" {
  name           = "${var.environment_name}-${var.service_name}-publish-storage-sync-enqueue-failed"
  log_group_name = data.terraform_remote_state.ecs_cluster.outputs.cloudwatch_log_group_name
  pattern        = "{ ($.logLevel = \"ERROR\") && ($.message = \"*publish-storage-sync enqueue failed*\") }"

  metric_transformation {
    name          = "PublishStorageSyncEnqueueFailed"
    namespace     = local.publish_storage_sync_metric_namespace
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_log_metric_filter" "publish_storage_sync_ssm_read_failed" {
  name           = "${var.environment_name}-${var.service_name}-publish-storage-sync-ssm-read-failed"
  log_group_name = data.terraform_remote_state.ecs_cluster.outputs.cloudwatch_log_group_name
  pattern        = "{ ($.logLevel = \"ERROR\") && ($.message = \"*publish-storage-sync SSM read failed*\") }"

  metric_transformation {
    name          = "PublishStorageSyncSsmReadFailed"
    namespace     = local.publish_storage_sync_metric_namespace
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "publish_storage_sync_enqueue_failed" {
  alarm_name          = "${var.environment_name}-${var.service_name}-publish-storage-sync-enqueue-failed"
  alarm_description   = "Publish-storage-sync SQS enqueue from discover service failed; publish chain still completed but publish-storage-sync did not run for the affected dataset(s)."
  namespace           = local.publish_storage_sync_metric_namespace
  metric_name         = "PublishStorageSyncEnqueueFailed"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  # SNS topic currently routes to PagerDuty (victor_ops name is a historical artifact)
  alarm_actions = [data.terraform_remote_state.account.outputs.data_management_victor_ops_sns_topic_id]
}

resource "aws_cloudwatch_metric_alarm" "publish_storage_sync_ssm_read_failed" {
  alarm_name          = "${var.environment_name}-${var.service_name}-publish-storage-sync-ssm-read-failed"
  alarm_description   = "Discover service could not read the publish-storage-sync enabled SSM parameter; publish chain still completed but publish-storage-sync was treated as disabled."
  namespace           = local.publish_storage_sync_metric_namespace
  metric_name         = "PublishStorageSyncSsmReadFailed"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  # SNS topic currently routes to PagerDuty (victor_ops name is a historical artifact)
  alarm_actions = [data.terraform_remote_state.account.outputs.data_management_victor_ops_sns_topic_id]
}
