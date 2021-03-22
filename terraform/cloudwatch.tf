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
