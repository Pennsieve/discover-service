resource "aws_iam_policy" "iam_policy" {
  name   = "${var.environment_name}-${var.service_name}-policy-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  path   = "/service/"
  policy = data.aws_iam_policy_document.iam_policy_document.json
}

resource "aws_iam_role_policy_attachment" "iam_role_policy_attachment" {
  role       = var.ecs_task_iam_role_id
  policy_arn = aws_iam_policy.iam_policy.arn
}

data "aws_iam_policy_document" "iam_policy_document" {
  statement {
    sid       = "KMSDecryptPermissions"
    effect    = "Allow"
    actions   = ["kms:Decrypt"]
    resources = [
      "arn:aws:kms:${data.aws_region.current_region.name}:${data.aws_caller_identity.current.account_id}:key/alias/aws/ssm"
    ]
  }

  statement {
    sid       = "SecretsManagerListPermissions"
    effect    = "Allow"
    actions   = ["secretsmanager:ListSecrets"]
    resources = ["*"]
  }

  statement {
    sid    = "SSMPermissions"
    effect = "Allow"

    actions = [
      "ssm:GetParameter",
      "ssm:GetParameters",
      "ssm:GetParametersByPath",
    ]

    resources = [
      "arn:aws:ssm:${data.aws_region.current_region.name}:${data.aws_caller_identity.current.account_id}:parameter/${var.environment_name}/${var.service_name}/*"
    ]
  }

  statement {
    sid    = "StartStateMachine"
    effect = "Allow"

    actions = [
      "states:StartExecution",
    ]

    resources = [
      data.terraform_remote_state.discover_publish.outputs.state_machine_id,
      data.terraform_remote_state.discover_release.outputs.state_machine_id,
    ]
  }

  statement {
    sid    = "GlueGetTable"
    effect = "Allow"

    actions = [
      "glue:GetTable",
      "glue:GetCatalogImportStatus"
    ]

    resources = [
      "arn:aws:glue:${data.aws_region.current_region.name}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${data.aws_region.current_region.name}:${data.aws_caller_identity.current.account_id}:database/${aws_athena_database.s3_access_logs_db.name}",
      "arn:aws:glue:${data.aws_region.current_region.name}:${data.aws_caller_identity.current.account_id}:table/${aws_athena_database.s3_access_logs_db.name}/*",
    ]
  }

  statement {
    sid    = "AthenaQueryExecution"
    effect = "Allow"

    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResultsStream"
    ]

    resources = [
      "arn:aws:athena:${data.aws_region.current_region.name}:${data.aws_caller_identity.current.account_id}:workgroup/primary"
    ]
  }

  statement {
    sid    = "SQSReceiveMessages"
    effect = "Allow"

    actions = [
      "sqs:DeleteMessage",
      "sqs:ReceiveMessage",
    ]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.discover_publish_queue_arn,
    ]
  }

  statement {
    sid    = "KMSDecryptMessages"
    effect = "Allow"

    actions = [
      "kms:Decrypt",
      "kms:GenerateDataKey",
    ]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.discover_publish_kms_key_arn,
    ]
  }

  statement {
    sid    = "AthenaResultBucket"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:PutObject",
      "s3:GetBucketLocation",
      "s3:CreateBucket",
      "s3:ListBucketMultipartUploads",
      "s3:ListMultipartUploadParts",
      "s3:AbortMultipartUpload",
    ]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.discover_publish_logs_s3_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.discover_publish_logs_s3_bucket_arn}/*",
    ]
  }

  statement {
    sid    = "S3PublishBucket"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:PutObject",
      "s3:ListBucketMultipartUploads",
      "s3:ListMultipartUploadParts",
      "s3:AbortMultipartUpload",
    ]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.discover_publish_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.discover_publish_bucket_arn}/*",
      data.terraform_remote_state.platform_infrastructure.outputs.discover_embargo_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.discover_embargo_bucket_arn}/*",
      data.terraform_remote_state.platform_infrastructure.outputs.sparc_publish_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.sparc_publish_bucket_arn}/*",
      data.terraform_remote_state.platform_infrastructure.outputs.sparc_embargo_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.sparc_embargo_bucket_arn}/*",
    ]
  }

  statement {
    sid    = "S3FrontendBucket"
    effect = "Allow"

    actions = [
      "s3:PutObject",
    ]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.discover_s3_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.discover_s3_bucket_arn}/*",
    ]
  }

  statement {
    sid    = "InvokeLambda"
    effect = "Allow"

    actions = [
      "lambda:InvokeFunction",
    ]

    resources = [
      data.terraform_remote_state.discover_s3clean_lambda.outputs.lambda_function_arn,
    ]
  }

  statement {
    sid       = "AssumeSPARCPublishBucketRole"
    effect    = "Allow"
    actions   = ["sts:AssumeRole"]
    resources = [data.terraform_remote_state.platform_infrastructure.outputs.sparc_bucket_role_arn]
  }

  statement {
    sid       = "AllowAccessToSPARCS3AccessLogTable"
    effect    = "Allow"
    actions   = ["glue:*"]
    resources = [
      "arn:aws:glue:${data.aws_region.current_region.name}:${data.terraform_remote_state.platform_infrastructure.outputs.sparc_account_id}:catalog",
      "arn:aws:glue:${data.aws_region.current_region.name}:${data.terraform_remote_state.platform_infrastructure.outputs.sparc_account_id}:database/${data.terraform_remote_state.platform_infrastructure.outputs.sparc_s3_access_logs_glue_db}",
      "arn:aws:glue:${data.aws_region.current_region.name}:${data.terraform_remote_state.platform_infrastructure.outputs.sparc_account_id}:table/${data.terraform_remote_state.platform_infrastructure.outputs.sparc_s3_access_logs_glue_db}/${data.terraform_remote_state.platform_infrastructure.outputs.sparc_s3_access_logs_glue_table}"
    ]
  }

  statement {
    sid       = "AllowAccessToSPARCS3AccessLogBucket"
    effect    = "Allow"
    actions   = ["s3:*"]
    resources = [
      "arn:aws:s3:::${data.terraform_remote_state.platform_infrastructure.outputs.sparc_s3_access_logs_bucket}",
      "arn:aws:s3:::${data.terraform_remote_state.platform_infrastructure.outputs.sparc_s3_access_logs_bucket}/${data.terraform_remote_state.platform_infrastructure.outputs.sparc_s3_access_logs_prefix}*"
    ]
  }

  # statement {
  #   sid    = "PublishToVictorOps"
  #   effect = "Allow"

  #   actions = [
  #     "sns:Publish",
  #   ]

  #   resources = [
  #     data.terraform_remote_state.account.outputs.data_management_victor_ops_sns_topic_arn,
  #   ]
  # }
}