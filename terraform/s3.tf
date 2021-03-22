# # Create Discover S3 Bucket
# resource "aws_s3_bucket" "s3_bucket" {
#   bucket = "${var.environment_name}-discover-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
#   policy = data.aws_iam_policy_document.discover_iam_policy_document.json

#   lifecycle {
#     prevent_destroy = "true"
#   }

#   logging {
#     target_bucket = data.terraform_remote_state.region.outputs.logs_s3_bucket_id
#     target_prefix = "${var.environment_name}/discover/s3/"
#   }

#   tags = merge(
#     local.common_tags,
#     map(
#       "Name", "${var.environment_name}-discover-s3-bucket-${data.terraform_remote_state.region.outputs.aws_region_shortname}",
#       "name", "${var.environment_name}-discover-s3-bucket-${data.terraform_remote_state.region.outputs.aws_region_shortname}",
#       "service_name", "discover",
#       "tier", "s3",
#     )
#   )

#   website {
#     index_document = "index.html"
#     error_document = "404.html"
#   }

#   cors_rule {
#     allowed_headers = ["*"]
#     allowed_methods = ["GET", "HEAD"]
#     allowed_origins = ["*"]
#     max_age_seconds = 3000
#   }

#   server_side_encryption_configuration {
#     rule {
#       apply_server_side_encryption_by_default {
#         sse_algorithm = "AES256"
#       }
#     }
#   }
# }
