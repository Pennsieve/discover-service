# # Create Discover Cloudfront Origin Access Identity
# resource "aws_cloudfront_origin_access_identity" "cloudfront_origin_access_identity" {
#   comment = "${var.environment_name}-discover-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
# }

# # Create Discover Cloudfront Distribution
# resource "aws_cloudfront_distribution" "cloudfront_distribution" {
#   aliases             = ["assets.${local.service}.${local.domain_name}"]
#   comment             = data.terraform_remote_state.platform_infrastructure.outputs.discover_s3_bucket_id
#   default_root_object = "index.html"
#   enabled             = true

#   is_ipv6_enabled = var.discover_is_ipv6_enabled
#   price_class     = "PriceClass_All"

#   origin {
#     domain_name = data.terraform_remote_state.platform_infrastructure.outputs.discover_bucket_domain_name
#     origin_id   = "${data.terraform_remote_state.platform_infrastructure.outputs.discover_s3_bucket_id}S3Origin"

#     s3_origin_config {
#       origin_access_identity = aws_cloudfront_origin_access_identity.cloudfront_origin_access_identity.cloudfront_access_identity_path
#     }
#   }

#   logging_config {
#     include_cookies = false
#     bucket          = data.terraform_remote_state.region.outputs.logs_s3_bucket_domain_name
#     prefix          = "${var.environment_name}/discover/cloudfront/"
#   }

#   default_cache_behavior {
#     allowed_methods        = ["GET", "HEAD", "OPTIONS"]
#     cached_methods         = ["GET", "HEAD"]
#     default_ttl            = 3600
#     max_ttl                = 86400
#     min_ttl                = 0
#     smooth_streaming       = "false"
#     target_origin_id       = "${data.terraform_remote_state.platform_infrastructure.outputs.discover_s3_bucket_id}S3Origin"
#     viewer_protocol_policy = "redirect-to-https"

#     forwarded_values {
#       query_string = false

#       cookies {
#         forward = "none"
#       }

#       # CORS headers
#       headers = [
#         "Access-Control-Request-Headers",
#         "Access-Control-Request-Method",
#         "Origin",
#       ]
#     }
#   }

#   restrictions {
#     geo_restriction {
#       restriction_type = "none"
#     }
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

#   viewer_certificate {
#     acm_certificate_arn      = aws_acm_certificate.certificate.arn
#     minimum_protocol_version = "TLSv1.2_2018"
#     ssl_support_method       = "sni-only"
#   }

#   custom_error_response {
#     error_caching_min_ttl = "300"
#     error_code            = "403"
#     response_code         = "200"
#     response_page_path    = "/index.html"
#   }

#   custom_error_response {
#     error_caching_min_ttl = "300"
#     error_code            = "404"
#     response_code         = "200"
#     response_page_path    = "/index.html"
#   }
# }
