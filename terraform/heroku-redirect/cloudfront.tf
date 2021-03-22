# CREATE CLOUDFRONT DISTRIBUTION
resource "aws_cloudfront_distribution" "cloudfront_distribution" {
  aliases = ["${var.service_name}.${local.domain_name}"]

  comment         = "HTTPS redirect to Heroku"
  enabled         = true
  http_version    = var.http_version
  is_ipv6_enabled = var.is_ipv6_enabled
  price_class     = "PriceClass_All"

  default_cache_behavior {
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    default_ttl            = 86400
    max_ttl                = 31536000
    min_ttl                = 0
    target_origin_id       = var.origin_id
    viewer_protocol_policy = "redirect-to-https"
    compress               = var.compress

    forwarded_values {
      headers      = ["*"]
      query_string = true

      cookies {
        forward = "all"
      }
    }
  }

  logging_config {
    include_cookies = false
    bucket          = data.terraform_remote_state.region.outputs.logs_s3_bucket_domain_name
    prefix          = "${var.environment_name}/${var.service_name}/cloudfront/"
  }

  origin {
    domain_name = var.origin_domain_name
    origin_id   = var.origin_id

    custom_origin_config {
      http_port                = 80
      https_port               = 443
      origin_protocol_policy   = "match-viewer"
      origin_ssl_protocols     = ["TLSv1", "TLSv1.1", "TLSv1.2"]
      origin_keepalive_timeout = 5
      origin_read_timeout      = 30
    }
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  tags = merge(
    local.common_tags,
      map(
        "Name", "${var.environment_name}-${var.service_name}-cloudfront-distribution-${data.terraform_remote_state.region.outputs.aws_region_shortname}",
        "name", "${var.environment_name}-${var.service_name}-cloudfront-distribution-${data.terraform_remote_state.region.outputs.aws_region_shortname}",
        "tier", "cloudfront-distribution",
      )
  )

  viewer_certificate {
    acm_certificate_arn      = local.acm_certificate_arn
    minimum_protocol_version = var.minimum_protocol_version
    ssl_support_method       = "sni-only"
  }
}
