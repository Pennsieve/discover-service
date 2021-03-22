# # Creats Discover Route53 ALIAS
# resource "aws_route53_record" "assets_route53_record" {
#   zone_id = local.hosted_zone
#   name    = "assets.${local.service}"
#   type    = "A"

#   alias {
#     name                   = aws_cloudfront_distribution.cloudfront_distribution.domain_name
#     zone_id                = aws_cloudfront_distribution.cloudfront_distribution.hosted_zone_id
#     evaluate_target_health = true
#   }
# }

# # Create ACM Certificate Record
# resource "aws_route53_record" "certificate_validation_record" {
#   name    = tolist(aws_acm_certificate.certificate.domain_validation_options)[0].resource_record_name
#   type    = tolist(aws_acm_certificate.certificate.domain_validation_options)[0].resource_record_type
#   zone_id = local.hosted_zone
#   records = [tolist(aws_acm_certificate.certificate.domain_validation_options)[0].resource_record_value]
#   ttl     = 60
# }
