resource "aws_route53_record" "public_route53_record" {
  zone_id = local.public_hosted_zone_id
  name    = var.service_name
  type    = "A"

  alias {
    name                   = aws_cloudfront_distribution.cloudfront_distribution.domain_name
    zone_id                = aws_cloudfront_distribution.cloudfront_distribution.hosted_zone_id
    evaluate_target_health = true
  }
}

# CREATE PRIVATE ROUTE53 RECORD
resource "aws_route53_record" "private_route53_record" {
  count   = var.is_pennsieve_org == false ? 1 : 0
  zone_id = data.terraform_remote_state.account.outputs.private_hosted_zone_id
  name    = var.service_name
  type    = "A"

  alias {
    name                   = aws_cloudfront_distribution.cloudfront_distribution.domain_name
    zone_id                = aws_cloudfront_distribution.cloudfront_distribution.hosted_zone_id
    evaluate_target_health = true
  }
}

# # Create TXT record for Google Analytics in Production
# resource "aws_route53_record" "google_site_verification_route53_record" {
#   # Only create this txt record for discover.blackfynn.com
#   count   = var.is_pennsieve_org ? 1 : 0
#   name    = var.service_name
#   ttl     = 300
#   type    = "TXT"
#   zone_id = local.public_hosted_zone_id

#   records = [
#     "google-site-verification=_h-BprQwz3px7V1WUnbKp55Pgl6tQ17Efkhgka3ZDok",
#   ]
# }
