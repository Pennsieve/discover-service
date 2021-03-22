# # Create Assets Cert
# resource "aws_acm_certificate" "certificate" {
#   domain_name       = "assets.${local.service}.${local.domain_name}"
#   validation_method = "DNS"
# }

# # Create Certificate Validation
# resource "aws_acm_certificate_validation" "certificate_validation" {
#   certificate_arn = aws_acm_certificate.certificate.arn

#   validation_record_fqdns = [
#     aws_route53_record.certificate_validation_record.fqdn,
#   ]
# }
