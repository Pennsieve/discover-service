output "cloudfront_arn" {
  value = aws_cloudfront_distribution.cloudfront_distribution.arn
}

output "public_route53_record" {
  value = aws_route53_record.public_route53_record.fqdn
}
