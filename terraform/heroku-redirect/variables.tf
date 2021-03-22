variable "aws_account" {}

variable "environment_name" {}

variable "service_name" {}

variable "vpc_name" {}

variable "compress" {
  default = false
}

# Using 1.1 due to Heroku limitations
variable "http_version" {
  type    = string
  default = "http1.1"
}

variable "is_pennsieve_org" {
  default = false
}

variable "is_ipv6_enabled" {
  default = false
}

variable "minimum_protocol_version" {
  type    = string
  default = "TLSv1.2_2018"
}

variable "origin_domain_name" {
  type = string
}

variable "origin_id" {
  type = string
}

locals {
  acm_certificate_arn   = data.terraform_remote_state.region.outputs.wildcard_cert_arn
  domain_name           = data.terraform_remote_state.account.outputs.domain_name
  public_hosted_zone_id = data.terraform_remote_state.account.outputs.public_hosted_zone_id

  common_tags = {
    aws_account      = var.aws_account
    aws_region       = data.aws_region.current.name
    environment_name = var.environment_name
    service_name     = var.service_name
  }
}
