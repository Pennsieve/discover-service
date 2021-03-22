data "aws_region" "current" {}

# IMPORT ACCOUNT DATA
data "terraform_remote_state" "account" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/terraform.tfstate"
    region = "us-east-1"
  }
}

# IMPORT REGION DATA
data "terraform_remote_state" "region" {
  backend = "s3"

  config = {
    bucket  = "${var.aws_account}-terraform-state"
    key     = "aws/${data.aws_region.current.name}/terraform.tfstate"
    region  = "us-east-1"
    profile = var.aws_account
  }
}
