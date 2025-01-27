data "aws_caller_identity" "current" {}

data "aws_region" "current_region" {}

# Import Account Data
data "terraform_remote_state" "account" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/terraform.tfstate"
    region = "us-east-1"
  }
}

# Import Region Data
data "terraform_remote_state" "region" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/${data.aws_region.current_region.name}/terraform.tfstate"
    region = "us-east-1"
  }
}

# # Import API Data
# data "terraform_remote_state" "api" {
#   backend = "s3"

#   config = {
#     bucket = "${var.aws_account}-terraform-state"
#     key    = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/${var.environment_name}/api/terraform.tfstate"
#     region = "us-east-1"
#   }
# }

# Import Authorization Service
data "terraform_remote_state" "authorization_service" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/${var.environment_name}/authorization-service/terraform.tfstate"
    region = "us-east-1"
  }
}


# Import Discover Publish Data
data "terraform_remote_state" "discover_publish" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/${var.environment_name}/discover-publish/terraform.tfstate"
    region = "us-east-1"
  }
}

# Import Discover Release Data
data "terraform_remote_state" "discover_release" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/${var.environment_name}/discover-release/terraform.tfstate"
    region = "us-east-1"
  }
}


# Import DOI Service Data
data "terraform_remote_state" "doi_service" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/${var.environment_name}/doi-service/terraform.tfstate"
    region = "us-east-1"
  }
}

# Import Platform Infrastructure Data
data "terraform_remote_state" "platform_infrastructure" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/${var.environment_name}/platform-infrastructure/terraform.tfstate"
    region = "us-east-1"
  }
}

# Import S3Clean Lambda Data
data "terraform_remote_state" "discover_s3clean_lambda" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/${var.environment_name}/discover-s3clean-lambda/terraform.tfstate"
    region = "us-east-1"
  }
}

# Import ElasticSearch Data
data "terraform_remote_state" "discover_elasticsearch" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/${var.environment_name}/discover-elasticsearch/terraform.tfstate"
    region = "us-east-1"
  }
}

data "terraform_remote_state" "africa_south_region" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/af-south-1/terraform.tfstate"
    region = "af-south-1"
  }
}
