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

# Import ECS Cluster Data
data "terraform_remote_state" "ecs_cluster" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/${var.environment_name}/ecs-cluster/terraform.tfstate"
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

# Import Auth Service (Go). The discover/preview embargo authorization endpoint was
# rehomed from the Scala authorization-service into auth-service's auth-api Lambda
# (F1). authorization_service_host (ssm.tf) now sources auth_api_internal_host from
# here. The authorization_service remote_state above is retained so the host can be
# reverted to the Scala service in one line if the cutover needs rolling back.
data "terraform_remote_state" "auth_service" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/${var.environment_name}/auth-service/terraform.tfstate"
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

// AFS1 region
data "terraform_remote_state" "africa_south_region" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/af-south-1/terraform.tfstate"
    region = "us-east-1"
  }
}

# Import VPC Data
data "terraform_remote_state" "vpc" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/terraform.tfstate"
    region = "us-east-1"
  }
}

# Import Fargate Cluster Data
data "terraform_remote_state" "fargate" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/${var.environment_name}/fargate/terraform.tfstate"
    region = "us-east-1"
  }
}

# Import Publish Storage Sync Data
data "terraform_remote_state" "publish_storage_sync" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/${var.environment_name}/publish-storage-sync/terraform.tfstate"
    region = "us-east-1"
  }
}
