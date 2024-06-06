resource "aws_athena_database" "s3_access_logs_db" {
  name   = var.glue_db_name
  bucket = aws_ssm_parameter.athena_output_path.value
}

resource "aws_athena_data_catalog" "sparc_glue_catalog" {
  name        = var.sparc_glue_catalog
  description = "SPARC's Glue based Data Catalog"
  type        = "GLUE"

  parameters = {
    "catalog-id" = data.terraform_remote_state.platform_infrastructure.outputs.sparc_account_id
  }
}

resource "aws_athena_data_catalog" "rejoin_glue_catalog" {
  name        = var.rejoin_glue_catalog
  description = "RE-JOIN (and Precision)'s Glue based Data Catalog"
  type        = "GLUE"

  parameters = {
    "catalog-id" = data.terraform_remote_state.platform_infrastructure.outputs.rejoin_account_id
  }
}
  