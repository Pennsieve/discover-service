resource "aws_athena_database" "s3_access_logs_db" {
  name   = "s3_access_logs_db"
  bucket = aws_ssm_parameter.athena_output_path.value
  }
  