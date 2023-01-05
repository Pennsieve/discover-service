resource "aws_athena_database" "s3_access_logs_db" {
  name   = "s3_access_logs_db"
  bucket = aws_ssm_parameter.athena_output_path.value
  }

  # AWS Glue Table
resource "aws_glue_catalog_table" "prod_discover_s3_logs" {
  name          = "prod_discover_s3_logs"
  database_name = "s3_access_logs_db"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://pennsieve-prod-discover-publish-logs-use1/prod/discover-publish/s3"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "my-stream"
      serialization_library = "org.apache.hadoop.hive.serde2.RegexSerDe"

      parameters = {
        "serialization.format" = 1
      }
    }

    columns {
      name = "bucketowner"
      type = "string"
    }

    columns {
      name = "bucket_name"
      type = "string"
    }

    columns {
      name    = "requestdatetime"
      type    = "string"
    }

    columns {
      name    = "remoteip"
      type    = "string"
    }

    columns {
      name    = "requester"
      type    = "string"
    }

    columns {
      name    = "requestid"
      type    = "string"
    }

    columns {
      name    = "operation"
      type    = "string"
    }

    columns {
      name    = "key"
      type    = "string"
    }

    columns {
      name    = "request_uri"
      type    = "string"
    }

    columns {
      name    = "httpstatus"
      type    = "string"
    }

    columns {
      name    = "errorcode"
      type    = "string"
    }

    columns {
      name    = "bytessent"
      type    = "string"
    }

    columns {
      name    = "objectsize"
      type    = "string"
    }

    columns {
      name    = "totaltime"
      type    = "string"
    }

    columns {
      name    = "turnaroundtime"
      type    = "string"
    }

    columns {
      name    = "referrer"
      type    = "string"
    }

    columns {
      name    = "useragent"
      type    = "string"
    }

    columns {
      name    = "hostid"
      type    = "string"
    }

    columns {
      name    = "sigv"
      type    = "string"
    }

    columns {
      name    = "ciphersuite"
      type    = "string"
    }

    columns {
      name    = "authtype"
      type    = "string"
    }

    columns {
      name    = "endpoint"
      type    = "string"
    }

    columns {
      name    = "tlsversion"
      type    = "string"
    }
  }
}