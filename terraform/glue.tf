# AWS Glue Table
resource "aws_glue_catalog_database" "glue_catalog_database_s3_logs" {
  name = var.glue_db_name
}

resource "aws_glue_catalog_table" "glue_catalog_table_s3_logs" {
  name          = "discover"
  database_name = aws_glue_catalog_database.glue_catalog_database_s3_logs.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${data.terraform_remote_state.platform_infrastructure.outputs.discover_publish_logs_s3_bucket_id}/${var.environment_name}/discover-publish/s3"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "hadoop_serialization_library"
      serialization_library = "org.apache.hadoop.hive.serde2.RegexSerDe"

      parameters = {
        "serialization.format" = 1
        "input.regex"          = "([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*))?.*$"

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
      name = "requestdatetime"
      type = "string"
    }

    columns {
      name = "remoteip"
      type = "string"
    }

    columns {
      name = "requester"
      type = "string"
    }

    columns {
      name = "requestid"
      type = "string"
    }

    columns {
      name = "operation"
      type = "string"
    }

    columns {
      name = "key"
      type = "string"
    }

    columns {
      name = "request_uri"
      type = "string"
    }

    columns {
      name = "httpstatus"
      type = "string"
    }

    columns {
      name = "errorcode"
      type = "string"
    }

    columns {
      name = "bytessent"
      type = "bigint"
    }

    columns {
      name = "objectsize"
      type = "bigint"
    }

    columns {
      name = "totaltime"
      type = "string"
    }

    columns {
      name = "turnaroundtime"
      type = "string"
    }

    columns {
      name = "referrer"
      type = "string"
    }

    columns {
      name = "useragent"
      type = "string"
    }

    columns {
      name = "versionid"
      type = "string"
    }

    columns {
      name = "hostid"
      type = "string"
    }

    columns {
      name = "sigv"
      type = "string"
    }

    columns {
      name = "ciphersuite"
      type = "string"
    }

    columns {
      name = "authtype"
      type = "string"
    }

    columns {
      name = "endpoint"
      type = "string"
    }

    columns {
      name = "tlsversion"
      type = "string"
    }
  }
}

resource "aws_athena_data_catalog" "sparc_glue_catalog" {
  name        = var.sparc_glue_catalog
  description = "SPARC's Glue based Data Catalog"
  type        = "GLUE"

  parameters = {
    "catalog-id" = data.terraform_remote_state.platform_infrastructure.outputs.sparc_account_id
  }
}
