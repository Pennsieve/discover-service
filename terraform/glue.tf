# AWS Glue Table
resource "aws_glue_catalog_table" "${var.environment_name}_${var.service_name}_s3_logs" {
  name          = "${var.environment_name}_${var.service_name}_s3_logs"
  database_name = "${var.glue_db_name}"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "${var.s3_glue_location}"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "hadoop_serilization_library"
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
