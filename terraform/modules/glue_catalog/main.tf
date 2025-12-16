resource "aws_glue_catalog_database" "main" {
  name        = var.database_name
  description = "Glue Data Catalog database for corporate data in Iceberg format"
  catalog_id  = var.account_id

  tags = var.tags

  lifecycle {
    # Prevent accidental deletion of the database
    prevent_destroy = true
  }
}

resource "aws_glue_catalog_table" "corporate_registry" {
  name          = var.table_name
  database_name = aws_glue_catalog_database.main.name
  catalog_id    = var.account_id
  table_type    = "ICEBERG"
  description   = "Corporate registry with ACID transaction support via Apache Iceberg"

  parameters = {
    EXTERNAL_TABLE_NAME      = var.table_name
    table_type               = "ICEBERG"
    classification           = "iceberg"
    write_compression        = "snappy"
    read_compression         = "snappy"
    "iceberg.format.version" = "2"
  }

  storage_descriptor {
    location      = "s3://${var.processed_bucket}/warehouse/${var.table_name}/"
    input_format  = "org.apache.iceberg.mr.hive.HiveIcebergInputFormat"
    output_format = "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat"
    compressed    = true

    ser_de_info {
      serialization_library = "org.apache.iceberg.mr.hive.HiveIcebergSerde"

      parameters = {
        "serialization.format" = "parquet"
      }
    }

    columns {
      name    = "corporate_id"
      type    = "string"
      comment = "Unique corporate identifier (deduped)"
    }

    columns {
      name    = "corporate_name"
      type    = "string"
      comment = "Harmonized corporate name"
    }

    columns {
      name    = "address"
      type    = "string"
      comment = "Corporate headquarters address"
    }

    columns {
      name    = "city"
      type    = "string"
      comment = "City of headquarters"
    }

    columns {
      name    = "state"
      type    = "string"
      comment = "State or province code"
    }

    columns {
      name    = "activity_places"
      type    = "int"
      comment = "Number of business locations"
    }

    columns {
      name    = "top_suppliers"
      type    = "string"
      comment = "Comma-separated list of top suppliers"
    }

    columns {
      name    = "main_customers"
      type    = "string"
      comment = "Comma-separated list of main customers"
    }

    columns {
      name    = "revenue"
      type    = "decimal(18,2)"
      comment = "Annual revenue in base currency"
    }

    columns {
      name    = "profit"
      type    = "decimal(18,2)"
      comment = "Annual profit/loss in base currency"
    }

    columns {
      name    = "load_date"
      type    = "timestamp"
      comment = "Data load timestamp (UTC)"
    }

    columns {
      name    = "source_system"
      type    = "string"
      comment = "Source system identifier (S1 or S2)"
    }

    columns {
      name    = "_etl_processed_dttm"
      type    = "timestamp"
      comment = "ETL processing timestamp"
    }

    columns {
      name    = "_data_contract_version"
      type    = "string"
      comment = "Data contract version for validation"
    }
  }

  partition_keys {
    name    = "year"
    type    = "int"
    comment = "Partition key: year"
  }

  partition_keys {
    name    = "month"
    type    = "int"
    comment = "Partition key: month"
  }

  depends_on = [aws_glue_catalog_database.main]
}

data "aws_caller_identity" "current" {}
