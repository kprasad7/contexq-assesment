"""Comprehensive AWS Glue ETL job (Iceberg) for OLIST sources.

This module serves two purposes:
1) Glue job entrypoint: ingest raw CSVs from S3, prepare Source 1 (supply chain)
   and Source 2 (financial), harmonize entities, then MERGE INTO an Iceberg table
   in AWS Glue Catalog.
2) Library of unit-tested helpers used by tests in tests/unit/test_etl_job.py.

Raw inputs (expected in the *raw* bucket):
  - source_supply/olist_order_items_dataset.csv
  - source_supply/olist_sellers_dataset.csv
  - source_financial/olist_order_payments_dataset.csv

Outputs (written to the *processed* bucket):
  - prepared_sources/source1_supply/ (parquet)
  - prepared_sources/source2_financial/ (parquet)
  - Iceberg warehouse under warehouse/<table_name>/
"""

from __future__ import annotations

import hashlib
import logging
import re
import sys
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


DATA_CONTRACT_VERSION = "1.0"


# -----------------------------
# Unit-tested helper functions
# -----------------------------
def cleanse_company_name(name: str | None) -> str:
	"""Lowercase + remove punctuation, leaving alphanumerics and spaces."""
	if not name:
		return ""
	cleaned = re.sub(r"[^a-z0-9\s]+", " ", name.lower())
	cleaned = re.sub(r"\s+", " ", cleaned).strip()
	return cleaned


def normalize_address(address: str | None) -> str:
	"""Lowercase + strip punctuation + normalize whitespace."""
	if not address:
		return ""
	cleaned = re.sub(r"[^a-z0-9\s]+", " ", address.lower())
	cleaned = re.sub(r"\s+", " ", cleaned).strip()
	return cleaned


def compute_corporate_id(canonical_name: str, canonical_address: str) -> str:
	"""Deterministic stable ID for an entity based on name+address."""
	raw = f"{canonical_name}|{canonical_address}".encode("utf-8")
	return hashlib.sha256(raw).hexdigest()[:16]


def prepare_supply_chain_dataframe(raw_sellers: DataFrame) -> DataFrame:
	"""Shape supply-chain source (S1) into a consistent schema.

	Expected input columns (see tests/unit/test_etl_job.py):
	  seller_id, seller_name, seller_city, seller_state, activity_places, top_suppliers
	"""
	return (
		raw_sellers.withColumn(
			"canonical_name",
			F.lower(F.trim(F.coalesce(F.col("seller_name"), F.lit("")))),
		)
		.withColumn("address", F.concat_ws(", ", F.col("seller_city"), F.col("seller_state")))
		.withColumnRenamed("seller_city", "city")
		.withColumnRenamed("seller_state", "state")
		.withColumn("source_system", F.lit("supply_chain"))
	)


def prepare_financial_dataframe(items: DataFrame, payments: DataFrame, sellers: DataFrame) -> DataFrame:
	"""Aggregate financial source (S2) metrics per seller.

	Unit tests expect:
	  revenue = sum(price + freight_value)
	  profit  = revenue * 0.18
	  transaction_count = count distinct order_id

	payments is currently unused for these metrics but included to match the
	real-world source shape.
	"""
	_ = payments  # included for interface consistency; metrics derive from order_items per tests

	per_seller = (
		items.groupBy("seller_id")
		.agg(
			F.sum(F.col("price") + F.col("freight_value")).cast("double").alias("revenue"),
			F.countDistinct("order_id").cast("int").alias("transaction_count"),
		)
		.withColumn("profit", (F.col("revenue") * F.lit(0.18)).cast("double"))
		.withColumn("source_system", F.lit("financial"))
	)

	sellers_dim = sellers.select("seller_id", "city", "state").dropDuplicates(["seller_id"])
	return per_seller.join(sellers_dim, on="seller_id", how="left")


def harmonize_sources(supply: DataFrame, financial: DataFrame) -> DataFrame:
	"""Harmonize supply + financial sources into a unified corporate registry.

	Strategy (simple heuristic):
	- Compute corporate_id for supply records from (canonical_name, address)
	- Attach that corporate_id to financial records using seller_id
	- Coalesce attributes + metrics per corporate_id
	"""
	supply_with_keys = (
		supply.withColumn("canonical_name_clean", F.udf(cleanse_company_name, "string")(F.col("canonical_name")))
		.withColumn("canonical_address", F.udf(normalize_address, "string")(F.col("address")))
		.withColumn(
			"corporate_id",
			F.udf(compute_corporate_id, "string")(F.col("canonical_name_clean"), F.col("canonical_address")),
		)
	)

	id_map = supply_with_keys.select(
		"seller_id",
		"corporate_id",
		F.col("canonical_name_clean").alias("corporate_name"),
		"address",
		"city",
		"state",
		"activity_places",
		"top_suppliers",
		"source_system",
	)

	financial_with_ids = (
		financial.join(id_map.select("seller_id", "corporate_id", "address", "city", "state"), on="seller_id", how="left")
		.withColumn("corporate_name", F.udf(cleanse_company_name, "string")(F.concat(F.lit("seller "), F.col("seller_id"))))
		.withColumn("source_system", F.lit("financial"))
		.select(
			"seller_id",
			"corporate_id",
			"corporate_name",
			"address",
			"city",
			"state",
			F.lit(None).cast("int").alias("activity_places"),
			F.lit(None).cast("string").alias("top_suppliers"),
			F.lit(None).cast("string").alias("main_customers"),
			F.col("revenue").cast("double").alias("revenue"),
			F.col("profit").cast("double").alias("profit"),
			F.col("transaction_count").cast("int").alias("transaction_count"),
			"source_system",
		)
	)

	supply_for_union = id_map.select(
		"seller_id",
		"corporate_id",
		"corporate_name",
		"address",
		"city",
		"state",
		F.col("activity_places").cast("int").alias("activity_places"),
		F.col("top_suppliers").cast("string").alias("top_suppliers"),
		F.lit(None).cast("string").alias("main_customers"),
		F.lit(None).cast("double").alias("revenue"),
		F.lit(None).cast("double").alias("profit"),
		F.lit(None).cast("int").alias("transaction_count"),
		F.lit("supply_chain").alias("source_system"),
	)

	unioned = supply_for_union.unionByName(financial_with_ids)

	return (
		unioned.groupBy("corporate_id")
		.agg(
			F.first("corporate_name", ignorenulls=True).alias("corporate_name"),
			F.first("address", ignorenulls=True).alias("address"),
			F.first("city", ignorenulls=True).alias("city"),
			F.first("state", ignorenulls=True).alias("state"),
			F.max("activity_places").alias("activity_places"),
			F.first("top_suppliers", ignorenulls=True).alias("top_suppliers"),
			F.first("main_customers", ignorenulls=True).alias("main_customers"),
			F.max("revenue").alias("revenue"),
			F.max("profit").alias("profit"),
			F.max("transaction_count").alias("transaction_count"),
			F.concat_ws(",", F.collect_set("source_system")).alias("source_system"),
		)
		.withColumn(
			"profit_margin",
			F.when(F.col("revenue") > 0, (F.col("profit") / F.col("revenue")).cast("double")).otherwise(F.lit(0.0)),
		)
	)


# -----------------------------
# Glue job helpers
# -----------------------------
def _get_spark_and_args() -> tuple[SparkSession, dict[str, Any], Any | None]:
	"""Return (spark, args, job) for Glue, or (spark, args, None) locally."""
	try:
		from awsglue.utils import getResolvedOptions  # type: ignore
		from pyspark.context import SparkContext
		from awsglue.context import GlueContext  # type: ignore
		from awsglue.job import Job  # type: ignore

		args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_bucket", "target_bucket", "database", "table"])
		sc = SparkContext.getOrCreate()
		glue_ctx = GlueContext(sc)
		spark = glue_ctx.spark_session
		job = Job(glue_ctx)
		job.init(args["JOB_NAME"], args)
		return spark, args, job
	except Exception:
		import argparse

		parser = argparse.ArgumentParser()
		parser.add_argument("--source_bucket", required=True)
		parser.add_argument("--target_bucket", required=True)
		parser.add_argument("--database", required=True)
		parser.add_argument("--table", required=True)
		ns = parser.parse_args()

		spark = (
			SparkSession.builder.appName("comprehensive-etl-local")
			.master("local[*]")
			.config("spark.sql.shuffle.partitions", "8")
			.config("spark.ui.enabled", "false")
			.getOrCreate()
		)
		return (
			spark,
			{"source_bucket": ns.source_bucket, "target_bucket": ns.target_bucket, "database": ns.database, "table": ns.table},
			None,
		)


def _configure_iceberg_glue_catalog(spark: SparkSession, warehouse_s3: str) -> None:
	"""Configure Spark session for Iceberg using AWS Glue Catalog.
	
	Note: Static configs (spark.sql.extensions, catalog-impl) are set via Glue job --conf parameters.
	Only dynamic runtime configs are set here.
	"""
	# Static configs already set via Glue job parameters:
	# - spark.sql.extensions
	# - spark.sql.catalog.glue_catalog
	# - spark.sql.catalog.glue_catalog.catalog-impl
	
	# Set runtime-configurable options
	spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
	spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", warehouse_s3)
	spark.conf.set("spark.sql.defaultCatalog", "glue_catalog")


def _read_olist_csv_sources(spark: SparkSession, source_bucket: str) -> tuple[DataFrame, DataFrame, DataFrame]:
	items_path = f"s3://{source_bucket}/source_supply/olist_order_items_dataset.csv"
	sellers_path = f"s3://{source_bucket}/source_supply/olist_sellers_dataset.csv"
	payments_path = f"s3://{source_bucket}/source_financial/olist_order_payments_dataset.csv"

	logger.info("Reading OLIST sources from raw bucket: %s", source_bucket)

	items = spark.read.option("header", "true").option("inferSchema", "true").csv(items_path)
	sellers = spark.read.option("header", "true").option("inferSchema", "true").csv(sellers_path)
	payments = spark.read.option("header", "true").option("inferSchema", "true").csv(payments_path)
	return items, sellers, payments


def _build_supply_from_olist(items: DataFrame, sellers: DataFrame) -> DataFrame:
	# OLIST has no seller name, so synthesize one for canonical_name behavior.
	seller_dim = sellers.select("seller_id", "seller_city", "seller_state")
	top_products = (
		items.groupBy("seller_id")
		.agg(
			F.countDistinct("product_id").cast("int").alias("activity_places"),
			F.slice(F.sort_array(F.collect_set("product_id")), 1, 10).alias("top_products"),
		)
		.withColumn("top_suppliers", F.concat_ws(",", F.col("top_products")))
		.drop("top_products")
	)

	raw_sellers = (
		seller_dim.join(top_products, on="seller_id", how="left")
		.withColumn("seller_name", F.concat(F.lit("Seller_"), F.col("seller_id")))
		.select(
			"seller_id",
			"seller_name",
			"seller_city",
			"seller_state",
			"activity_places",
			"top_suppliers",
		)
	)
	return prepare_supply_chain_dataframe(raw_sellers)


def _merge_into_iceberg(spark: SparkSession, harmonized: DataFrame, database: str, table: str) -> None:
	now = datetime.now(timezone.utc)
	target = f"glue_catalog.{database}.{table}"

	staging = (
		harmonized.select(
			"corporate_id",
			"corporate_name",
			"address",
			"city",
			"state",
			F.col("activity_places").cast("int").alias("activity_places"),
			F.col("top_suppliers").cast("string").alias("top_suppliers"),
			F.col("main_customers").cast("string").alias("main_customers"),
			F.col("revenue").cast("decimal(18,2)").alias("revenue"),
			F.col("profit").cast("decimal(18,2)").alias("profit"),
			F.col("source_system").cast("string").alias("source_system"),
		)
		.withColumn("load_date", F.current_timestamp())
		.withColumn("_etl_processed_dttm", F.current_timestamp())
		.withColumn("_data_contract_version", F.lit(DATA_CONTRACT_VERSION))
		.withColumn("year", F.lit(now.year).cast("int"))
		.withColumn("month", F.lit(now.month).cast("int"))
	)

	staging.createOrReplaceTempView("staging_corporate_registry")
	spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{database}")

	merge_sql = f"""
	MERGE INTO {target} t
	USING staging_corporate_registry s
	  ON t.corporate_id = s.corporate_id
	WHEN MATCHED THEN UPDATE SET
	  t.corporate_name = COALESCE(s.corporate_name, t.corporate_name),
	  t.address        = COALESCE(s.address, t.address),
	  t.city           = COALESCE(s.city, t.city),
	  t.state          = COALESCE(s.state, t.state),
	  t.activity_places= COALESCE(s.activity_places, t.activity_places),
	  t.top_suppliers  = COALESCE(s.top_suppliers, t.top_suppliers),
	  t.main_customers = COALESCE(s.main_customers, t.main_customers),
	  t.revenue        = COALESCE(s.revenue, t.revenue),
	  t.profit         = COALESCE(s.profit, t.profit),
	  t.load_date      = COALESCE(s.load_date, t.load_date),
	  t.source_system  = COALESCE(s.source_system, t.source_system),
	  t._etl_processed_dttm    = s._etl_processed_dttm,
	  t._data_contract_version = s._data_contract_version,
	  t.year = s.year,
	  t.month = s.month
	WHEN NOT MATCHED THEN INSERT (
	  corporate_id, corporate_name, address, city, state, activity_places,
	  top_suppliers, main_customers, revenue, profit, load_date, source_system,
	  _etl_processed_dttm, _data_contract_version, year, month
	) VALUES (
	  s.corporate_id, s.corporate_name, s.address, s.city, s.state, s.activity_places,
	  s.top_suppliers, s.main_customers, s.revenue, s.profit, s.load_date, s.source_system,
	  s._etl_processed_dttm, s._data_contract_version, s.year, s.month
	)
	"""

	logger.info("Running Iceberg MERGE into %s", target)
	spark.sql(merge_sql)


def main() -> int:
	spark, args, job = _get_spark_and_args()

	source_bucket = args["source_bucket"]
	target_bucket = args["target_bucket"]
	database = args["database"]
	table = args["table"]

	warehouse = f"s3://{target_bucket}/warehouse"
	_configure_iceberg_glue_catalog(spark, warehouse)

	items, sellers, payments = _read_olist_csv_sources(spark, source_bucket)

	supply_prepared = _build_supply_from_olist(items, sellers)
	financial_prepared = prepare_financial_dataframe(
		items.select("order_id", "seller_id", "price", "freight_value"),
		payments.select("order_id", "payment_value"),
		supply_prepared,
	)

	# Persist prepared sources for inspection/reuse.
	prepared_s1_out = f"s3://{target_bucket}/prepared_sources/source1_supply/"
	prepared_s2_out = f"s3://{target_bucket}/prepared_sources/source2_financial/"
	logger.info("Writing prepared sources to processed bucket")
	supply_prepared.write.mode("overwrite").parquet(prepared_s1_out)
	financial_prepared.write.mode("overwrite").parquet(prepared_s2_out)

	harmonized = harmonize_sources(supply_prepared, financial_prepared)
	_merge_into_iceberg(spark, harmonized, database, table)

	if job is not None:
		job.commit()
	logger.info("✓ Comprehensive ETL finished")
	return 0


if __name__ == "__main__":
	raise SystemExit(main())

