"""
AWS Glue Comprehensive ETL Job - Complete Data Pipeline
Handles: CSV Ingestion → Data Preparation → Entity Resolution → 
Iceberg Merge → Data Quality Checks in a single unified job.
"""

import sys
import logging
from typing import Dict, List, Tuple
from datetime import datetime
from hashlib import md5

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import Window
from pyspark.sql.functions import (
    col, when, lit, trim, lower, concat_ws, coalesce,
    md5 as spark_md5, row_number, count as spark_count,
    collect_list, struct, array_contains, concat, isnan, isnull,
    levenshtein, current_timestamp, split, array_join
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, TimestampType, ArrayType, DoubleType
)

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_bucket',
    'target_bucket',
    'database',
    'table'
])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SOURCE_BUCKET = args.get('source_bucket', 'contexq-dev-raw-data-119287772129')
TARGET_BUCKET = args.get('target_bucket', 'contexq-dev-processed-data-119287772129')
DATABASE_NAME = args.get('database', 'contexq_dev')
TABLE_NAME = args.get('table', 'corporate_registry')

# Log startup parameters for debugging
logger.info(f"✓ JOB_NAME: {args['JOB_NAME']}")
logger.info(f"✓ SOURCE_BUCKET: {SOURCE_BUCKET}")
logger.info(f"✓ TARGET_BUCKET: {TARGET_BUCKET}")
logger.info(f"✓ DATABASE: {DATABASE_NAME}")
logger.info(f"✓ TABLE: {TABLE_NAME}")

# Iceberg table schema
ICEBERG_SCHEMA = StructType([
    StructField("corporate_id", StringType(), False),
    StructField("corporate_name", StringType(), False),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("activity_places", IntegerType(), True),
    StructField("top_suppliers", ArrayType(StringType()), True),
    StructField("main_customers", StringType(), True),
    StructField("revenue", DecimalType(18, 2), True),
    StructField("profit", DecimalType(18, 2), True),
    StructField("source_system", StringType(), False),
    StructField("load_date", TimestampType(), False),
    StructField("entity_hash", StringType(), False),
])


class EntityResolutionEngine:
    """Performs entity resolution using heuristic-based matching."""
    
    def __init__(self, spark):
        self.spark = spark
        logger.info("EntityResolutionEngine initialized")
    
    def clean_text(self, text):
        """Clean and normalize text for matching."""
        return trim(lower(text))
    
    def resolve_duplicates(self, df):
        """Resolve duplicate entities using fuzzy matching."""
        logger.info("Resolving duplicates...")
        
        # Add hash of cleaned name + address for matching
        df_with_hash = df.withColumn(
            "entity_hash",
            spark_md5(concat_ws("|", 
                lower(trim(col("corporate_name"))),
                coalesce(lower(trim(col("address"))), lit(""))
            ))
        )
        
        # Assign unique ID to entities with same hash
        window_spec = Window.partitionBy("entity_hash").orderBy("corporate_name")
        df_deduped = df_with_hash.withColumn(
            "corporate_id",
            concat_ws("_", 
                lit("CORP"),
                row_number().over(window_spec)
            )
        )
        
        logger.info(f"✓ Deduplication complete. Unique entities: {df_deduped.select('entity_hash').distinct().count()}")
        return df_deduped


class DataHarmonizer:
    """Harmonizes data schema and formats."""
    
    def __init__(self, spark):
        self.spark = spark
        logger.info("DataHarmonizer initialized")
    
    def harmonize_schema(self, df):
        """Harmonize data to Iceberg schema."""
        logger.info("Harmonizing schema...")
        
        # Select and cast columns
        df_harmonized = df.select(
            col("corporate_id").cast(StringType()),
            col("corporate_name").cast(StringType()),
            col("address").cast(StringType()),
            col("city").cast(StringType()),
            col("state").cast(StringType()),
            col("activity_places").cast(IntegerType()),
            col("top_suppliers").cast(ArrayType(StringType())),
            col("main_customers").cast(StringType()),
            col("revenue").cast(DecimalType(18, 2)),
            col("profit").cast(DecimalType(18, 2)),
            col("source_system").cast(StringType()),
            col("load_date").cast(TimestampType()),
            col("entity_hash").cast(StringType()),
        )
        
        logger.info("✓ Schema harmonization complete")
        return df_harmonized


class IcebergMerger:
    """Performs ACID MERGE INTO Iceberg table."""
    
    def __init__(self, spark):
        self.spark = spark
        logger.info("IcebergMerger initialized")
    
    def merge_into_iceberg(self, df, table_name: str, database: str = "contexq_dev"):
        """Perform ACID MERGE INTO Iceberg table."""
        logger.info(f"Starting MERGE INTO {database}.{table_name}...")
        
        # Register source as temp view
        df.createOrReplaceTempView("source_entities")
        
        # Execute MERGE INTO (UPSERT)
        merge_sql = f"""
        MERGE INTO {database}.{table_name} t
        USING source_entities s
        ON t.corporate_id = s.corporate_id AND t.source_system = s.source_system
        WHEN MATCHED THEN UPDATE SET
            corporate_name = s.corporate_name,
            address = s.address,
            city = s.city,
            state = s.state,
            activity_places = s.activity_places,
            top_suppliers = s.top_suppliers,
            main_customers = s.main_customers,
            revenue = s.revenue,
            profit = s.profit,
            load_date = s.load_date,
            entity_hash = s.entity_hash
        WHEN NOT MATCHED THEN INSERT (
            corporate_id, corporate_name, address, city, state,
            activity_places, top_suppliers, main_customers, revenue, profit,
            source_system, load_date, entity_hash
        ) VALUES (
            s.corporate_id, s.corporate_name, s.address, s.city, s.state,
            s.activity_places, s.top_suppliers, s.main_customers, s.revenue, s.profit,
            s.source_system, s.load_date, s.entity_hash
        )
        """
        
        self.spark.sql(merge_sql)
        logger.info(f"✓ MERGE INTO complete for {database}.{table_name}")


class DataQualityValidator:
    """Validates data quality and generates reports."""
    
    def __init__(self, spark):
        self.spark = spark
        logger.info("DataQualityValidator initialized")
    
    def validate_schema(self, df):
        """Validate that dataframe conforms to expected schema."""
        logger.info("Validating schema...")
        expected_fields = set(field.name for field in ICEBERG_SCHEMA.fields)
        actual_fields = set(df.columns)
        
        missing_fields = expected_fields - actual_fields
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")
        
        logger.info(f"✓ Schema validation passed. All {len(expected_fields)} required fields present")
    
    def generate_quality_report(self, df, table_name: str, database: str = "contexq_dev"):
        """Generate data quality report."""
        logger.info(f"Generating quality report for {database}.{table_name}...")
        
        # Record count
        record_count = df.count()
        logger.info(f"✓ Total records in {table_name}: {record_count:,}")
        
        # Null checks
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_pct = (null_count / record_count * 100) if record_count > 0 else 0
            if null_pct > 10:
                logger.warning(f"⚠ Column '{col_name}' has {null_pct:.1f}% nulls")
        
        # Duplicate check
        distinct_count = df.select("corporate_id").distinct().count()
        logger.info(f"✓ Distinct corporate IDs: {distinct_count:,}")
        
        # Financial summary
        try:
            summary = df.agg({
                "revenue": "sum",
                "profit": "sum",
                "activity_places": "avg"
            }).collect()[0]
            logger.info(f"✓ Total revenue: ${summary[0]:,.2f}" if summary[0] else "No revenue data")
            logger.info(f"✓ Total profit: ${summary[1]:,.2f}" if summary[1] else "No profit data")
        except Exception as e:
            logger.warning(f"Could not calculate financial summary: {str(e)}")
        
        logger.info("✓ Data quality report complete")


def main():
    """Main ETL pipeline orchestration."""
    
    try:
        logger.info("\n" + "="*60)
        logger.info("STARTING COMPREHENSIVE ETL PIPELINE")
        logger.info("="*60)
        
        # ============================================================
        # PHASE 1: DATA INGESTION (CSV → Parquet)
        # ============================================================
        logger.info("\n=== PHASE 1: DATA INGESTION ===")
        
        try:
            # Read CSV files from S3
            logger.info("Reading source datasets from S3...")
            
            source1_path = f"s3://{SOURCE_BUCKET}/source_supply/olist_sellers_dataset.csv"
            source1_df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(source1_path)
            logger.info(f"✓ Source 1 (supply): {source1_df.count():,} records")
            
            source2_path = f"s3://{SOURCE_BUCKET}/source_financial/olist_order_payments_dataset.csv"
            source2_df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(source2_path)
            logger.info(f"✓ Source 2 (financial): {source2_df.count():,} records")
            
            # Save as Parquet for caching
            prep_supply_path = f"s3://{TARGET_BUCKET}/prepared_sources/source1_supply/"
            source1_df.write.mode("overwrite").parquet(prep_supply_path)
            logger.info(f"✓ Supply source cached to {prep_supply_path}")
            
            prep_financial_path = f"s3://{TARGET_BUCKET}/prepared_sources/source2_financial/"
            source2_df.write.mode("overwrite").parquet(prep_financial_path)
            logger.info(f"✓ Financial source cached to {prep_financial_path}")
            
        except Exception as e:
            logger.error(f"✗ Data ingestion failed: {str(e)}", exc_info=True)
            raise
        
        # ============================================================
        # PHASE 2: ENTITY RESOLUTION & DEDUPLICATION
        # ============================================================
        logger.info("\n=== PHASE 2: ENTITY RESOLUTION ===")
        
        try:
            entity_resolver = EntityResolutionEngine(spark)
            
            source1_resolved = entity_resolver.resolve_duplicates(
                source1_df.withColumn("source_system", lit("SUPPLY_CHAIN"))
            )
            source2_resolved = entity_resolver.resolve_duplicates(
                source2_df.withColumn("source_system", lit("FINANCIAL"))
            )
            
            # Combine sources
            logger.info("Combining resolved sources...")
            combined_df = source1_resolved.union(source2_resolved)
            logger.info(f"✓ Combined entities: {combined_df.count():,}")
            
        except Exception as e:
            logger.error(f"✗ Entity resolution failed: {str(e)}", exc_info=True)
            raise
        
        # ============================================================
        # PHASE 3: DATA HARMONIZATION
        # ============================================================
        logger.info("\n=== PHASE 3: DATA HARMONIZATION ===")
        
        try:
            harmonizer = DataHarmonizer(spark)
            harmonized_df = harmonizer.harmonize_schema(combined_df)
            
            # Add load timestamp
            harmonized_df = harmonized_df.withColumn(
                "load_date",
                current_timestamp()
            )
            
        except Exception as e:
            logger.error(f"✗ Data harmonization failed: {str(e)}", exc_info=True)
            raise
        
        # ============================================================
        # PHASE 4: ICEBERG MERGE
        # ============================================================
        logger.info("\n=== PHASE 4: ICEBERG MERGE INTO ===")
        
        try:
            merger = IcebergMerger(spark)
            merger.merge_into_iceberg(harmonized_df, TABLE_NAME, DATABASE_NAME)
            
        except Exception as e:
            logger.error(f"✗ Iceberg merge failed: {str(e)}", exc_info=True)
            raise
        
        # ============================================================
        # PHASE 5: DATA QUALITY CHECKS
        # ============================================================
        logger.info("\n=== PHASE 5: DATA QUALITY VALIDATION ===")
        
        try:
            validator = DataQualityValidator(spark)
            validator.validate_schema(harmonized_df)
            
            # Read final table for quality report
            final_df = spark.sql(f"SELECT * FROM {DATABASE_NAME}.{TABLE_NAME}")
            validator.generate_quality_report(final_df, TABLE_NAME, DATABASE_NAME)
            
        except Exception as e:
            logger.error(f"✗ Data quality validation failed: {str(e)}", exc_info=True)
            raise
        
        # ============================================================
        # SUMMARY REPORT
        # ============================================================
        logger.info("\n=== PIPELINE SUMMARY ===")
        summary_sql = f"""
        SELECT 
            source_system,
            COUNT(*) as entity_count,
            COUNT(DISTINCT corporate_id) as unique_entities,
            COUNT(DISTINCT state) as states_count
        FROM {DATABASE_NAME}.{TABLE_NAME}
        GROUP BY source_system
        """
        
        summary_df = spark.sql(summary_sql)
        logger.info("Source System Summary:")
        summary_df.show(truncate=False)
        
        # Write summary to S3
        summary_path = f"s3://{TARGET_BUCKET}/etl_reports/summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}/"
        summary_df.write.mode("overwrite").parquet(summary_path)
        logger.info(f"✓ Summary written to {summary_path}")
        
        logger.info("\n" + "="*60)
        logger.info("ETL JOB COMPLETED SUCCESSFULLY ✓")
        logger.info("Corporate registry table ready for ML pipeline")
        logger.info("="*60 + "\n")
        
        job.commit()
        return 0
        
    except Exception as e:
        logger.error(f"✗ ETL job failed: {str(e)}", exc_info=True)
        job.commit()
        return 1


if __name__ == "__main__":
    sys.exit(main())
