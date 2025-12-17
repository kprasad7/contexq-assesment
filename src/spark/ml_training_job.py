"""AWS Glue ML training job.

Reads the Iceberg corporate registry table from AWS Glue Catalog and trains a Spark ML model.
Logs metrics and registers the model with MLflow.

This file is intentionally written to be:
- runnable in AWS Glue (with awsglue libraries available)
- importable locally (basic syntax checks) without requiring awsglue
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from typing import Any

import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import StandardScaler, StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def _get_spark_and_args() -> tuple[SparkSession, dict[str, Any], Any | None]:
    """Return (spark, args, job) for Glue, or (spark, args, None) locally."""
    try:
        from awsglue.context import GlueContext  # type: ignore
        from awsglue.job import Job  # type: ignore
        from awsglue.utils import getResolvedOptions  # type: ignore
        from pyspark.context import SparkContext

        args = getResolvedOptions(
            sys.argv,
            [
                "JOB_NAME",
                "mlflow_tracking_uri",
                "experiment_name",
                "database",
                "table",
            ],
        )

        sc = SparkContext.getOrCreate()
        glue_ctx = GlueContext(sc)
        spark = glue_ctx.spark_session
        job = Job(glue_ctx)
        job.init(args["JOB_NAME"], args)
        return spark, args, job
    except Exception:
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--mlflow_tracking_uri", default="http://localhost:5000")
        parser.add_argument("--experiment_name", default="olist-profit-prediction")
        parser.add_argument("--database", required=True)
        parser.add_argument("--table", required=True)
        ns = parser.parse_args()

        spark = (
            SparkSession.builder.appName("ml-training-local")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        return (
            spark,
            {
                "mlflow_tracking_uri": ns.mlflow_tracking_uri,
                "experiment_name": ns.experiment_name,
                "database": ns.database,
                "table": ns.table,
            },
            None,
        )


class ProfitPredictionModel:
    """ML pipeline for profit prediction."""

    def __init__(
        self,
        spark: SparkSession,
        mlflow_tracking_uri: str,
        database: str,
        table: str,
        experiment_name: str,
    ):
        self.spark = spark
        self.database = database
        self.table = table
        self.experiment_name = experiment_name
        mlflow.set_tracking_uri(mlflow_tracking_uri)
        logger.info("MLflow tracking URI: %s", mlflow_tracking_uri)
    
    def load_training_data(self):
        """Load corporate_registry from Iceberg."""
        logger.info("Loading corporate_registry from Iceberg...")

        # Configure Iceberg + Glue Catalog (works in Glue 4.0 when Iceberg is enabled)
        self.spark.conf.set(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        self.spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        self.spark.conf.set(
            "spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        self.spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        self.spark.conf.set("spark.sql.defaultCatalog", "glue_catalog")

        df = self.spark.sql(
            f"""
            SELECT
                corporate_id,
                CAST(revenue AS DOUBLE) AS revenue,
                CAST(profit AS DOUBLE) AS profit,
                CAST(activity_places AS INT) AS activity_places,
                COALESCE(top_suppliers, '') AS top_suppliers,
                COALESCE(source_system, '') AS source_system
            FROM glue_catalog.{self.database}.{self.table}
            WHERE revenue IS NOT NULL AND profit IS NOT NULL
            """
        )

        # Label (simple business outcome): profit above a fixed threshold
        df = df.withColumn(
            "high_profit_label",
            when(col("profit") > lit(100000.0), lit(1)).otherwise(lit(0)).cast("int"),
        )
        
        logger.info("✓ Loaded %,d training records", df.count())
        return df
    
    def prepare_features(self, df):
        """Feature engineering and preparation."""
        logger.info("Preparing features...")

        # Derived numeric features
        df_features = (
            df.withColumn(
                "profit_margin",
                when(col("revenue") > 0, (col("profit") / col("revenue"))).otherwise(lit(0.0)).cast("double"),
            )
            .withColumn(
                "top_suppliers_count",
                when(
                    (F.length(F.trim(col("top_suppliers"))) == 0),
                    lit(0),
                )
                .otherwise(F.size(F.split(col("top_suppliers"), ",")))
                .cast("int"),
            )
        )

        logger.info("✓ Features prepared")
        return df_features
    
    def build_pipeline(self):
        """Build ML pipeline."""
        logger.info("Building ML pipeline...")

        indexer = StringIndexer(
            inputCol="source_system",
            outputCol="source_system_idx",
            handleInvalid="keep",
        )
        
        # Feature vector assembly
        vector_assembler = VectorAssembler(
            inputCols=[
                "revenue",
                "profit_margin",
                "activity_places",
                "top_suppliers_count",
                "source_system_idx",
            ],
            outputCol="features"
        )
        
        # Feature scaling
        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features",
            withMean=True,
            withStd=True
        )
        
        # Logistic Regression classifier
        lr = LogisticRegression(
            featuresCol="scaled_features",
            labelCol="high_profit_label",
            maxIter=100,
            regParam=0.01,
            elasticNetParam=0.5,
            family="binomial"
        )
        
        # Build pipeline
        pipeline = Pipeline(stages=[indexer, vector_assembler, scaler, lr])
        logger.info("✓ Pipeline built")
        return pipeline
    
    def train_and_evaluate(self, df_indexed):
        """Train model and evaluate."""
        logger.info("Training and evaluating model...")
        
        # Train/test split (80/20)
        df_train, df_test = df_indexed.randomSplit([0.8, 0.2], seed=42)
        logger.info(f"✓ Train records: {df_train.count():,}, Test records: {df_test.count():,}")
        
        # Build and fit pipeline
        pipeline = self.build_pipeline()
        model = pipeline.fit(df_train)
        
        # Make predictions
        predictions = model.transform(df_test)
        
        # Evaluate with BinaryClassificationEvaluator
        bc_evaluator = BinaryClassificationEvaluator(
            labelCol="high_profit_label",
            metricName="areaUnderROC"
        )
        auc = bc_evaluator.evaluate(predictions)
        
        # Evaluate with MulticlassClassificationEvaluator
        mc_evaluator = MulticlassClassificationEvaluator(
            labelCol="high_profit_label",
            predictionCol="prediction",
            metricName="f1"
        )
        f1 = mc_evaluator.evaluate(predictions)
        
        logger.info(f"✓ Model Metrics:")
        logger.info(f"  - AUC: {auc:.4f}")
        logger.info(f"  - F1-Score: {f1:.4f}")
        
        return model, {"auc": auc, "f1": f1, "timestamp": datetime.now().isoformat()}
    
    def log_model_to_mlflow(self, model, metrics: dict):
        """Log model and metrics to MLflow."""
        logger.info("Logging model to MLflow...")
        
        mlflow.set_experiment(self.experiment_name)
        
        with mlflow.start_run(run_name=f"profit-prediction-{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
            # Log metrics
            mlflow.log_metric("auc", metrics["auc"])
            mlflow.log_metric("f1", metrics["f1"])
            
            # Log model
            mlflow.spark.log_model(
                model,
                artifact_path="profit-prediction-model",
                registered_model_name="profit-prediction-v1"
            )
            
            # Log params
            mlflow.log_param("train_test_split", "0.8/0.2")
            mlflow.log_param("regParam", 0.01)
            mlflow.log_param("elasticNetParam", 0.5)
            mlflow.log_param("maxIter", 100)
            
            logger.info("✓ Model logged to MLflow")
            return mlflow.active_run().info.run_id


def main():
    """Main ML training orchestration."""
    logger.info("Starting ML training job: Profit Prediction Model")

    spark, args, job = _get_spark_and_args()

    mlflow_tracking_uri = args.get("mlflow_tracking_uri", "http://localhost:5000")
    experiment_name = args.get("experiment_name", "olist-profit-prediction")
    database = args.get("database", "contexq_dev")
    table = args.get("table", "corporate_registry")

    try:
        # Initialize model
        model_trainer = ProfitPredictionModel(
            spark,
            mlflow_tracking_uri=mlflow_tracking_uri,
            database=database,
            table=table,
            experiment_name=experiment_name,
        )
        
        # Load training data
        df = model_trainer.load_training_data()
        
        # Display statistics
        logger.info("\n=== DATA STATISTICS ===")
        stats_sql = """
        SELECT
            COUNT(*) as total_records,
            SUM(CASE WHEN high_profit_label = 1 THEN 1 ELSE 0 END) as high_profit_count,
            SUM(CASE WHEN high_profit_label = 0 THEN 1 ELSE 0 END) as low_profit_count,
            ROUND(AVG(CAST(revenue AS DOUBLE)), 2) as avg_revenue,
            ROUND(AVG(CAST(profit AS DOUBLE)), 2) as avg_profit,
            ROUND(STDDEV(CAST(revenue AS DOUBLE)), 2) as stddev_revenue
        FROM training_data
        """
        df.createOrReplaceTempView("training_data")
        spark.sql(stats_sql).show(truncate=False)
        
        # Prepare features
        df_indexed = model_trainer.prepare_features(df)
        
        # Train and evaluate
        model, metrics = model_trainer.train_and_evaluate(df_indexed)
        
        # Log to MLflow
        run_id = model_trainer.log_model_to_mlflow(model, metrics)
        
        logger.info("Model training completed successfully")
        logger.info("MLflow Run ID: %s", run_id)
        logger.info("Experiment: %s", experiment_name)
        logger.info("Model Name: profit-prediction-v1")

        if job is not None:
            job.commit()
        return 0

    except Exception as e:
        logger.error("✗ ML training job failed: %s", str(e), exc_info=True)
        if job is not None:
            job.commit()
        return 1


if __name__ == "__main__":
    sys.exit(main())
