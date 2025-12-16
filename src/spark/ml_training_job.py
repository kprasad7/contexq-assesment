"""
AWS Glue ML Training Job for Profit Prediction Model.
Reads corporate_registry Iceberg table and trains LR classifier.
Registers best model in MLflow.
"""

import sys
import logging
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql.functions import (
    col, when, lit, rand, round as spark_round,
    count as spark_count, avg, stddev
)

import mlflow
import mlflow.spark
from mlflow.models.signature import infer_signature

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'mlflow_tracking_uri',
    'experiment_name'
])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

MLFLOW_TRACKING_URI = args.get('mlflow_tracking_uri', 'http://localhost:5000')
EXPERIMENT_NAME = args.get('experiment_name', 'olist-profit-prediction')


class ProfitPredictionModel:
    """ML pipeline for profit prediction."""
    
    def __init__(self, spark, mlflow_tracking_uri: str):
        self.spark = spark
        mlflow.set_tracking_uri(mlflow_tracking_uri)
        logger.info(f"MLflow tracking URI: {mlflow_tracking_uri}")
    
    def load_training_data(self):
        """Load corporate_registry from Iceberg."""
        logger.info("Loading corporate_registry from Iceberg...")
        
        df = self.spark.sql("""
        SELECT
            CAST(revenue AS DOUBLE) as revenue,
            CAST(profit AS DOUBLE) as profit,
            activity_places,
            source_system,
            CASE 
                WHEN CAST(profit AS DOUBLE) > 100000 THEN 1
                ELSE 0 
            END as high_profit_label
        FROM contexq_dev.corporate_registry
        WHERE revenue IS NOT NULL AND profit IS NOT NULL
        """)
        
        logger.info(f"✓ Loaded {df.count():,} training records")
        return df
    
    def prepare_features(self, df):
        """Feature engineering and preparation."""
        logger.info("Preparing features...")
        
        # Calculate derived features
        df_features = df.withColumn(
            "profit_margin",
            when(col("revenue") > 0, spark_round(col("profit") / col("revenue"), 2)).otherwise(0)
        ).withColumn(
            "revenue_normalized",
            spark_round((col("revenue") - col("revenue").mean().over()) / col("revenue").stddev().over(), 2)
        )
        
        # Source system encoding
        indexer = StringIndexer(inputCol="source_system", outputCol="source_system_idx")
        df_indexed = indexer.fit(df_features).transform(df_features)
        
        logger.info("✓ Features prepared")
        return df_indexed
    
    def build_pipeline(self):
        """Build ML pipeline."""
        logger.info("Building ML pipeline...")
        
        # Feature vector assembly
        vector_assembler = VectorAssembler(
            inputCols=["revenue", "profit_margin", "activity_places", "source_system_idx"],
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
        pipeline = Pipeline(stages=[vector_assembler, scaler, lr])
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
        
        mlflow.set_experiment(EXPERIMENT_NAME)
        
        with mlflow.start_run(run_name=f"profit-prediction-{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
            # Log metrics
            mlflow.log_metric("auc", metrics['auc'])
            mlflow.log_metric("f1", metrics['f1'])
            
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
    
    try:
        # Initialize model
        model_trainer = ProfitPredictionModel(spark, MLFLOW_TRACKING_URI)
        
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
        logger.info(f"MLflow Run ID: {run_id}")
        logger.info(f"Experiment: {EXPERIMENT_NAME}")
        logger.info("Model Name: profit-prediction-v1")
        
        job.commit()
        return 0
        
    except Exception as e:
        logger.error(f"✗ ML training job failed: {str(e)}", exc_info=True)
        job.commit()
        return 1


if __name__ == "__main__":
    sys.exit(main())
