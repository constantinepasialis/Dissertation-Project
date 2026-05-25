import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from google.cloud import storage
from Logger import Logger
from DBConnector import DBConnector

# Spark Session Initialization
spark = SparkSession.builder.appName("Classification_Model_Creation").getOrCreate()

# Read config file
client = storage.Client()
bucket = client.bucket("dataops-assets")
blob = bucket.blob("config/config.ini")

config = configparser.ConfigParser()
config.read_string(blob.download_as_text())

# Define paths and URI
INPUT_PATH = config.get("buckets", "silver")
CHAMPION_PATH = config.get("buckets", "model")
URI = config.get("database_log", "uri")

db = DBConnector(URI)
logger= Logger(db)

# Data loading
try : 
    df = spark.read.parquet(INPUT_PATH).repartition(20).cache()

    # Feature engineering
    df = df.withColumn("total_motility", col("Progressive motility (%)") + col("Non progressive sperm motility (%)"))
    df = df.withColumn("motility_ratio", col("Progressive motility (%)") / (col("total_motility") + 1e-6))
    df = df.withColumn("motility_vitality_interaction", col("Progressive motility (%)") * col("Sperm vitality (%)"))
    df = df.withColumn("defect_ratio", (col("Head defects (%)") + col("Midpiece and neck defects (%)") + col("Tail defects (%)")) / 3)
    df = df.withColumn("health_index", (col("Sperm vitality (%)") + col("Progressive motility (%)") + col("Normal spermatozoa (%)")) / 3)

    # Define label based on WHO (World Health Organization) criteria 
    df = df.withColumn(
        "label",
        when(
            (col("Sperm concentration (x10⁶/mL)") >= 15) &
            (col("Progressive motility (%)") >= 32) &
            (col("Normal spermatozoa (%)") >= 4) &
            (col("Sperm vitality (%)") >= 58) &
            (col("Ejaculate volume (mL)") >= 1.5),
            1
        ).otherwise(0)
    )

    logger.log("INFO", "Dataframe", "Data featuring was SUCCESSFUL")
except Exception as e:
    logger.log("ERROR", "Dataframe", f"Data featuring FAILED. Reason : {e}")

# Define feature columns
feature_cols = [
    "Ejaculate volume (mL)", "pH", "Progressive motility (%)", 
    "Non progressive sperm motility (%)", "Sperm vitality (%)", 
    "Head defects (%)", "Midpiece and neck defects (%)", 
    "Tail defects (%)", "motility_ratio", 
    "motility_vitality_interaction", "defect_ratio", "health_index"
]

try :
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_vec")
    scaler = StandardScaler(inputCol="features_vec", outputCol="features")

    # Model definition
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        numTrees=100,
        maxDepth=6
    )

    pipeline = Pipeline(stages=[assembler, scaler, rf])

    # Split data into training and testing sets
    train, test = df.randomSplit([0.7, 0.3])

    # Train the challenger model
    challenger_model = pipeline.fit(train)
    challenger_preds = challenger_model.transform(test)

    # Load existing champion model if available
    has_champion = False
    champion_f1 = 0.0

    try:
        champion_model = PipelineModel.load(CHAMPION_PATH)
        champion_preds = champion_model.transform(test)
        has_champion = True
        logger.log("INFO", "MLM", "Existing Champion model successfully loaded")
    except Exception as e:
        logger.log("INFO", "MLM", "No valid Champion model found at path. Initializing first run")

    # Evaluate both models
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
    evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")

    challenger_f1 = evaluator_f1.evaluate(challenger_preds)
    challenger_acc = evaluator_acc.evaluate(challenger_preds)

    if has_champion:
        champion_f1 = evaluator_f1.evaluate(champion_preds)
        champion_acc = evaluator_acc.evaluate(champion_preds)

    # Decision logic for model promotion
    if (not has_champion) or (challenger_f1 > champion_f1):
        logger.log("INFO", "MLM", "Promoting Challenger to Champion")
        challenger_model.write().overwrite().save(CHAMPION_PATH)
    else:
        logger.log("INFO", "MLM", "Challenger did not outperform. Keeping existing Champion")
except Exception as e :
    logger.log("ERROR", "Dataframe", f"Machine Learning Model creation FAILED. Reason : {e}")

spark.stop()