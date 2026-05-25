from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, round, col, bround
import random
from google.cloud import storage
import configparser
from io import StringIO
from Logger import Logger
from DBConnector import DBConnector

# Google Cloud Paths
client = storage.Client()
bucket = client.bucket("dataops-assets")
blob = bucket.blob("config/config.ini")

config_text = blob.download_as_text()

config = configparser.ConfigParser()
config.read_string(config_text)

OUTPUT_PATH = config.get("buckets", "bronze")
URI = config.get("database_log", "uri")

db = DBConnector(URI)
logger= Logger(db)

# Spark Session initialization
spark = SparkSession.builder \
    .appName("Semen_Dataset_Generator") \
    .getOrCreate()

# Parameters
ROWS = 2_000_000

try :
    # Base DataFrame
    df = spark.range(1, ROWS + 1).withColumnRenamed("id", "id")

    # Core measurements
    df = (
        df
        .withColumn("Ejaculate volume (mL)", round((rand() * (10.0 - 1.3) + 1.3),1))
        .withColumn("Sperm concentration (x10⁶/mL)", (rand() * (350.0 - 3.8) + 3.8).cast("int"))
        .withColumn(
            "Total sperm count (x10⁶)",
            bround(col("Ejaculate volume (mL)") * col("Sperm concentration (x10⁶/mL)"),1)
        )
        .withColumn("pH", round(rand() * (8.0 - 7.0) + 7.0,1))
    )

    # Motility (sums to 100%)
    m1 = rand()
    m2 = rand()
    m3 = rand()
    s = m1 + m2 + m3

    df = (
        df
        .withColumn("Progressive motility (%)", round(m1 / s * 100, 1))
        .withColumn("Non progressive sperm motility (%)", round(m2 / s * 100, 1))
        .withColumn("Immotile sperm (%)", round(m3 / s * 100, 1))
    )

    # Vitality & morphology
    df = (
        df
        # Range: 40.0 to 100.0
        .withColumn("Sperm vitality (%)", round(rand() * (100.0 - 40.0) + 40.0, 1))

        # Range: 85.7 to 100.0
        .withColumn("Head defects (%)", round(rand() * (100.0 - 85.7) + 85.7, 1))

        # Range: 7.3 to 50.5
        .withColumn("Midpiece and neck defects (%)", round(rand() * (50.5 - 7.3) + 7.3, 1))

        # Range: 2.5 to 51.7
        .withColumn("Tail defects (%)", round(rand() * (51.7 - 2.5) + 2.5, 1))
    )

    # Normal spermatozoa
    df = df.withColumn(
        "Normal spermatozoa (%)",
        round(
            (1 - col("Head defects (%)") / 100)
            * (1 - col("Midpiece and neck defects (%)") / 100)
            * (1 - col("Tail defects (%)") / 100)
            * 100,
            1
        )
    )
    logger.log("INFO", "System", "Data creation was SUCCESSFUL")
except Exception as e:
    logger.log("ERROR", "System", f"Data creation FAILED. Reason : {e}")

try :
# Write to Parquet (Spark-managed)
    df = df.drop("id")
    df \
        .repartition(20) \
        .write \
        .mode("overwrite") \
        .parquet(OUTPUT_PATH)
    logger.log("INFO", "Dataframe", "Dataframe was written into OUTPUT_PATH SUCCESSFULLY")
except Exception as e :
    logger.log("ERROR", "Dataframe", f"Dataframe FAILED. Reason : {e}")

spark.stop()