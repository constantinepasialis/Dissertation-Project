from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from google.cloud import storage
import configparser
from io import StringIO
from Logger import Logger
from DBConnector import DBConnector

# Read config file
client = storage.Client()
bucket = client.bucket("dataops-assets")
blob = bucket.blob("config/config.ini")

config_text = blob.download_as_text()

config = configparser.ConfigParser()
config.read_string(config_text)

# Define Paths and URI
INPUT_PATH = config.get("buckets", "bronze")
OUTPUT_PATH = config.get("buckets", "silver")

ASTHENO_FOLDER = config.get("buckets", "astheno")
HYPO_FOLDER = config.get("buckets", "hypo")
NECRO_FOLDER = config.get("buckets", "necro")
OLIGO_FOLDER = config.get("buckets", "oligo")
TERA_FOLDER = config.get("buckets", "tera")

URI = config.get("database_log", "uri")

db = DBConnector(URI)
logger= Logger(db)

# Spark Session Initialization
spark = SparkSession.builder \
    .appName("Semen_Data_Transformation") \
    .getOrCreate()

try:
    # Data loading
    df = spark.read.parquet(INPUT_PATH).repartition(20).cache()
    logger.log("INFO", "Dataframe", "Data loading was SUCCESSFUL")
except Exception as e:
    logger.log("ERROR", "Dataframe", f"Data loading FAILED. Reason : {e}")

# Data Cleaning
df = df.drop_duplicates()
df = df.dropna()

try :
    # Filtering
    df = df.withColumn("Oligospermia", when(col("Sperm concentration (x10⁶/mL)") < 15, 1).otherwise(0))
    df = df.withColumn("Asthenozoospermia", when(col("Progressive motility (%)") < 32, 1).otherwise(0))
    df = df.withColumn("Teratozoospermia", when(col("Normal spermatozoa (%)") < 4, 1).otherwise(0))
    df = df.withColumn("Necrospermia", when(col("Sperm vitality (%)") < 58, 1).otherwise(0))
    df = df.withColumn("Hypospermia", when(col("Ejaculate volume (mL)") < 1.5, 1).otherwise(0))

    # Create dataframes based on filtering
    df_oligospermia = df.filter(col("Oligospermia")==1)
    df_asthenozoospermia = df.filter(col("Asthenozoospermia")==1)
    df_teratozoospermia = df.filter(col("Teratozoospermia")==1)
    df_necrospermia = df.filter(col("Necrospermia")==1)
    df_hypospermia = df.filter(col("Hypospermia")==1)

    logger.log("INFO", "Dataframe", "Data filtering was SUCCESSFUL")
except Exception as e:
    logger.log("ERROR", "Dataframe", f"Data filtering FAILED. Reason : {e}")

try :
    # Save each dataframe in specified files
    df_oligospermia.write.mode("overwrite").parquet(OLIGO_FOLDER)
    df_asthenozoospermia.write.mode("overwrite").parquet(ASTHENO_FOLDER)
    df_teratozoospermia.write.mode("overwrite").parquet(TERA_FOLDER)
    df_necrospermia.write.mode("overwrite").parquet(NECRO_FOLDER)
    df_hypospermia.write.mode("overwrite").parquet(HYPO_FOLDER)

    # Save base dataframe in next layer (Gold Layer)
    df.repartition(20).write.mode("overwrite").parquet(OUTPUT_PATH)

    logger.log("INFO", "Dataframe", "Data writing was SUCCESSFUL")
except Exception as e:
    logger.log("ERROR", "Dataframe", f"Data writing FAILED. Reason : {e}")

#Stopping Spark Session
spark.stop()