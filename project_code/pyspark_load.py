from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col,avg
from google.cloud import storage
import configparser
from io import StringIO
import json
from Logger import Logger
from DBConnector import DBConnector

# Read config file
client = storage.Client()
bucket = client.bucket("dataops-assets")
blob = bucket.blob("config/config.ini")

config_text = blob.download_as_text()

config = configparser.ConfigParser()
config.read_string(config_text)

# Define paths and URI
INPUT_PATH = config.get("buckets", "silver")
MONGODB_URI = config.get("database", "uri")
POSTEGRESQL_URI = config.get("database_log", "uri")

db = DBConnector(POSTEGRESQL_URI)
logger= Logger(db)

# Spark Session initialization
spark = SparkSession.builder \
    .appName("MongoDB-ETL") \
    .config("spark.jars", "gs://semendata/jars/mongo-spark-connector_2.12-10.4.0.jar,"
                          "gs://semendata/jars/mongodb-driver-sync-4.11.1.jar,"
                          "gs://semendata/jars/mongodb-driver-core-4.11.1.jar,"
                          "gs://semendata/jars/bson-4.11.1.jar") \
    .getOrCreate()

# Data loading
df = spark.read.parquet(INPUT_PATH).repartition(20).cache()

try :
    # Load whole dataframe into MongoDB
    df.write.format("mongodb") \
        .option("connection.uri", MONGODB_URI) \
        .option("collection", "semen_data") \
        .mode("append") \
        .save()
    logger.log("INFO", "Data", "Data is written SUCCESSFULLY into MongoDB")    
except Exception as e :
    logger.log("ERROR", "Data", f"Data FAILED to be written into MongoDB. Reason : {e}")

# Calculate statistics for each disease and count of patients with each disease
oligo_stats = (df.groupBy("Oligospermia").agg(
        avg("Sperm concentration (x10⁶/mL)").alias("Avg Sperm concentration (x10⁶/mL)"),
        avg("Total sperm count (x10⁶)").alias("Avg Total sperm count (x10⁶)")
    )
)

astheno_stats = (df.groupBy("Asthenozoospermia").agg(
        avg("Progressive motility (%)").alias("Avg Progressive motility (%)"),
        avg("Immotile sperm (%)").alias("Avg Immotile sperm (%)")
    )
)

terato_stats = (df.groupBy("Teratozoospermia").agg(
        avg("Normal spermatozoa (%)").alias("Avg Normal spermatozoa (%)"),
        avg("Head defects (%)").alias("Avg Head defects (%)"),
        avg("Midpiece and neck defects (%)").alias("Avg Midpiece and neck defects (%)"),
        avg("Tail defects (%)").alias("Avg Tail defects (%)")
    )
)

necro_stats = (df.groupBy("Necrospermia").agg(
        avg("Sperm vitality (%)").alias("Avg Sperm vitality (%)")
    )
)

hypo_stats = (df.groupBy("Hypospermia").agg(
        avg("Ejaculate volume (mL)").alias("Avg Ejaculate volume (mL)"),
        avg("pH").alias("Avg pH")
    )
)
# Count of patients with each disease
oligo_count = df.filter(col("Oligospermia") == 1).count()
astheno_count = df.filter(col("Asthenozoospermia") == 1).count()
terato_count = df.filter(col("Teratozoospermia") == 1).count()
necro_count = df.filter(col("Necrospermia") == 1).count()
hypo_count = df.filter(col("Hypospermia") == 1).count()

# Create a DataFrame for the count of patients with each disease
diseases = [
    Row(Disease="Oligospermia", Count=oligo_count),
    Row(Disease="Asthenozoospermia", Count=astheno_count),
    Row(Disease="Teratozoospermia", Count=terato_count),
    Row(Disease="Necrospermia", Count=necro_count),
    Row(Disease="Hypospermia", Count=hypo_count)
]

diseases_count = spark.createDataFrame(diseases)

# Convert the statistics and counts to a dictionary format for JSON serialization

# 1. Oligospermia
pd_oligo = oligo_stats.toPandas()
pd_oligo["Oligospermia"] = pd_oligo["Oligospermia"].astype(str) # Μετατροπή σε string για JSON
oligo_dict = pd_oligo.set_index("Oligospermia").to_dict(orient="index")

# 2. Asthenozoospermia
pd_astheno = astheno_stats.toPandas()
pd_astheno["Asthenozoospermia"] = pd_astheno["Asthenozoospermia"].astype(str)
astheno_dict = pd_astheno.set_index("Asthenozoospermia").to_dict(orient="index")

# 3. Teratozoospermia
pd_terato = terato_stats.toPandas()
pd_terato["Teratozoospermia"] = pd_terato["Teratozoospermia"].astype(str)
terato_dict = pd_terato.set_index("Teratozoospermia").to_dict(orient="index")

# 4. Necrospermia
pd_necro = necro_stats.toPandas()
pd_necro["Necrospermia"] = pd_necro["Necrospermia"].astype(str)
necro_dict = pd_necro.set_index("Necrospermia").to_dict(orient="index")

# 5. Hypospermia
pd_hypo = hypo_stats.toPandas()
pd_hypo["Hypospermia"] = pd_hypo["Hypospermia"].astype(str)
hypo_dict = pd_hypo.set_index("Hypospermia").to_dict(orient="index")

# 6. Diseases Count (Εδώ το index είναι ήδη Strings, οπότε δεν χρειάζεται αλλαγή)
diseases_dict = diseases_count.toPandas().set_index("Disease").to_dict(orient="index")

# Combine all metrics into a single dictionary for JSON output
metric_report = {
    "Oligospermia_Analysis": oligo_dict,
    "Asthenozoospermia_Analysis": astheno_dict,
    "Teratozoospermia_Analysis": terato_dict,
    "Necrospermia_Analysis": necro_dict,
    "Hypospermia_Analysis" : hypo_dict,
    "Diseases_Count": diseases_dict
}

# Serialize the metrics report to JSON format
json_output = json.dumps(metric_report, indent=4, ensure_ascii=False)

# Create an RDD from the JSON string and read it as a DataFrame
json_rdd = spark.sparkContext.parallelize([json_output])
df_to_load = spark.read.json(json_rdd)

try :
    # Αποθήκευση στη MongoDB μέσω Spark Connector
    df_to_load.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("connection.uri", MONGODB_URI) \
        .option("collection", "metrics") \
        .save()
    logger.log("INFO", "Data", "Data Metrics is written SUCCESSFULLY into MongoDB")    
except Exception as e :
    logger.log("ERROR", "Data", f"Data MetricsFAILED to be written into MongoDB. Reason : {e}")

spark.stop()