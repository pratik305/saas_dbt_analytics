# Base Path

base_path = "/Volumes/workspace/saas_revenue/saas"

# Import library of timestamp
from pyspark.sql.functions import current_timestamp

# Function to ingest csv file to bronze table

def ingest_csv_to_bronze(file_name, table_name):
    df = (
        spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("mergeSchema", "true")
            .csv(f"{base_path}/{file_name}")
    )

    df = df.withColumn("ingestion_ts", current_timestamp())

    (
        df.write
            .format("delta")
            .mode("overwrite")   # first load only
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
    )
# path to csv files

tables = [
    ("ravenstack_accounts.csv", "bronze_accounts"),
    ("ravenstack_churn_events.csv", "bronze_churn_events"),
    ("ravenstack_feature_usage.csv", "bronze_feature_usage"),
    ("ravenstack_subscriptions.csv", "bronze_subscriptions"),
    ("ravenstack_support_tickets.csv", "bronze_support_tickets"),
]

# Loop through all csv files and ingest to bronze table

for file_name, table_name in tables:
    ingest_csv_to_bronze(file_name, table_name)
