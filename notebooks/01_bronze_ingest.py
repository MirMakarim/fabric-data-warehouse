# Fabric notebook source

# MARKDOWN ********************

# # 01 · Bronze ingest
# Loads the raw CSV files from the Lakehouse `Files/raw/` folder into Delta tables
# without changing the data. Every row gets ingestion metadata so later layers can
# trace where it came from.
#
# **Attach:** Lakehouse `lh_procurement` as the default lakehouse.

# CELL ********************

from pyspark.sql import functions as F

RAW_PATH = "Files/raw"
SOURCES = ["suppliers", "materials", "plants", "purchase_orders", "goods_receipts", "invoices", "fx_rates"]

# PARAMETERS CELL ********************

# Default for manual runs; the pipeline pl_procurement_daily passes its own run_id
run_id = "manual"

# CELL ********************

def ingest(source: str) -> int:
    """Read one raw CSV as strings and load it into bronze_<source> (full refresh)."""
    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)          # bronze keeps everything as string
        .csv(f"{RAW_PATH}/{source}.csv")
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
    )
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", True)
       .saveAsTable(f"bronze_{source}"))
    return df.count()

# CELL ********************

results = {source: ingest(source) for source in SOURCES}
for source, rows in results.items():
    print(f"bronze_{source:<16} {rows:>8,} rows")

# CELL ********************

# Return row counts to the calling pipeline for monitoring
import json
mssparkutils.notebook.exit(json.dumps(results))
