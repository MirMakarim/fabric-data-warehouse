# Fabric notebook source

# MARKDOWN ********************

# # 02 · Silver transform
# Cleans and conforms the bronze tables:
# - trims text and fixes data types
# - removes exact duplicates and duplicate business keys
# - rejects invalid rows (quantity <= 0) into a quarantine table
# - converts all amounts to EUR with monthly FX rates
#
# **Attach:** Lakehouse `lh_procurement` as the default lakehouse.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def trim_all(df):
    """Trim every string column."""
    for name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(name, F.trim(F.col(name)))
    return df

def drop_meta(df):
    return df.drop("_source_file", "_ingested_at", "_run_id")

def save(df, name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(name)
    print(f"{name:<28} {df.count():>8,} rows")

# CELL ********************

# Master data
suppliers = (
    trim_all(drop_meta(spark.table("bronze_suppliers")))
    .withColumn("payment_terms_days", F.col("payment_terms_days").cast("int"))
    .withColumn("is_single_source", F.lower("is_single_source") == "true")
    .dropDuplicates(["supplier_id"])
)
materials = trim_all(drop_meta(spark.table("bronze_materials"))).dropDuplicates(["material_id"])
plants = trim_all(drop_meta(spark.table("bronze_plants"))).dropDuplicates(["plant_id"])

save(suppliers, "silver_suppliers")
save(materials, "silver_materials")
save(plants, "silver_plants")

# CELL ********************

# FX rates: units of foreign currency per 1 EUR, one rate per month
fx = (
    drop_meta(spark.table("bronze_fx_rates"))
    .withColumn("month_start", F.to_date("month_start"))
    .withColumn("units_per_eur", F.col("units_per_eur").cast("decimal(18,6)"))
)
save(fx, "silver_fx_rates")

fx_lookup = fx.select(
    F.col("currency").alias("_fx_cur"), F.col("month_start").alias("_fx_month"), "units_per_eur"
)

def to_eur(df, amount_col, date_col, out_col):
    """Join the monthly FX rate and convert amount_col to EUR."""
    df = df.withColumn("_month", F.trunc(date_col, "month"))
    cond = (df["currency"] == fx_lookup["_fx_cur"]) & (df["_month"] == fx_lookup["_fx_month"])
    return (
        df.join(fx_lookup, cond, "left")
          .withColumn(out_col, F.round(F.col(amount_col) / F.col("units_per_eur"), 2))
          .drop("_fx_cur", "_fx_month", "_month", "units_per_eur")
    )

# CELL ********************

# Purchase order lines
po_raw = (
    trim_all(drop_meta(spark.table("bronze_purchase_orders")))
    .dropDuplicates()                                           # exact duplicate rows
    .withColumn("po_line", F.col("po_line").cast("int"))
    .withColumn("po_date", F.to_date("po_date"))
    .withColumn("requested_delivery_date", F.to_date("requested_delivery_date"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(18,4)"))
)

# Keep one row per business key (latest PO date wins)
w = Window.partitionBy("po_number", "po_line").orderBy(F.col("po_date").desc())
po_dedup = po_raw.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

valid = po_dedup.filter(F.col("quantity") > 0)
rejected = po_dedup.filter(~(F.col("quantity") > 0)).withColumn("reject_reason", F.lit("quantity <= 0"))

po = to_eur(valid.withColumn("line_amount", F.col("quantity") * F.col("unit_price")),
            "line_amount", "po_date", "line_amount_eur")

save(po, "silver_purchase_order_lines")
save(rejected, "silver_quarantine_purchase_orders")

# CELL ********************

# Goods receipts and invoices
receipts = (
    trim_all(drop_meta(spark.table("bronze_goods_receipts")))
    .withColumn("po_line", F.col("po_line").cast("int"))
    .withColumn("receipt_date", F.to_date("receipt_date"))
    .withColumn("received_qty", F.col("received_qty").cast("int"))
    .dropDuplicates(["po_number", "po_line"])
)
invoices = (
    trim_all(drop_meta(spark.table("bronze_invoices")))
    .withColumn("po_line", F.col("po_line").cast("int"))
    .withColumn("invoice_date", F.to_date("invoice_date"))
    .withColumn("invoice_amount", F.col("invoice_amount").cast("decimal(18,2)"))
    .dropDuplicates(["invoice_id"])
)
invoices = to_eur(invoices, "invoice_amount", "invoice_date", "invoice_amount_eur")

save(receipts, "silver_goods_receipts")
save(invoices, "silver_invoices")

# CELL ********************

# Data quality checks: fail the run if keys are missing after conversion
missing_fx = po.filter(F.col("line_amount_eur").isNull()).count()
orphan_suppliers = po.join(suppliers, "supplier_id", "left_anti").count()
assert missing_fx == 0, f"{missing_fx} PO lines without FX rate"
assert orphan_suppliers == 0, f"{orphan_suppliers} PO lines with unknown supplier"
print("Data quality checks passed")
