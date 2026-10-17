# Fabric notebook source

# MARKDOWN ********************

# # 03 · Gold model
# Builds the business-ready star schema from the silver tables:
#
# | Table | Grain |
# |---|---|
# | `gold_dim_supplier` | one row per supplier |
# | `gold_dim_material` | one row per material |
# | `gold_dim_plant` | one row per plant |
# | `gold_dim_date` | one row per calendar day |
# | `gold_fact_po_line` | one row per purchase order line |
# | `gold_fact_invoice` | one row per invoice |
#
# **Attach:** Lakehouse `lh_procurement` as the default lakehouse.

# CELL ********************

from pyspark.sql import functions as F

def save(df, name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(name)
    print(f"{name:<22} {df.count():>8,} rows")

# CELL ********************

# Dimensions
dim_supplier = spark.table("silver_suppliers").select(
    "supplier_id", "supplier_name", "country", "category", "payment_terms_days",
    F.when(F.col("is_single_source"), "Single source").otherwise("Multi source").alias("sourcing_type"),
)
dim_material = spark.table("silver_materials").select("material_id", "material_name", "category", "unit")
dim_plant = spark.table("silver_plants").select("plant_id", "plant_name", "country", "region")

dim_date = (
    spark.sql("SELECT explode(sequence(to_date('2024-01-01'), to_date('2026-12-31'), interval 1 day)) AS date")
    .select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMM").alias("month_name"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.weekofyear("date").alias("iso_week"),
        (F.dayofweek("date").isin(1, 7)).alias("is_weekend"),
    )
)

save(dim_supplier, "gold_dim_supplier")
save(dim_material, "gold_dim_material")
save(dim_plant, "gold_dim_plant")
save(dim_date, "gold_dim_date")

# CELL ********************

# Fact: purchase order lines with delivery performance
po = spark.table("silver_purchase_order_lines")
gr = spark.table("silver_goods_receipts").select("po_number", "po_line", "receipt_date", "received_qty")

fact_po_line = (
    po.join(gr, ["po_number", "po_line"], "left")
    .select(
        "po_number", "po_line", "supplier_id", "material_id", "plant_id",
        F.date_format("po_date", "yyyyMMdd").cast("int").alias("po_date_key"),
        F.date_format("requested_delivery_date", "yyyyMMdd").cast("int").alias("requested_date_key"),
        F.date_format("receipt_date", "yyyyMMdd").cast("int").alias("receipt_date_key"),
        "quantity", "received_qty", "currency",
        F.col("line_amount").cast("decimal(18,2)").alias("line_amount"),
        F.col("line_amount_eur").cast("decimal(18,2)").alias("spend_eur"),
        F.datediff("receipt_date", "po_date").alias("po_cycle_days"),
        F.datediff("receipt_date", "requested_delivery_date").alias("delivery_delay_days"),
        F.when(F.col("receipt_date").isNull(), None)
         .when(F.col("receipt_date") <= F.col("requested_delivery_date"), 1).otherwise(0).alias("is_on_time"),
    )
)
save(fact_po_line, "gold_fact_po_line")

# CELL ********************

# Fact: invoices
fact_invoice = spark.table("silver_invoices").select(
    "invoice_id", "po_number", "po_line",
    F.date_format("invoice_date", "yyyyMMdd").cast("int").alias("invoice_date_key"),
    "currency", "invoice_amount",
    F.col("invoice_amount_eur").cast("decimal(18,2)").alias("invoice_amount_eur"),
)
save(fact_invoice, "gold_fact_invoice")

# CELL ********************

# Optimize for Direct Lake: V-Order and compaction
for t in ["gold_fact_po_line", "gold_fact_invoice"]:
    spark.sql(f"OPTIMIZE {t} VORDER")
