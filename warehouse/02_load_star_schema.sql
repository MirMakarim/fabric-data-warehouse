-- =============================================================
-- 02 · Load the star schema from the Lakehouse gold layer
-- Uses cross-database queries (same workspace) and CTAS.
-- Called by the pipeline after notebook 03_gold_model.
-- =============================================================
CREATE OR ALTER PROCEDURE dw.usp_load_star_schema
AS
BEGIN
    SET NOCOUNT ON;

    -- Dimensions ---------------------------------------------------
    DROP TABLE IF EXISTS dw.dim_supplier;
    CREATE TABLE dw.dim_supplier AS
    SELECT CAST(supplier_id AS VARCHAR(10))         AS supplier_id,
           CAST(supplier_name AS VARCHAR(100))      AS supplier_name,
           CAST(country AS VARCHAR(2))              AS country,
           CAST(category AS VARCHAR(40))            AS category,
           CAST(payment_terms_days AS INT)          AS payment_terms_days,
           CAST(sourcing_type AS VARCHAR(20))       AS sourcing_type
    FROM   lh_procurement.dbo.gold_dim_supplier;

    DROP TABLE IF EXISTS dw.dim_material;
    CREATE TABLE dw.dim_material AS
    SELECT CAST(material_id AS VARCHAR(10))   AS material_id,
           CAST(material_name AS VARCHAR(100)) AS material_name,
           CAST(category AS VARCHAR(40))       AS category,
           CAST(unit AS VARCHAR(5))            AS unit
    FROM   lh_procurement.dbo.gold_dim_material;

    DROP TABLE IF EXISTS dw.dim_plant;
    CREATE TABLE dw.dim_plant AS
    SELECT CAST(plant_id AS VARCHAR(10))    AS plant_id,
           CAST(plant_name AS VARCHAR(60))  AS plant_name,
           CAST(country AS VARCHAR(2))      AS country,
           CAST(region AS VARCHAR(20))      AS region
    FROM   lh_procurement.dbo.gold_dim_plant;

    DROP TABLE IF EXISTS dw.dim_date;
    CREATE TABLE dw.dim_date AS
    SELECT date_key, CAST([date] AS DATE) AS [date], [year], [quarter], [month],
           CAST(month_name AS VARCHAR(3)) AS month_name,
           CAST(year_month AS VARCHAR(7)) AS year_month,
           iso_week, is_weekend
    FROM   lh_procurement.dbo.gold_dim_date;

    -- Facts ----------------------------------------------------------
    DROP TABLE IF EXISTS dw.fact_po_line;
    CREATE TABLE dw.fact_po_line AS
    SELECT CAST(po_number AS VARCHAR(12)) AS po_number,
           po_line, supplier_id, material_id, plant_id,
           po_date_key, requested_date_key, receipt_date_key,
           quantity, received_qty,
           CAST(currency AS VARCHAR(3))   AS currency,
           CAST(line_amount AS DECIMAL(18,2)) AS line_amount,
           CAST(spend_eur AS DECIMAL(18,2))   AS spend_eur,
           po_cycle_days, delivery_delay_days, is_on_time
    FROM   lh_procurement.dbo.gold_fact_po_line;

    DROP TABLE IF EXISTS dw.fact_invoice;
    CREATE TABLE dw.fact_invoice AS
    SELECT CAST(invoice_id AS VARCHAR(12)) AS invoice_id,
           CAST(po_number AS VARCHAR(12))  AS po_number,
           po_line, invoice_date_key,
           CAST(currency AS VARCHAR(3))    AS currency,
           CAST(invoice_amount AS DECIMAL(18,2))     AS invoice_amount,
           CAST(invoice_amount_eur AS DECIMAL(18,2)) AS invoice_amount_eur
    FROM   lh_procurement.dbo.gold_fact_invoice;

    -- Informational keys (not enforced in Fabric Warehouse) ----------
    ALTER TABLE dw.dim_supplier ADD CONSTRAINT pk_dim_supplier PRIMARY KEY NONCLUSTERED (supplier_id) NOT ENFORCED;
    ALTER TABLE dw.dim_material ADD CONSTRAINT pk_dim_material PRIMARY KEY NONCLUSTERED (material_id) NOT ENFORCED;
    ALTER TABLE dw.dim_plant    ADD CONSTRAINT pk_dim_plant    PRIMARY KEY NONCLUSTERED (plant_id)    NOT ENFORCED;
    ALTER TABLE dw.dim_date     ADD CONSTRAINT pk_dim_date     PRIMARY KEY NONCLUSTERED (date_key)    NOT ENFORCED;

    ALTER TABLE dw.fact_po_line ADD CONSTRAINT fk_po_supplier FOREIGN KEY (supplier_id) REFERENCES dw.dim_supplier (supplier_id) NOT ENFORCED;
    ALTER TABLE dw.fact_po_line ADD CONSTRAINT fk_po_material FOREIGN KEY (material_id) REFERENCES dw.dim_material (material_id) NOT ENFORCED;
    ALTER TABLE dw.fact_po_line ADD CONSTRAINT fk_po_plant    FOREIGN KEY (plant_id)    REFERENCES dw.dim_plant (plant_id)       NOT ENFORCED;
    ALTER TABLE dw.fact_po_line ADD CONSTRAINT fk_po_date     FOREIGN KEY (po_date_key) REFERENCES dw.dim_date (date_key)        NOT ENFORCED;

    -- Row counts for the pipeline log
    SELECT 'dim_supplier' AS table_name, COUNT(*) AS row_count FROM dw.dim_supplier
    UNION ALL SELECT 'dim_material', COUNT(*) FROM dw.dim_material
    UNION ALL SELECT 'dim_plant',    COUNT(*) FROM dw.dim_plant
    UNION ALL SELECT 'dim_date',     COUNT(*) FROM dw.dim_date
    UNION ALL SELECT 'fact_po_line', COUNT(*) FROM dw.fact_po_line
    UNION ALL SELECT 'fact_invoice', COUNT(*) FROM dw.fact_invoice;
END;
GO
