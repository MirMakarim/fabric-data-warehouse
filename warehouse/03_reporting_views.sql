-- =============================================================
-- 03 · Reporting views for analysts and ad-hoc SQL
-- =============================================================

-- Monthly spend and delivery performance per supplier
CREATE OR ALTER VIEW rpt.vw_supplier_monthly AS
SELECT d.year_month,
       s.supplier_id,
       s.supplier_name,
       s.category,
       s.sourcing_type,
       SUM(f.spend_eur)                                    AS spend_eur,
       COUNT(*)                                            AS po_lines,
       AVG(CAST(f.is_on_time AS DECIMAL(5,4)))             AS on_time_rate,
       AVG(CAST(f.po_cycle_days AS DECIMAL(9,2)))          AS avg_po_cycle_days
FROM   dw.fact_po_line f
JOIN   dw.dim_supplier s ON s.supplier_id = f.supplier_id
JOIN   dw.dim_date     d ON d.date_key    = f.po_date_key
GROUP BY d.year_month, s.supplier_id, s.supplier_name, s.category, s.sourcing_type;
GO

-- Share of spend with single-source suppliers by category
CREATE OR ALTER VIEW rpt.vw_single_source_exposure AS
SELECT m.category,
       SUM(f.spend_eur)                                                      AS total_spend_eur,
       SUM(CASE WHEN s.sourcing_type = 'Single source' THEN f.spend_eur END) AS single_source_spend_eur,
       CAST(SUM(CASE WHEN s.sourcing_type = 'Single source' THEN f.spend_eur ELSE 0 END)
            / NULLIF(SUM(f.spend_eur), 0) AS DECIMAL(5,4))                   AS single_source_share
FROM   dw.fact_po_line f
JOIN   dw.dim_supplier s ON s.supplier_id = f.supplier_id
JOIN   dw.dim_material m ON m.material_id = f.material_id
GROUP BY m.category;
GO

-- Top 10 suppliers by spend with their rank inside each category
CREATE OR ALTER VIEW rpt.vw_top_suppliers AS
WITH supplier_spend AS (
    SELECT s.category, s.supplier_name, SUM(f.spend_eur) AS spend_eur
    FROM   dw.fact_po_line f
    JOIN   dw.dim_supplier s ON s.supplier_id = f.supplier_id
    GROUP BY s.category, s.supplier_name
)
SELECT category, supplier_name, spend_eur,
       RANK() OVER (PARTITION BY category ORDER BY spend_eur DESC)        AS rank_in_category,
       spend_eur * 1.0 / SUM(spend_eur) OVER (PARTITION BY category)      AS share_of_category
FROM   supplier_spend;
GO
