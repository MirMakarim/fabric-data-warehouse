-- =============================================================
-- Fabric Warehouse: wh_procurement
-- 01 · Schema
-- =============================================================
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dw')
    EXEC('CREATE SCHEMA dw');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'rpt')
    EXEC('CREATE SCHEMA rpt');
GO
