-- ============================================================
-- SECTION 1: SCHEMA & TABLE CREATION
-- ============================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS inventory;

-- Drop and recreate InventorySnapshot (idempotent setup)
DROP TABLE IF EXISTS inventory."InventorySnapshot" CASCADE;

CREATE TABLE inventory."InventorySnapshot" (
    sku_id          VARCHAR(50)     NOT NULL,
    product_name    VARCHAR(255)    NOT NULL,
    warehouse_code  VARCHAR(20)     NOT NULL,
    stock_on_hand   INT             NOT NULL DEFAULT 0,
    last_updated    TIMESTAMP WITH TIME ZONE NOT NULL,
    unit_cost       DECIMAL(10, 2)  NOT NULL,

    CONSTRAINT pk_inventory_snapshot PRIMARY KEY (sku_id, warehouse_code)
);

COMMENT ON TABLE inventory."InventorySnapshot"
    IS 'Daily inventory snapshot table. Full extract per pipeline run. Source for MS Fabric Lakehouse ingestion via On-premises Data Gateway.';

COMMENT ON COLUMN inventory."InventorySnapshot".sku_id
    IS 'Stock Keeping Unit identifier. Unique per product variant.';

COMMENT ON COLUMN inventory."InventorySnapshot".unit_cost
    IS 'Unit cost in local currency. DECIMAL(10,2) precision must be preserved in Fabric Delta table.';


-- ============================================================
-- SECTION 2: REFERENCE DATA (Warehouses & Product Categories)
-- ============================================================

-- 12 Regional Warehouse Codes (matches case study)
-- WHxx format: WH01 = North Hub, WH12 = South Island
-- Used as warehouse_code values throughout data generation

-- Product Category Prefixes (for SKU and product name generation):
--   ELEC = Electronics
--   APPL = Appliances
--   FURN = Furniture
--   CLTH = Clothing
--   GROC = Grocery / FMCG
--   SPRT = Sports & Outdoors
--   HLTH = Health & Beauty
--   TOYS = Toys & Games
--   TOOL = Tools & Hardware
--   AUTO = Automotive


-- ============================================================
-- SECTION 3: SEED DATA GENERATION (2,000,000 rows)
-- ============================================================
-- Strategy:
--   80,000 unique SKUs  x  up to 12 warehouses (not all SKUs
--   appear in every warehouse) = ~2,000,000 rows.
--
-- Root cause of original error:
--   CROSS JOIN with 10 descriptor words produced 10 candidate
--   rows sharing the same (sku_id, warehouse_code) key within
--   a single INSERT statement. PostgreSQL raises:
--     "ON CONFLICT DO UPDATE command cannot affect row a second time"
--   because ON CONFLICT cannot resolve intra-statement duplicates —
--   only conflicts against already-committed table rows.
--
-- Fix:
--   The descriptor word is now derived deterministically from
--   (sku_seq + wh_num) using MOD, eliminating the CROSS JOIN on
--   desc_word entirely. Each (sku_id, warehouse_code) pair is now
--   generated exactly once in the SELECT — no intra-batch duplicates.
-- ============================================================

INSERT INTO inventory."InventorySnapshot" (
    sku_id,
    product_name,
    warehouse_code,
    stock_on_hand,
    last_updated,
    unit_cost
)
SELECT
    -- SKU_ID: e.g. ELEC-00001, GROC-00042
    cat.prefix
        || '-'
        || LPAD(s.sku_seq::TEXT, 5, '0')                        AS sku_id,

    -- Product name: Category label + deterministic descriptor + SKU sequence
    -- Descriptor is derived from (sku_seq + wh_num) MOD 10 — no CROSS JOIN needed,
    -- guarantees one unique product_name per (sku_id, warehouse_code) pair.
    cat.label
        || ' '
        || (ARRAY[
                'Premium','Pro','Lite','Max','Ultra',
                'Classic','Essential','Advanced','Smart','Compact'
           ])[ ((s.sku_seq + w.wh_num) % 10) + 1 ]
        || ' '
        || LPAD(s.sku_seq::TEXT, 5, '0')                        AS product_name,

    -- Warehouse code: WH01 through WH12
    'WH' || LPAD(w.wh_num::TEXT, 2, '0')                        AS warehouse_code,

    -- Stock on hand: realistic range per category
    GREATEST(
        0,
        (cat.base_stock
            + (RANDOM() * cat.stock_range)::INT
            - (RANDOM() * 50)::INT)
    )                                                            AS stock_on_hand,

    -- Last updated: within the last 24 hours, timezone-aware
    (NOW() - (RANDOM() * INTERVAL '24 hours'))
        AT TIME ZONE 'UTC'                                       AS last_updated,

    -- Unit cost: realistic per category with DECIMAL(10,2) precision
    ROUND(
        (cat.min_cost + RANDOM() * (cat.max_cost - cat.min_cost))::NUMERIC,
        2
    )                                                            AS unit_cost

FROM
    -- 80,000 SKUs across 8 categories (~10,000 each)
    generate_series(1, 10000) AS s(sku_seq)

    -- 8 product categories
    CROSS JOIN (VALUES
        ('ELEC', 'Electronics',      200,  800,   49.99,  1299.99),
        ('APPL', 'Appliances',       150,  500,   89.99,  2499.99),
        ('FURN', 'Furniture',         50,  200,  119.99,  3999.99),
        ('CLTH', 'Clothing',         300, 1000,    9.99,   249.99),
        ('GROC', 'Grocery',          400, 1500,    0.99,    89.99),
        ('SPRT', 'Sports',           100,  600,   14.99,   799.99),
        ('HLTH', 'Health & Beauty',  250,  900,    4.99,   199.99),
        ('TOOL', 'Tools',            100,  400,   12.99,   599.99)
    ) AS cat(prefix, label, base_stock, stock_range, min_cost, max_cost)

    -- 12 warehouses — no descriptor CROSS JOIN, so each (sku_id, warehouse_code)
    -- is produced exactly once before reaching the conflict handler
    CROSS JOIN generate_series(1, 12) AS w(wh_num)

WHERE
    -- ~70% warehouse presence rate (deterministic, no duplicates)
    (s.sku_seq * 31 + w.wh_num * 17) % 10 < 7

-- ON CONFLICT is now a true safety net for re-runs only,
-- not a substitute for intra-batch deduplication
ON CONFLICT (sku_id, warehouse_code) DO UPDATE
    SET stock_on_hand = EXCLUDED.stock_on_hand,
        last_updated  = EXCLUDED.last_updated,
        unit_cost     = EXCLUDED.unit_cost;


-- ============================================================
-- SECTION 4: SIMULATE INTRA-DAY CORRECTIONS
-- ============================================================
-- Warehouse staff correct the same record multiple times per day.
-- This mimics the case study's statement:
--   "the same record may be corrected multiple times before the
--    supervisor confirms completion"
-- Updates a random 5% of rows with fresh timestamps and adjusted
-- stock levels to represent mid-day corrections.
-- ============================================================

UPDATE inventory."InventorySnapshot"
SET
    stock_on_hand = GREATEST(0, stock_on_hand + (RANDOM() * 100 - 50)::INT),
    last_updated  = (NOW() - (RANDOM() * INTERVAL '4 hours')) AT TIME ZONE 'UTC'
WHERE
    -- Deterministic 5% sample using ctid hash
    (hashtext(sku_id || warehouse_code) % 20) = 0;


-- ============================================================
-- SECTION 5: EDGE CASE ROWS (for pipeline integrity validation)
-- ============================================================

-- Zero stock on hand (valid write-off state)
INSERT INTO inventory."InventorySnapshot"
    (sku_id, product_name, warehouse_code, stock_on_hand, last_updated, unit_cost)
VALUES
    ('EDGE-ZERO-01', 'Zero Stock Test Product',    'WH01', 0, NOW() AT TIME ZONE 'UTC', 9.99),
    ('EDGE-ZERO-02', 'Written Off Item Reference', 'WH06', 0, NOW() AT TIME ZONE 'UTC', 0.01)
ON CONFLICT (sku_id, warehouse_code) DO NOTHING;

-- High unit cost (tests DECIMAL(10,2) precision in Fabric Delta table)
INSERT INTO inventory."InventorySnapshot"
    (sku_id, product_name, warehouse_code, stock_on_hand, last_updated, unit_cost)
VALUES
    ('APPL-HIGHVAL-01', 'Industrial Appliance Server Unit', 'WH03', 2,  NOW() AT TIME ZONE 'UTC', 99999.99),
    ('ELEC-HIGHVAL-01', 'Enterprise Electronics Array',     'WH11', 1,  NOW() AT TIME ZONE 'UTC', 12345.67)
ON CONFLICT (sku_id, warehouse_code) DO NOTHING;

-- Product name with special characters (tests encoding in CSV vs pipeline)
INSERT INTO inventory."InventorySnapshot"
    (sku_id, product_name, warehouse_code, stock_on_hand, last_updated, unit_cost)
VALUES
    ('CLTH-SPEC-01', 'Café & Co. Tote Bag — Premium', 'WH02', 55, NOW() AT TIME ZONE 'UTC', 24.99),
    ('GROC-SPEC-01', 'Crème Brûlée Mix (500g)',        'WH07', 88, NOW() AT TIME ZONE 'UTC',  6.49)
ON CONFLICT (sku_id, warehouse_code) DO NOTHING;

-- Maximum length product name (255 chars)
INSERT INTO inventory."InventorySnapshot"
    (sku_id, product_name, warehouse_code, stock_on_hand, last_updated, unit_cost)
VALUES
    (
        'TOOL-MAXLEN-01',
        RPAD('MaxLength Tool Product Name For Pipeline Schema Validation Testing Purposes Only', 255, ' X'),
        'WH09',
        10,
        NOW() AT TIME ZONE 'UTC',
        49.99
    )
ON CONFLICT (sku_id, warehouse_code) DO NOTHING;


-- ============================================================
-- SECTION 6: PIPELINE AUDIT LOG TABLE
-- ============================================================
-- This is the Lakehouse audit table equivalent maintained locally
-- for reference. The actual audit_log is written to Fabric by the
-- Set Variable + Copy Data activities in the pipeline.
-- Kept here for local regression testing.
-- ============================================================

DROP TABLE IF EXISTS inventory.pipeline_audit_log;

CREATE TABLE inventory.pipeline_audit_log (
    audit_id         SERIAL          PRIMARY KEY,
    run_timestamp    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    source_table     VARCHAR(255)    NOT NULL,
    rows_transferred INT             NOT NULL,
    execution_status VARCHAR(50)     NOT NULL,  -- SUCCESS | FAILURE | PARTIAL
    pipeline_run_id  VARCHAR(100),              -- Fabric pipeline run GUID
    duration_seconds INT                        -- Optional: pipeline duration
);

COMMENT ON TABLE inventory.pipeline_audit_log
    IS 'Local mirror of the Fabric Lakehouse audit log. One row per pipeline execution. Used for regression testing and local lineage validation.';

-- Seed one historical audit entry to validate audit log structure
INSERT INTO inventory.pipeline_audit_log
    (run_timestamp, source_table, rows_transferred, execution_status, pipeline_run_id, duration_seconds)
VALUES
    (
        NOW() - INTERVAL '1 day',
        'inventory.InventorySnapshot',
        0,
        'FAILURE',
        'MANUAL-EXPORT-LEGACY',
        NULL
    );


-- ============================================================
-- SECTION 7: VALIDATION QUERIES
-- ============================================================
-- Run these after data generation to confirm readiness
-- before triggering the Fabric Pipeline.
-- ============================================================

-- 7.1 Total row count (target: ~2,000,000)
SELECT
    COUNT(*)                    AS total_rows,
    COUNT(DISTINCT sku_id)      AS unique_skus,
    COUNT(DISTINCT warehouse_code) AS unique_warehouses
FROM inventory."InventorySnapshot";

-- 7.2 Row count per warehouse
SELECT
    warehouse_code,
    COUNT(*)            AS row_count,
    MIN(stock_on_hand)  AS min_stock,
    MAX(stock_on_hand)  AS max_stock,
    ROUND(AVG(unit_cost)::NUMERIC, 2) AS avg_unit_cost
FROM inventory."InventorySnapshot"
GROUP BY warehouse_code
ORDER BY warehouse_code;

-- 7.3 Confirm no NULLs in NOT NULL columns
SELECT
    COUNT(*) FILTER (WHERE sku_id IS NULL)         AS null_sku_id,
    COUNT(*) FILTER (WHERE product_name IS NULL)   AS null_product_name,
    COUNT(*) FILTER (WHERE warehouse_code IS NULL) AS null_warehouse_code,
    COUNT(*) FILTER (WHERE stock_on_hand IS NULL)  AS null_stock_on_hand,
    COUNT(*) FILTER (WHERE last_updated IS NULL)   AS null_last_updated,
    COUNT(*) FILTER (WHERE unit_cost IS NULL)      AS null_unit_cost
FROM inventory."InventorySnapshot";

-- 7.4 Decimal precision check on unit_cost
SELECT
    sku_id,
    unit_cost,
    LENGTH(SPLIT_PART(unit_cost::TEXT, '.', 2)) AS decimal_places
FROM inventory."InventorySnapshot"
WHERE LENGTH(SPLIT_PART(unit_cost::TEXT, '.', 2)) > 2
LIMIT 10;

-- 7.5 Edge case rows present
SELECT sku_id, product_name, warehouse_code, stock_on_hand, unit_cost
FROM inventory."InventorySnapshot"
WHERE sku_id LIKE 'EDGE-%' OR sku_id LIKE '%-HIGHVAL-%' OR sku_id LIKE '%-SPEC-%' OR sku_id LIKE '%-MAXLEN-%'
ORDER BY sku_id;

-- 7.6 Confirm audit log table exists with seed entry
SELECT * FROM inventory.pipeline_audit_log;


-- ============================================================
-- SECTION 8: PIPELINE TRIGGER SIMULATION QUERY
-- ============================================================
-- This is the query the Fabric Copy Data activity will execute
-- against the PostgreSQL source. Validate it returns correctly
-- before configuring the linked service.
-- ============================================================

SELECT
    sku_id,
    product_name,
    warehouse_code,
    stock_on_hand,
    last_updated,
    unit_cost
FROM inventory."InventorySnapshot"
ORDER BY sku_id, warehouse_code
