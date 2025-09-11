-- =============================================================================
-- DEMO FILE UPLOAD COMMANDS - PATTERN-BASED JSON EDITION
-- =============================================================================
-- Pattern-based commands to upload JSON files to Snowflake stages
-- This script uses wildcards for flexible file uploading:
-- - Initial files: customers.json, products.json, etc.
-- - Update files: customers_update_*.json, products_update_*.json, etc.
--
-- IMPORTANT NOTE: File paths with spaces must be quoted!
-- ❌ WRONG: PUT file:///path with spaces/*.json @stage
-- ✅ CORRECT: PUT 'file:///path with spaces/*.json' @stage
-- =============================================================================

USE SCHEMA SNOWPIPE_DT_DEMO.DEMO_STAGES;

SELECT '=== UPLOADING INITIAL DATA FILES (PATTERN-BASED) ===' as UPLOAD_PHASE;

-- =============================================================================
-- OPTION 1: UPLOAD INITIAL FILES ONLY (customers.json, products.json, etc.)
-- =============================================================================
-- Use these commands to upload only the initial baseline files

-- Upload specific initial files using exact patterns
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/customers.json' @STG_CUSTOMERS_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/products.json' @STG_PRODUCTS_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/orders.json' @STG_ORDERS_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/order_items.json' @STG_ORDER_ITEMS_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/suppliers.json' @STG_SUPPLIERS_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/inventory.json' @STG_INVENTORY_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/warehouses.json' @STG_WAREHOUSES_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/employees.json' @STG_EMPLOYEES_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/sales_territories.json' @STG_SALES_TERRITORIES_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/promotions.json' @STG_PROMOTIONS_FILES;

-- =============================================================================
-- OPTION 2: UPLOAD UPDATE FILES USING PATTERNS (run separately after initial load)
-- =============================================================================
-- Uncomment these when you want to upload the timestamped update files
-- Replace the timestamp pattern with your actual timestamp


-- Upload all customer update files (any timestamp)
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/customers_update_*.json' @STG_CUSTOMERS_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/products_update_*.json' @STG_PRODUCTS_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/orders_update_*.json' @STG_ORDERS_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/order_items_update_*.json' @STG_ORDER_ITEMS_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/suppliers_update_*.json' @STG_SUPPLIERS_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/inventory_update_*.json' @STG_INVENTORY_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/warehouses_update_*.json' @STG_WAREHOUSES_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/employees_update_*.json' @STG_EMPLOYEES_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/sales_territories_update_*.json' @STG_SALES_TERRITORIES_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/promotions_update_*.json' @STG_PROMOTIONS_FILES;


-- =============================================================================
-- OPTION 3: UPLOAD ALL FILES AT ONCE (initial + updates) - USE WITH CAUTION
-- =============================================================================
-- This uploads ALL JSON files to each stage - use only if you want everything at once
-- Uncomment only if you want to upload both initial and update files simultaneously

/*
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/customers*.json' @STG_CUSTOMERS_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/products*.json' @STG_PRODUCTS_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/orders*.json' @STG_ORDERS_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/order_items*.json' @STG_ORDER_ITEMS_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/suppliers*.json' @STG_SUPPLIERS_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/inventory*.json' @STG_INVENTORY_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/warehouses*.json' @STG_WAREHOUSES_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/employees*.json' @STG_EMPLOYEES_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/sales_territories*.json' @STG_SALES_TERRITORIES_FILES;
PUT 'file:///Users/vthota/Documents/SE Learning/CursorAI/Snowpipe_DT_DEMOS/sample_data/promotions*.json' @STG_PROMOTIONS_FILES;
*/

SELECT 'Files uploaded successfully!' as STATUS;

-- =============================================================================
-- VERIFY UPLOADED FILES
-- =============================================================================

SELECT '=== VERIFYING UPLOADED FILES IN STAGES ===' as VERIFICATION;

-- List files in each stage to verify upload
LIST @STG_CUSTOMERS_FILES;
LIST @STG_PRODUCTS_FILES;
LIST @STG_ORDERS_FILES;
LIST @STG_ORDER_ITEMS_FILES;
LIST @STG_SUPPLIERS_FILES;

-- Quick verification of remaining stages
SELECT 'Additional stages contain files...' as INFO;
-- LIST @STG_INVENTORY_FILES;
-- LIST @STG_WAREHOUSES_FILES;
-- LIST @STG_EMPLOYEES_FILES;
-- LIST @STG_SALES_TERRITORIES_FILES;
-- LIST @STG_PROMOTIONS_FILES;

-- =============================================================================
-- MONITOR SNOWPIPE PROCESSING
-- =============================================================================

SELECT '=== MONITORING SNOWPIPE PROCESSING ===' as MONITORING;

-- Check if Snowpipe is processing the files (may take 1-3 minutes)
SELECT 
    'PIPE_CUSTOMERS' as PIPE_NAME,
    SYSTEM$PIPE_STATUS('SNOWPIPE_DT_DEMO.STAGE_DATA.PIPE_CUSTOMERS') as STATUS
UNION ALL
SELECT 
    'PIPE_PRODUCTS',
    SYSTEM$PIPE_STATUS('SNOWPIPE_DT_DEMO.STAGE_DATA.PIPE_PRODUCTS')
UNION ALL
SELECT 
    'PIPE_ORDERS',
    SYSTEM$PIPE_STATUS('SNOWPIPE_DT_DEMO.STAGE_DATA.PIPE_ORDERS')
UNION ALL
SELECT 
    'PIPE_ORDER_ITEMS',
    SYSTEM$PIPE_STATUS('SNOWPIPE_DT_DEMO.STAGE_DATA.PIPE_ORDER_ITEMS')
UNION ALL
SELECT 
    'PIPE_SUPPLIERS',
    SYSTEM$PIPE_STATUS('SNOWPIPE_DT_DEMO.STAGE_DATA.PIPE_SUPPLIERS');

-- =============================================================================
-- MANUAL SNOWPIPE REFRESH (if needed)
-- =============================================================================
-- Note: For internal stages, Snowpipe might need manual refresh
-- If auto-ingest doesn't work immediately, uncomment and run these:

/*
ALTER PIPE SNOWPIPE_DT_DEMO.STAGE_DATA.PIPE_CUSTOMERS REFRESH;
ALTER PIPE SNOWPIPE_DT_DEMO.STAGE_DATA.PIPE_PRODUCTS REFRESH;
ALTER PIPE SNOWPIPE_DT_DEMO.STAGE_DATA.PIPE_ORDERS REFRESH;
ALTER PIPE SNOWPIPE_DT_DEMO.STAGE_DATA.PIPE_ORDER_ITEMS REFRESH;
ALTER PIPE SNOWPIPE_DT_DEMO.STAGE_DATA.PIPE_SUPPLIERS REFRESH;
ALTER PIPE SNOWPIPE_DT_DEMO.STAGE_DATA.PIPE_INVENTORY REFRESH;
ALTER PIPE SNOWPIPE_DT_DEMO.STAGE_DATA.PIPE_WAREHOUSES REFRESH;
ALTER PIPE SNOWPIPE_DT_DEMO.STAGE_DATA.PIPE_EMPLOYEES REFRESH;
ALTER PIPE SNOWPIPE_DT_DEMO.STAGE_DATA.PIPE_TERRITORIES REFRESH;
ALTER PIPE SNOWPIPE_DT_DEMO.STAGE_DATA.PIPE_PROMOTIONS REFRESH;
*/

-- =============================================================================
-- DEMO WORKFLOW GUIDANCE
-- =============================================================================

SELECT '=== DEMO WORKFLOW GUIDANCE ===' as GUIDANCE;

SELECT 
    'PHASE 1: Upload initial files (this script)' as STEP_1,
    'PHASE 2: Wait 2-3 minutes for Snowpipe processing' as STEP_2,
    'PHASE 3: Create Streams & Tasks (@05_create_streams_and_tasks.sql)' as STEP_3,
    'PHASE 4: Upload update files (uncomment OPTION 2 above or use 06B script)' as STEP_4,
    'PHASE 5: Monitor pipeline (@07_demo_monitoring_validation.sql)' as STEP_5;

SELECT 'Initial file upload complete! Wait for Snowpipe, then create Streams & Tasks.' as NEXT_ACTION;