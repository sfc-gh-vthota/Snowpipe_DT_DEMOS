-- =============================================================================
-- UPLOAD INCREMENTAL UPDATE FILES TO SNOWFLAKE STAGES
-- =============================================================================
-- This script uploads the timestamped update files to demonstrate:
-- 1. Initial load processed by Snowpipe
-- 2. Later update files trigger Streams to capture changes
-- 3. Tasks process the stream data with MERGE operations
-- 
-- Run this AFTER the initial data has been loaded and processed
-- =============================================================================

USE DATABASE SNOWPIPE_DT_DEMO;
USE SCHEMA DEMO_STAGES;

SELECT '=== UPLOADING INCREMENTAL UPDATE FILES ===' as STATUS;

-- Upload all timestamped update files using patterns (works with any timestamp)
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

SELECT 'Update files uploaded successfully!' as STATUS;

-- Check uploaded files in each stage
SELECT '=== VERIFYING UPDATE FILES IN STAGES ===' as STATUS;

LIST @STG_CUSTOMERS_FILES;
LIST @STG_PRODUCTS_FILES;
LIST @STG_ORDERS_FILES;

SELECT 'Update files are now available for Snowpipe processing!' as STATUS;

-- =============================================================================
-- MONITORING AFTER UPDATE UPLOAD
-- =============================================================================

SELECT '=== MONITORING SNOWPIPE AFTER UPDATE FILES ===' as STATUS;

-- Monitor Snowpipe activity for the new files
SELECT 
    PIPE_NAME,
    FILE_NAME,
    ROW_COUNT,
    ROW_PARSED,
    ERROR_COUNT,
    STATUS,
    LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME=>'SNOWPIPE_DT_DEMO.STAGE_DATA.STG_CUSTOMERS', 
    START_TIME=> DATEADD(hours, -2, CURRENT_TIMESTAMP())
))
WHERE FILE_NAME LIKE '%update%'
ORDER BY LAST_LOAD_TIME DESC
LIMIT 10;

-- Check stream status after update files are processed
USE SCHEMA STAGE_DATA;

SELECT 'Checking if streams detected new data after update files...' as STATUS;

SELECT 
    'STG_CUSTOMERS_STREAM' as STREAM_NAME,
    SYSTEM$STREAM_HAS_DATA('SNOWPIPE_DT_DEMO.STAGE_DATA.STG_CUSTOMERS_STREAM') as HAS_DATA,
    CASE 
        WHEN SYSTEM$STREAM_HAS_DATA('SNOWPIPE_DT_DEMO.STAGE_DATA.STG_CUSTOMERS_STREAM') = 'true' 
        THEN 'Stream detected new data - Tasks should process soon'
        ELSE 'No new data detected yet - wait for Snowpipe processing'
    END as INTERPRETATION

UNION ALL

SELECT 
    'STG_PRODUCTS_STREAM',
    SYSTEM$STREAM_HAS_DATA('SNOWPIPE_DT_DEMO.STAGE_DATA.STG_PRODUCTS_STREAM'),
    CASE 
        WHEN SYSTEM$STREAM_HAS_DATA('SNOWPIPE_DT_DEMO.STAGE_DATA.STG_PRODUCTS_STREAM') = 'true' 
        THEN 'Stream detected new data - Tasks should process soon'
        ELSE 'No new data detected yet - wait for Snowpipe processing'
    END

UNION ALL

SELECT 
    'STG_ORDERS_STREAM',
    SYSTEM$STREAM_HAS_DATA('SNOWPIPE_DT_DEMO.STAGE_DATA.STG_ORDERS_STREAM'),
    CASE 
        WHEN SYSTEM$STREAM_HAS_DATA('SNOWPIPE_DT_DEMO.STAGE_DATA.STG_ORDERS_STREAM') = 'true' 
        THEN 'Stream detected new data - Tasks should process soon'
        ELSE 'No new data detected yet - wait for Snowpipe processing'
    END;

SELECT '=== EXPECTED DEMO FLOW AFTER UPDATE FILES ===' as DEMO_FLOW;

SELECT 
    '1. Snowpipe processes update files (new DATA_VERSIONs)' as STEP_1,
    '2. Streams capture the new records as changes' as STEP_2,
    '3. Tasks execute MERGE operations every minute' as STEP_3,
    '4. Latest tables get updated with highest DATA_VERSION' as STEP_4,
    '5. Stage tables contain ALL versions (history preserved)' as STEP_5;

SELECT 'Monitor with @07_demo_monitoring_validation.sql in 2-3 minutes!' as NEXT_ACTION;
