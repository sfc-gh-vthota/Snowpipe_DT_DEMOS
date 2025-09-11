-- =============================================================================
-- DEMO MONITORING AND VALIDATION QUERIES - STREAMS & TASKS EDITION
-- =============================================================================
-- These queries demonstrate the key differences between:
-- - Stage tables (complete historical data)
-- - Latest data tables (maintained by Streams & Tasks)
-- And shows monitoring of the entire Snowpipe → Streams → Tasks pipeline
-- =============================================================================

-- PREREQUISITES: 
-- 1. All setup scripts (01-05B) have been run successfully
-- 2. JSON files have been uploaded and processed by Snowpipe
-- 3. Streams are capturing changes and Tasks are processing them
-- 4. Latest data tables are being maintained by the Tasks

-- Set context
USE DATABASE SNOWPIPE_DT_DEMO;
USE SCHEMA SNOWPIPE_DT_DEMO.STAGE_DATA;

-- =============================================================================
-- SECTION 1: MONITORING SNOWPIPE ACTIVITY
-- =============================================================================

-- Check Snowpipe status for all pipes
SELECT 
    'CUSTOMERS' as TABLE_NAME,
    PARSE_JSON(SYSTEM$PIPE_STATUS('STAGE_DATA.PIPE_CUSTOMERS')) as PIPE_STATUS
UNION ALL
SELECT 
    'PRODUCTS',
    PARSE_JSON(SYSTEM$PIPE_STATUS('STAGE_DATA.PIPE_PRODUCTS'))
UNION ALL
SELECT 
    'ORDERS',
    PARSE_JSON(SYSTEM$PIPE_STATUS('STAGE_DATA.PIPE_ORDERS'))
UNION ALL
SELECT 
    'ORDER_ITEMS',
    PARSE_JSON(SYSTEM$PIPE_STATUS('STAGE_DATA.PIPE_ORDER_ITEMS'))
UNION ALL
SELECT 
    'SUPPLIERS',
    PARSE_JSON(SYSTEM$PIPE_STATUS('STAGE_DATA.PIPE_SUPPLIERS'));

-- View Snowpipe load history
SELECT 
    PIPE_NAME,
    FILE_NAME,
    ROW_COUNT,
    ROW_PARSED,
    ERROR_COUNT,
    ERROR_LIMIT,
    STATUS,
    LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME=>'SNOWPIPE_DT_DEMO.STAGE_DATA.STG_CUSTOMERS', 
    START_TIME=> DATEADD(hours, -24, CURRENT_TIMESTAMP())
));

-- =============================================================================
-- SECTION 2: STREAMS AND TASKS MONITORING
-- =============================================================================

-- Check Stream status
SELECT 
    'STG_CUSTOMERS_STREAM' as STREAM_NAME,
    SYSTEM$STREAM_HAS_DATA('SNOWPIPE_DT_DEMO.STAGE_DATA.STG_CUSTOMERS_STREAM') as HAS_DATA
UNION ALL
SELECT 
    'STG_PRODUCTS_STREAM',
    SYSTEM$STREAM_HAS_DATA('SNOWPIPE_DT_DEMO.STAGE_DATA.STG_PRODUCTS_STREAM')
UNION ALL
SELECT 
    'STG_ORDERS_STREAM',
    SYSTEM$STREAM_HAS_DATA('SNOWPIPE_DT_DEMO.STAGE_DATA.STG_ORDERS_STREAM')
UNION ALL
SELECT 
    'STG_ORDER_ITEMS_STREAM',
    SYSTEM$STREAM_HAS_DATA('SNOWPIPE_DT_DEMO.STAGE_DATA.STG_ORDER_ITEMS_STREAM')
UNION ALL
SELECT 
    'STG_SUPPLIERS_STREAM',
    SYSTEM$STREAM_HAS_DATA('SNOWPIPE_DT_DEMO.STAGE_DATA.STG_SUPPLIERS_STREAM');

-- View Task execution history
SELECT 
    NAME as TASK_NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    DATEDIFF('seconds', SCHEDULED_TIME, COMPLETED_TIME) as RUNTIME_SECONDS,
    RETURN_VALUE,
    ERROR_CODE,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME LIKE 'PROCESS_%_STREAM'
ORDER BY SCHEDULED_TIME DESC
LIMIT 20;

-- Check Task status
SHOW TASKS LIKE 'PROCESS_%_STREAM';

-- =============================================================================
-- SECTION 3: DATA VALIDATION - HISTORICAL VS LATEST
-- =============================================================================

-- CUSTOMERS EXAMPLE: Show historical changes vs latest record
SELECT '=== CUSTOMERS: HISTORICAL DATA (Stage Table) ===' as SECTION;

SELECT 
    CUSTOMER_ID,
    CUSTOMER_NAME,
    ADDRESS,
    CITY,
    STATE,
    DATA_VERSION,
    RECORD_TIMESTAMP
FROM STAGE_DATA.STG_CUSTOMERS 
WHERE CUSTOMER_ID IN (1, 2, 3, 4, 5)  -- Show first 5 customers
ORDER BY CUSTOMER_ID, DATA_VERSION;

SELECT '=== CUSTOMERS: LATEST DATA ONLY (Tasks-Maintained Table) ===' as SECTION;

SELECT 
    CUSTOMER_ID,
    CUSTOMER_NAME,
    ADDRESS,
    CITY,
    STATE,
    DATA_VERSION,
    RECORD_TIMESTAMP
FROM LATEST_DATA.LATEST_CUSTOMERS 
WHERE CUSTOMER_ID IN (1, 2, 3, 4, 5)  -- Same customers
ORDER BY CUSTOMER_ID;

-- PRODUCTS EXAMPLE: Show price changes over time
SELECT '=== PRODUCTS: PRICE HISTORY (Stage Table) ===' as SECTION;

SELECT 
    PRODUCT_ID,
    PRODUCT_NAME,
    PRICE,
    DATA_VERSION,
    RECORD_TIMESTAMP
FROM STAGE_DATA.STG_PRODUCTS 
WHERE PRODUCT_ID IN (1, 2, 3)  -- Show first 3 products
ORDER BY PRODUCT_ID, DATA_VERSION;

SELECT '=== PRODUCTS: CURRENT PRICES ONLY (Tasks-Maintained Table) ===' as SECTION;

SELECT 
    PRODUCT_ID,
    PRODUCT_NAME,
    PRICE,
    DATA_VERSION,
    RECORD_TIMESTAMP
FROM LATEST_DATA.LATEST_PRODUCTS 
WHERE PRODUCT_ID IN (1, 2, 3)  -- Same products
ORDER BY PRODUCT_ID;

-- ORDERS EXAMPLE: Show order status progression
SELECT '=== ORDERS: STATUS HISTORY (Stage Table) ===' as SECTION;

SELECT 
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_STATUS,
    ORDER_TOTAL,
    DATA_VERSION,
    RECORD_TIMESTAMP
FROM STAGE_DATA.STG_ORDERS 
WHERE ORDER_ID IN (1, 2, 3)  -- Show first 3 orders
ORDER BY ORDER_ID, DATA_VERSION;

SELECT '=== ORDERS: CURRENT STATUS ONLY (Tasks-Maintained Table) ===' as SECTION;

SELECT 
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_STATUS,
    ORDER_TOTAL,
    DATA_VERSION,
    RECORD_TIMESTAMP
FROM LATEST_DATA.LATEST_ORDERS 
WHERE ORDER_ID IN (1, 2, 3)  -- Same orders
ORDER BY ORDER_ID;

-- =============================================================================
-- SECTION 4: SUMMARY STATISTICS
-- =============================================================================

-- Count comparison: Historical vs Latest records
SELECT 
    'Stage Tables (All Historical Records)' as DATA_TYPE,
    (SELECT COUNT(*) FROM STAGE_DATA.STG_CUSTOMERS) as CUSTOMERS,
    (SELECT COUNT(*) FROM STAGE_DATA.STG_PRODUCTS) as PRODUCTS,
    (SELECT COUNT(*) FROM STAGE_DATA.STG_ORDERS) as ORDERS,
    (SELECT COUNT(*) FROM STAGE_DATA.STG_SUPPLIERS) as SUPPLIERS

UNION ALL

SELECT 
    'Latest Data Tables (Maintained by Tasks)' as DATA_TYPE,
    (SELECT COUNT(*) FROM LATEST_DATA.LATEST_CUSTOMERS) as CUSTOMERS,
    (SELECT COUNT(*) FROM LATEST_DATA.LATEST_PRODUCTS) as PRODUCTS,
    (SELECT COUNT(*) FROM LATEST_DATA.LATEST_ORDERS) as ORDERS,
    (SELECT COUNT(*) FROM LATEST_DATA.LATEST_SUPPLIERS) as SUPPLIERS;

-- Show version distribution in stage tables
SELECT 
    'CUSTOMERS' as TABLE_NAME,
    DATA_VERSION,
    COUNT(*) as RECORD_COUNT
FROM STAGE_DATA.STG_CUSTOMERS 
GROUP BY DATA_VERSION
ORDER BY DATA_VERSION

UNION ALL

SELECT 
    'PRODUCTS',
    DATA_VERSION,
    COUNT(*)
FROM STAGE_DATA.STG_PRODUCTS 
GROUP BY DATA_VERSION
ORDER BY DATA_VERSION

UNION ALL

SELECT 
    'ORDERS',
    DATA_VERSION,
    COUNT(*)
FROM STAGE_DATA.STG_ORDERS 
GROUP BY DATA_VERSION
ORDER BY DATA_VERSION;

-- =============================================================================
-- SECTION 5: DEMONSTRATION QUERIES
-- =============================================================================

-- Show a customer's address changes over time
SELECT '=== CUSTOMER ADDRESS CHANGE DEMONSTRATION ===' as DEMO;

WITH customer_changes AS (
    SELECT 
        CUSTOMER_ID,
        CUSTOMER_NAME,
        ADDRESS,
        CITY,
        STATE,
        DATA_VERSION,
        RECORD_TIMESTAMP,
        LAG(ADDRESS) OVER (PARTITION BY CUSTOMER_ID ORDER BY DATA_VERSION) as PREVIOUS_ADDRESS
    FROM STAGE_DATA.STG_CUSTOMERS
    WHERE CUSTOMER_ID = 1
)
SELECT 
    CUSTOMER_ID,
    CUSTOMER_NAME,
    'Version ' || DATA_VERSION as VERSION,
    CASE 
        WHEN PREVIOUS_ADDRESS IS NULL THEN 'Initial Address: ' || ADDRESS
        ELSE 'Changed from: ' || PREVIOUS_ADDRESS || ' to: ' || ADDRESS
    END as ADDRESS_CHANGE,
    RECORD_TIMESTAMP
FROM customer_changes
ORDER BY DATA_VERSION;

-- Show current address from Tasks-maintained Latest Table
SELECT 
    CUSTOMER_ID,
    CUSTOMER_NAME,
    'Current Address: ' || ADDRESS as CURRENT_ADDRESS,
    'Version ' || DATA_VERSION as FINAL_VERSION,
    RECORD_TIMESTAMP
FROM LATEST_DATA.LATEST_CUSTOMERS
WHERE CUSTOMER_ID = 1;

-- Show product price evolution
SELECT '=== PRODUCT PRICE EVOLUTION DEMONSTRATION ===' as DEMO;

SELECT 
    PRODUCT_ID,
    PRODUCT_NAME,
    'Version ' || DATA_VERSION as VERSION,
    '$' || PRICE as PRICE,
    RECORD_TIMESTAMP
FROM STAGE_DATA.STG_PRODUCTS
WHERE PRODUCT_ID = 1
ORDER BY DATA_VERSION;

-- Show current price from Tasks-maintained Latest Table
SELECT 
    PRODUCT_ID,
    PRODUCT_NAME,
    'Current Price: $' || PRICE as CURRENT_PRICE,
    'Version ' || DATA_VERSION as FINAL_VERSION,
    RECORD_TIMESTAMP,
    STREAM_PROCESSED_AT
FROM LATEST_DATA.LATEST_PRODUCTS
WHERE PRODUCT_ID = 1;

-- =============================================================================
-- SECTION 6: STREAMS & TASKS SPECIFIC MONITORING
-- =============================================================================

SELECT '=== STREAMS & TASKS PROCESSING ANALYSIS ===' as DEMO;

-- Show processing lag between data load and task processing
SELECT 
    'CUSTOMERS' as TABLE_NAME,
    COUNT(*) as RECORDS_PROCESSED,
    MAX(LOAD_TIMESTAMP) as LAST_DATA_LOADED,
    MAX(STREAM_PROCESSED_AT) as LAST_TASK_PROCESSED,
    DATEDIFF('minutes', MAX(LOAD_TIMESTAMP), MAX(STREAM_PROCESSED_AT)) as PROCESSING_LAG_MINUTES
FROM LATEST_DATA.LATEST_CUSTOMERS

UNION ALL

SELECT 
    'PRODUCTS',
    COUNT(*),
    MAX(LOAD_TIMESTAMP),
    MAX(STREAM_PROCESSED_AT),
    DATEDIFF('minutes', MAX(LOAD_TIMESTAMP), MAX(STREAM_PROCESSED_AT))
FROM LATEST_DATA.LATEST_PRODUCTS

UNION ALL

SELECT 
    'ORDERS',
    COUNT(*),
    MAX(LOAD_TIMESTAMP),
    MAX(STREAM_PROCESSED_AT),
    DATEDIFF('minutes', MAX(LOAD_TIMESTAMP), MAX(STREAM_PROCESSED_AT))
FROM LATEST_DATA.LATEST_ORDERS;

-- Show task execution performance
SELECT 
    'Recent Task Performance' as ANALYSIS,
    NAME as TASK_NAME,
    RUNTIME_SECONDS,
    SCHEDULED_TIME,
    CASE 
        WHEN RUNTIME_SECONDS < 30 THEN '🟢 FAST'
        WHEN RUNTIME_SECONDS < 60 THEN '🟡 MODERATE' 
        ELSE '🔴 SLOW'
    END as PERFORMANCE_STATUS
FROM (
    SELECT 
        NAME,
        DATEDIFF('seconds', SCHEDULED_TIME, COMPLETED_TIME) as RUNTIME_SECONDS,
        SCHEDULED_TIME,
        ROW_NUMBER() OVER (PARTITION BY NAME ORDER BY SCHEDULED_TIME DESC) as rn
    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
    WHERE NAME LIKE 'PROCESS_%_STREAM'
      AND STATE = 'SUCCEEDED'
) 
WHERE rn = 1
ORDER BY TASK_NAME;
