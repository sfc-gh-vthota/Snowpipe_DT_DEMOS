-- =============================================================================
-- MASTER DEMO SCRIPT - SNOWPIPE + STREAMS + TASKS
-- =============================================================================
-- Complete walkthrough demonstrating Snowpipe with JSON schema detection,
-- Streams for change data capture, and Tasks for intelligent MERGE processing
--
-- This script provides a guided demo experience with explanations
-- =============================================================================

-- =============================================================================
-- DEMO INTRODUCTION
-- =============================================================================
/*
🎯 DEMO OBJECTIVES:
1. Show how Snowpipe automatically loads JSON data with schema detection
2. Demonstrate how Streams capture changes in real-time
3. Show how Tasks process changes with intelligent MERGE operations
4. Compare historical data (stage tables) vs current data (latest tables)
5. Prove the business value of this modern pipeline architecture

📊 THE BUSINESS SCENARIO:
An e-commerce company needs to:
- Track all changes to customer, product, and order data (for compliance/analysis)
- Provide fast access to current data (for applications)  
- Handle complex JSON schema changes automatically
- Process new data in real-time as it arrives

💡 THE SOLUTION:
- Snowpipe: Automatically loads ALL historical JSON records with schema detection
- Streams: Capture changes in real-time for processing
- Tasks: Automatically maintain LATEST records with intelligent MERGE logic
*/

SELECT '🚀 Starting Snowpipe + Streams + Tasks Demo...' as STATUS;

-- =============================================================================
-- STEP 1: VERIFY ENVIRONMENT SETUP
-- =============================================================================

SELECT '=== STEP 1: VERIFYING ENVIRONMENT ===' as DEMO_STEP;

-- Check if database and schemas exist
USE DATABASE SNOWPIPE_DT_DEMO;
SHOW SCHEMAS;

-- Verify stage tables exist
USE SCHEMA STAGE_DATA;
SHOW TABLES;

-- Verify latest data tables exist
USE SCHEMA LATEST_DATA;
SHOW TABLES;

-- Verify file stages exist  
USE SCHEMA DEMO_STAGES;
SHOW STAGES;

-- Verify Snowpipes exist
USE SCHEMA STAGE_DATA;
SHOW PIPES;

-- Verify Streams exist
SHOW STREAMS;

-- Verify Tasks exist
SHOW TASKS;

-- =============================================================================
-- STEP 2: CHECK CURRENT DATA STATUS
-- =============================================================================

SELECT '=== STEP 2: CHECKING DATA STATUS ===' as DEMO_STEP;

-- Check if data has been loaded into stage tables
SELECT 
    'STG_CUSTOMERS' as TABLE_NAME,
    COUNT(*) as TOTAL_RECORDS,
    COUNT(DISTINCT CUSTOMER_ID) as UNIQUE_ENTITIES,
    MIN(DATA_VERSION) as MIN_VERSION,
    MAX(DATA_VERSION) as MAX_VERSION,
    MAX(LOAD_TIMESTAMP) as LAST_LOADED
FROM STAGE_DATA.STG_CUSTOMERS

UNION ALL

SELECT 
    'STG_PRODUCTS',
    COUNT(*),
    COUNT(DISTINCT PRODUCT_ID),
    MIN(DATA_VERSION),
    MAX(DATA_VERSION),
    MAX(LOAD_TIMESTAMP)
FROM STAGE_DATA.STG_PRODUCTS

UNION ALL

SELECT 
    'STG_ORDERS',
    COUNT(*),
    COUNT(DISTINCT ORDER_ID),
    MIN(DATA_VERSION),
    MAX(DATA_VERSION),
    MAX(LOAD_TIMESTAMP)
FROM STAGE_DATA.STG_ORDERS;

-- Check Latest Tables status (maintained by Tasks)
SELECT 
    'LATEST_CUSTOMERS' as TABLE_NAME,
    COUNT(*) as CURRENT_RECORDS,
    MAX(STREAM_PROCESSED_AT) as LAST_PROCESSED
FROM LATEST_DATA.LATEST_CUSTOMERS

UNION ALL

SELECT 
    'LATEST_PRODUCTS',
    COUNT(*),
    MAX(STREAM_PROCESSED_AT)
FROM LATEST_DATA.LATEST_PRODUCTS

UNION ALL

SELECT 
    'LATEST_ORDERS',
    COUNT(*),
    MAX(STREAM_PROCESSED_AT)
FROM LATEST_DATA.LATEST_ORDERS;

-- =============================================================================
-- STEP 3: SNOWPIPE + JSON SCHEMA DETECTION DEMONSTRATION
-- =============================================================================

SELECT '=== STEP 3: SNOWPIPE & JSON SCHEMA DETECTION ===' as DEMO_STEP;

-- Show Snowpipe status and recent activity
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
    START_TIME=> DATEADD(hours, -48, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC
LIMIT 5;

-- Show how Snowpipe handled the JSON structure
DESCRIBE TABLE STAGE_DATA.STG_CUSTOMERS;

-- Show a sample of loaded JSON data with automatic schema detection
SELECT 
    CUSTOMER_ID,      -- Auto-detected from JSON
    CUSTOMER_NAME,    -- Auto-detected as STRING
    EMAIL,           -- Auto-detected as STRING
    ADDRESS,         -- Auto-detected as STRING
    RECORD_TIMESTAMP, -- Auto-detected as TIMESTAMP
    DATA_VERSION,    -- Auto-detected as NUMBER
    LOAD_TIMESTAMP   -- Default timestamp
FROM STAGE_DATA.STG_CUSTOMERS 
LIMIT 5;

-- =============================================================================
-- STEP 4: STREAMS DEMONSTRATION
-- =============================================================================

SELECT '=== STEP 4: STREAMS FOR CHANGE DATA CAPTURE ===' as DEMO_STEP;

-- Check if streams have data to process
SELECT 
    'STG_CUSTOMERS_STREAM' as STREAM_NAME,
    SYSTEM$STREAM_HAS_DATA('SNOWPIPE_DT_DEMO.STAGE_DATA.STG_CUSTOMERS_STREAM') as HAS_PENDING_DATA,
    CASE 
        WHEN SYSTEM$STREAM_HAS_DATA('SNOWPIPE_DT_DEMO.STAGE_DATA.STG_CUSTOMERS_STREAM') = 'true' 
        THEN 'Stream has data waiting to be processed'
        ELSE 'Stream is empty (good - means tasks are consuming data)'
    END as STATUS_INTERPRETATION

UNION ALL

SELECT 
    'STG_PRODUCTS_STREAM',
    SYSTEM$STREAM_HAS_DATA('SNOWPIPE_DT_DEMO.STAGE_DATA.STG_PRODUCTS_STREAM'),
    CASE 
        WHEN SYSTEM$STREAM_HAS_DATA('SNOWPIPE_DT_DEMO.STAGE_DATA.STG_PRODUCTS_STREAM') = 'true' 
        THEN 'Stream has data waiting to be processed'
        ELSE 'Stream is empty (good - means tasks are consuming data)'
    END;

-- =============================================================================
-- STEP 5: TASKS DEMONSTRATION
-- =============================================================================

SELECT '=== STEP 5: TASKS & MERGE PROCESSING ===' as DEMO_STEP;

-- Show Task execution history
SELECT 
    NAME as TASK_NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    DATEDIFF('seconds', SCHEDULED_TIME, COMPLETED_TIME) as RUNTIME_SECONDS,
    CASE 
        WHEN RETURN_VALUE = 'Task succeeded.' THEN '✅ SUCCESS'
        ELSE '❌ ' || COALESCE(ERROR_MESSAGE, 'UNKNOWN ERROR')
    END as EXECUTION_STATUS
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME LIKE 'PROCESS_%_STREAM'
  AND SCHEDULED_TIME >= DATEADD('hours', -2, CURRENT_TIMESTAMP())
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;

-- =============================================================================
-- STEP 6: HISTORICAL DATA DEMONSTRATION
-- =============================================================================

SELECT '=== STEP 6: HISTORICAL DATA (COMPLETE AUDIT TRAIL) ===' as DEMO_STEP;

-- Show how Customer 1's address changed over time (ALL VERSIONS)
SELECT 
    '👤 Customer Address History - All Versions' as DEMO_TYPE,
    CUSTOMER_ID,
    CUSTOMER_NAME,
    ADDRESS,
    CITY,
    STATE,
    'Version ' || DATA_VERSION as VERSION,
    RECORD_TIMESTAMP,
    'STAGE TABLE' as SOURCE
FROM STAGE_DATA.STG_CUSTOMERS
WHERE CUSTOMER_ID = 1
ORDER BY DATA_VERSION;

-- Show how Product 1's price changed over time (ALL VERSIONS)
SELECT 
    '💰 Product Price History - All Versions' as DEMO_TYPE,
    PRODUCT_ID,
    PRODUCT_NAME,
    '$' || PRICE as PRICE,
    'Version ' || DATA_VERSION as VERSION,
    RECORD_TIMESTAMP,
    'STAGE TABLE' as SOURCE
FROM STAGE_DATA.STG_PRODUCTS
WHERE PRODUCT_ID = 1
ORDER BY DATA_VERSION;

-- =============================================================================
-- STEP 7: LATEST DATA DEMONSTRATION (MAINTAINED BY TASKS)
-- =============================================================================

SELECT '=== STEP 7: LATEST DATA (MAINTAINED BY TASKS) ===' as DEMO_STEP;

-- Show ONLY the latest records (processed by Tasks)
SELECT 
    '👤 Customer Latest Address - Current Only' as DEMO_TYPE,
    CUSTOMER_ID,
    CUSTOMER_NAME,
    ADDRESS,
    CITY,
    STATE,
    'Version ' || DATA_VERSION as FINAL_VERSION,
    RECORD_TIMESTAMP,
    STREAM_PROCESSED_AT,
    'LATEST TABLE' as SOURCE
FROM LATEST_DATA.LATEST_CUSTOMERS
WHERE CUSTOMER_ID = 1;

SELECT 
    '💰 Product Current Price - Current Only' as DEMO_TYPE,
    PRODUCT_ID,
    PRODUCT_NAME,
    '$' || PRICE as CURRENT_PRICE,
    'Version ' || DATA_VERSION as FINAL_VERSION,
    RECORD_TIMESTAMP,
    STREAM_PROCESSED_AT,
    'LATEST TABLE' as SOURCE
FROM LATEST_DATA.LATEST_PRODUCTS
WHERE PRODUCT_ID = 1;

-- =============================================================================
-- STEP 8: SIDE-BY-SIDE COMPARISON
-- =============================================================================

SELECT '=== STEP 8: HISTORICAL vs LATEST COMPARISON ===' as DEMO_STEP;

-- Compare record counts: Historical vs Latest
WITH comparison AS (
    SELECT 
        'Historical Records (Stage Tables)' as DATA_TYPE,
        'All versions of data over time' as DESCRIPTION,
        (SELECT COUNT(*) FROM STAGE_DATA.STG_CUSTOMERS) as CUSTOMERS,
        (SELECT COUNT(*) FROM STAGE_DATA.STG_PRODUCTS) as PRODUCTS,
        (SELECT COUNT(*) FROM STAGE_DATA.STG_ORDERS) as ORDERS
    
    UNION ALL
    
    SELECT 
        'Latest Records (Task-Maintained Tables)' as DATA_TYPE,
        'Only current version of each entity' as DESCRIPTION,
        (SELECT COUNT(*) FROM LATEST_DATA.LATEST_CUSTOMERS) as CUSTOMERS,
        (SELECT COUNT(*) FROM LATEST_DATA.LATEST_PRODUCTS) as PRODUCTS,
        (SELECT COUNT(*) FROM LATEST_DATA.LATEST_ORDERS) as ORDERS
)
SELECT * FROM comparison;

-- Show processing lag analysis
SELECT 
    'CUSTOMERS' as TABLE_NAME,
    COUNT(*) as RECORDS_PROCESSED,
    MAX(LOAD_TIMESTAMP) as LAST_DATA_LOADED,
    MAX(STREAM_PROCESSED_AT) as LAST_TASK_PROCESSED,
    DATEDIFF('minutes', MAX(LOAD_TIMESTAMP), MAX(STREAM_PROCESSED_AT)) as PROCESSING_LAG_MINUTES,
    CASE 
        WHEN DATEDIFF('minutes', MAX(LOAD_TIMESTAMP), MAX(STREAM_PROCESSED_AT)) < 5 
        THEN '🟢 EXCELLENT'
        WHEN DATEDIFF('minutes', MAX(LOAD_TIMESTAMP), MAX(STREAM_PROCESSED_AT)) < 15 
        THEN '🟡 GOOD' 
        ELSE '🔴 REVIEW NEEDED'
    END as PERFORMANCE_STATUS
FROM LATEST_DATA.LATEST_CUSTOMERS;

-- =============================================================================
-- STEP 9: BUSINESS VALUE DEMONSTRATION
-- =============================================================================

SELECT '=== STEP 9: BUSINESS VALUE DEMONSTRATION ===' as DEMO_STEP;

-- Use Case 1: Audit Trail (Historical Data)
SELECT 
    '🔍 AUDIT TRAIL: Customer Address Changes' as USE_CASE,
    CUSTOMER_ID,
    CUSTOMER_NAME,
    ADDRESS as OLD_ADDRESS,
    LEAD(ADDRESS) OVER (PARTITION BY CUSTOMER_ID ORDER BY DATA_VERSION) as NEW_ADDRESS,
    RECORD_TIMESTAMP as CHANGE_DATE,
    CASE 
        WHEN LEAD(ADDRESS) OVER (PARTITION BY CUSTOMER_ID ORDER BY DATA_VERSION) IS NULL 
        THEN 'CURRENT'
        ELSE 'CHANGED'
    END as STATUS
FROM STAGE_DATA.STG_CUSTOMERS
WHERE CUSTOMER_ID = 2  -- Customer with multiple address changes
ORDER BY DATA_VERSION;

-- Use Case 2: Fast Application Queries (Latest Data)
SELECT 
    '⚡ FAST QUERIES: Current Customer Information' as USE_CASE,
    COUNT(*) as RECORDS_TO_SCAN,
    'Optimized for application performance - no window functions needed' as BENEFIT
FROM LATEST_DATA.LATEST_CUSTOMERS;

-- Use Case 3: Real-time Processing Performance
SELECT 
    '🔄 REAL-TIME PROCESSING' as USE_CASE,
    COUNT(*) as TOTAL_CUSTOMERS,
    COUNT(DISTINCT CASE WHEN STREAM_PROCESSED_AT IS NOT NULL THEN CUSTOMER_ID END) as PROCESSED_BY_TASKS,
    ROUND(COUNT(DISTINCT CASE WHEN STREAM_PROCESSED_AT IS NOT NULL THEN CUSTOMER_ID END) / COUNT(*) * 100, 2) as PROCESSING_PERCENTAGE
FROM LATEST_DATA.LATEST_CUSTOMERS;

-- =============================================================================
-- STEP 10: DEMO CONCLUSION AND ARCHITECTURE SUMMARY
-- =============================================================================

SELECT '=== STEP 10: DEMO CONCLUSION ===' as DEMO_STEP;

SELECT 
    '✅ Snowpipe with JSON Schema Detection' as FEATURE,
    'Automatically loaded ' || (SELECT COUNT(*) FROM STAGE_DATA.STG_CUSTOMERS) || 
    ' customer records across ' || (SELECT COUNT(DISTINCT DATA_VERSION) FROM STAGE_DATA.STG_CUSTOMERS) || ' versions' as RESULT

UNION ALL

SELECT 
    '✅ Streams for Change Data Capture',
    'Real-time tracking of ' || (SELECT COUNT(*) FROM SHOW STREAMS) || ' streams for automatic processing'

UNION ALL

SELECT 
    '✅ Tasks for Intelligent MERGE Processing',
    'Maintaining ' || (SELECT COUNT(*) FROM LATEST_DATA.LATEST_CUSTOMERS) || 
    ' current customer records with automatic deduplication'

UNION ALL

SELECT 
    '✅ JSON Schema Evolution Support',
    'Handled complex nested JSON structures without manual schema definition'

UNION ALL

SELECT 
    '✅ Complete Audit Trail + Current State',
    'Historical changes preserved for compliance, current state optimized for performance';

-- Architecture Summary
SELECT '🏗️ ARCHITECTURE: JSON Files → Snowpipe → Stage Tables → Streams → Tasks → Latest Tables' as PIPELINE_FLOW;

-- Final summary
SELECT 
    '🎉 DEMO COMPLETE!' as STATUS,
    'Snowpipe + Streams + Tasks provide a complete solution for real-time data processing with full historical preservation' as CONCLUSION;

-- =============================================================================
-- NEXT STEPS FOR PRODUCTION
-- =============================================================================

/*
🚀 TO IMPLEMENT IN PRODUCTION:

1. EXTERNAL STAGES: Use S3/Azure/GCS with event notifications for auto-triggering
2. ERROR HANDLING: Implement comprehensive error handling and alerting for all components
3. MONITORING: Set up dashboards for Snowpipe, Stream, and Task monitoring  
4. SCALING: Adjust warehouse sizes, task schedules, and clustering as needed
5. SECURITY: Implement proper access controls, data masking, and encryption
6. TESTING: Add data quality checks and validation procedures at each stage

💡 BUSINESS VALUE ACHIEVED:
- Reduced ETL complexity and maintenance (serverless processing)
- Automatic JSON schema evolution handling  
- Real-time data processing with sub-minute latency
- Fast queries for applications (Latest Tables)
- Complete audit trail (Stage Tables)
- Cost optimization through automated scaling

📊 METRICS TO TRACK:
- Snowpipe load times and success rates
- Stream lag and processing efficiency
- Task execution performance and error rates
- Query response times (stage vs latest tables)
- Storage usage optimization
- Cost per TB processed

🔧 OPERATIONAL MONITORING:
Use the monitoring script (07_demo_monitoring_validation.sql) for:
- Pipeline health checks
- Performance optimization
- Data quality validation
- Cost monitoring and optimization
*/

-- End of Master Demo Script
SELECT 'Demo Complete! Use 07_demo_monitoring_validation.sql for ongoing monitoring.' as FINAL_MESSAGE;