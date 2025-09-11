-- =============================================================================
-- FORCE RELOAD PIPES - USE WHEN NEEDED FOR DEMO TROUBLESHOOTING
-- =============================================================================
-- This script forces all Snowpipes to reload their files
-- Use this when:
-- 1. Files were uploaded but not processed (e.g., during demo setup)
-- 2. You need to reload previously processed files
-- 3. Troubleshooting data loading issues
-- 
-- NOTE: FORCE parameter is NOT valid in pipe definitions!
-- The correct approach is using ALTER PIPE ... REFRESH
-- =============================================================================

USE SCHEMA SNOWPIPE_DT_DEMO.STAGE_DATA;

SELECT '=== FORCING RELOAD OF ALL SNOWPIPES ===' as ACTION;

-- Force refresh all pipes to reload files
ALTER PIPE PIPE_CUSTOMERS REFRESH;
ALTER PIPE PIPE_PRODUCTS REFRESH;
ALTER PIPE PIPE_ORDERS REFRESH;
ALTER PIPE PIPE_ORDER_ITEMS REFRESH;
ALTER PIPE PIPE_SUPPLIERS REFRESH;
ALTER PIPE PIPE_INVENTORY REFRESH;
ALTER PIPE PIPE_WAREHOUSES REFRESH;
ALTER PIPE PIPE_EMPLOYEES REFRESH;
ALTER PIPE PIPE_TERRITORIES REFRESH;
ALTER PIPE PIPE_PROMOTIONS REFRESH;

SELECT 'All pipes refreshed - check status below' as STATUS;

-- Check pipe status after refresh
SHOW PIPES IN SCHEMA SNOWPIPE_DT_DEMO.STAGE_DATA;

-- Monitor pipe activity
SELECT 
    PIPE_NAME,
    IS_AUTOINGEST_ENABLED,
    PIPE_STATUS,
    LAST_RECEIVED_MESSAGE_TIMESTAMP,
    LAST_FORWARDED_MESSAGE_TIMESTAMP
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE PIPE_NAME LIKE 'PIPE_%';

-- Check recent copy history to verify files are being processed
SELECT 
    PIPE_NAME,
    FILE_NAME,
    ROW_COUNT,
    STATUS,
    LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME=>'SNOWPIPE_DT_DEMO.STAGE_DATA.STG_CUSTOMERS', 
    START_TIME=> DATEADD(hours, -1, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC
LIMIT 5;

SELECT '✅ Force reload complete! Monitor the copy history above to see results.' as FINAL_STATUS;
