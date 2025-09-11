-- =============================================================================
-- JSON SCHEMA DETECTION AND TABLE CREATION
-- =============================================================================
-- This script demonstrates the proper use of INFER_SCHEMA for JSON schema detection
-- INFER_SCHEMA is used to create tables based on detected schema from JSON files
-- This should be run AFTER uploading JSON files to stages but BEFORE creating Snowpipe
-- =============================================================================

USE SCHEMA SNOWPIPE_DT_DEMO.STAGE_DATA;

-- =============================================================================
-- STEP 1: DEMONSTRATE INFER_SCHEMA FUNCTION
-- =============================================================================

-- First, let's see what INFER_SCHEMA detects from a JSON file
-- (This requires files to be uploaded to the stage first)

-- Example: Infer schema from customers JSON file
SELECT INFER_SCHEMA(
  LOCATION=>'@SNOWPIPE_DT_DEMO.DEMO_STAGES.STG_CUSTOMERS_FILES',
  FILE_FORMAT=>(
    TYPE=>'JSON'
    COMPRESSION=>'AUTO'
    STRIP_OUTER_ARRAY=>TRUE
    STRIP_NULL_VALUES=>FALSE
    REPLACE_INVALID_CHARACTERS=>TRUE
    DATE_FORMAT=>'AUTO'
    TIME_FORMAT=>'AUTO'
    TIMESTAMP_FORMAT=>'AUTO'
  )
);

-- =============================================================================
-- STEP 2: CREATE TABLES USING INFER_SCHEMA (Alternative Approach)
-- =============================================================================

-- Option 1: Create a new table using inferred schema
-- This creates a table with columns automatically detected from JSON
/*
CREATE TABLE STG_CUSTOMERS_AUTO 
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
        LOCATION=>'@SNOWPIPE_DT_DEMO.DEMO_STAGES.STG_CUSTOMERS_FILES',
        FILE_FORMAT=>(
            TYPE=>'JSON'
            COMPRESSION=>'AUTO'
            STRIP_OUTER_ARRAY=>TRUE
            STRIP_NULL_VALUES=>FALSE
            REPLACE_INVALID_CHARACTERS=>TRUE
            DATE_FORMAT=>'AUTO'
            TIME_FORMAT=>'AUTO'
            TIMESTAMP_FORMAT=>'AUTO'
        )
    ))
);
*/

-- =============================================================================
-- STEP 3: VALIDATE JSON SCHEMA DETECTION
-- =============================================================================

-- Show the structure of our existing tables vs what would be detected
DESCRIBE TABLE STG_CUSTOMERS;

-- Compare with what INFER_SCHEMA would suggest
-- (Run this after files are uploaded)
/*
SELECT 
    COLUMN_NAME,
    TYPE,
    NULLABLE,
    EXPRESSION
FROM TABLE(INFER_SCHEMA(
    LOCATION=>'@SNOWPIPE_DT_DEMO.DEMO_STAGES.STG_CUSTOMERS_FILES',
    FILE_FORMAT=>(TYPE=>'JSON', STRIP_OUTER_ARRAY=>TRUE)
));
*/

-- =============================================================================
-- STEP 4: GENERATE CREATE TABLE STATEMENTS
-- =============================================================================

-- Use GENERATE_COLUMN_DESCRIPTION to create table DDL from JSON
-- This is useful for generating table creation scripts automatically
/*
SELECT GENERATE_COLUMN_DESCRIPTION(
    ARRAY_AGG(OBJECT_CONSTRUCT(*)),
    'STG_CUSTOMERS_FROM_JSON'
) AS CREATE_TABLE_STATEMENT
FROM TABLE(INFER_SCHEMA(
    LOCATION=>'@SNOWPIPE_DT_DEMO.DEMO_STAGES.STG_CUSTOMERS_FILES',
    FILE_FORMAT=>(TYPE=>'JSON', STRIP_OUTER_ARRAY=>TRUE)
));
*/

-- =============================================================================
-- STEP 5: HANDLING NESTED JSON STRUCTURES
-- =============================================================================

-- For complex nested JSON, we might want to see how Snowflake handles it
-- This query shows how nested JSON objects would be flattened
/*
SELECT 
    COLUMN_NAME,
    TYPE,
    NULLABLE,
    EXPRESSION,
    CASE 
        WHEN COLUMN_NAME LIKE '%.%' THEN 'Nested field from JSON object'
        ELSE 'Top-level field'
    END AS FIELD_TYPE
FROM TABLE(INFER_SCHEMA(
    LOCATION=>'@SNOWPIPE_DT_DEMO.DEMO_STAGES.STG_CUSTOMERS_FILES',
    FILE_FORMAT=>(TYPE=>'JSON', STRIP_OUTER_ARRAY=>TRUE)
))
ORDER BY COLUMN_NAME;
*/

-- =============================================================================
-- STEP 6: MONITORING SCHEMA EVOLUTION
-- =============================================================================

-- To handle schema evolution, you can periodically check if new fields appear
-- This query would show differences between current table and JSON schema

/*
WITH current_columns AS (
    SELECT COLUMN_NAME, DATA_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'STAGE_DATA' 
    AND TABLE_NAME = 'STG_CUSTOMERS'
),
inferred_columns AS (
    SELECT COLUMN_NAME, TYPE as DATA_TYPE
    FROM TABLE(INFER_SCHEMA(
        LOCATION=>'@SNOWPIPE_DT_DEMO.DEMO_STAGES.STG_CUSTOMERS_FILES',
        FILE_FORMAT=>(TYPE=>'JSON', STRIP_OUTER_ARRAY=>TRUE)
    ))
)
SELECT 
    COALESCE(c.COLUMN_NAME, i.COLUMN_NAME) as COLUMN_NAME,
    c.DATA_TYPE as CURRENT_TYPE,
    i.DATA_TYPE as INFERRED_TYPE,
    CASE 
        WHEN c.COLUMN_NAME IS NULL THEN 'NEW_FIELD'
        WHEN i.COLUMN_NAME IS NULL THEN 'REMOVED_FIELD'  
        WHEN c.DATA_TYPE != i.DATA_TYPE THEN 'TYPE_CHANGED'
        ELSE 'UNCHANGED'
    END as STATUS
FROM current_columns c
FULL OUTER JOIN inferred_columns i ON c.COLUMN_NAME = i.COLUMN_NAME
WHERE COALESCE(c.COLUMN_NAME, i.COLUMN_NAME) NOT IN ('LOAD_TIMESTAMP')
ORDER BY STATUS, COLUMN_NAME;
*/

-- =============================================================================
-- IMPORTANT NOTES
-- =============================================================================

/*
🔍 KEY POINTS ABOUT INFER_SCHEMA:

1. INFER_SCHEMA is a TABLE FUNCTION, not a Snowpipe parameter
2. It's used to CREATE TABLES with automatically detected schema
3. It analyzes staged files and suggests column names and types
4. It handles JSON flattening automatically (nested objects become columns)
5. It's perfect for evolving schemas and unknown JSON structures

📋 WORKFLOW:
1. Upload JSON files to stages
2. Use INFER_SCHEMA to analyze the JSON structure
3. Create tables using the inferred schema (optional)
4. Create Snowpipe with MATCH_BY_COLUMN_NAME to load data
5. Monitor for schema changes over time

⚡ FOR THIS DEMO:
- We already have predefined tables (STG_CUSTOMERS, etc.)
- Snowpipe will use MATCH_BY_COLUMN_NAME to map JSON fields to columns
- This works great for known JSON structures
- Use INFER_SCHEMA when you have unknown or evolving JSON schemas

🚀 PRODUCTION TIP:
Use INFER_SCHEMA in a CI/CD pipeline to automatically detect schema changes
and alert when JSON files contain new fields that need table schema updates!
*/
