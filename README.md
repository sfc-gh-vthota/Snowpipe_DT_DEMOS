# 🏔️ Snowpipe + Streams + Tasks Demo

A complete demonstration of Snowflake's modern data pipeline using **Snowpipe** for JSON ingestion with schema detection, **Streams** for change data capture, and **Tasks** for intelligent data processing.

## 🎯 What This Demo Shows

**Business Scenario:** Customer addresses, product prices, and order statuses change frequently. You need:
- ✅ **Complete history** of all changes (audit trail)  
- ✅ **Current state** tables with only the latest version of each record
- ✅ **Automatic processing** of new data as it arrives
- ✅ **JSON schema detection** for complex, evolving data structures

**Solution:** Snowpipe → Streams → Tasks Pipeline

## 🏗️ Architecture Overview

```
JSON Files → Snowpipe → Stage Tables (History) → Streams → Tasks → Latest Tables (Current State)
     ↓           ↓            ↓                    ↓         ↓             ↓
  Complex    Schema      All Records           Change    MERGE        One Record
  Nested     Detection   Multiple Versions     Data      Logic       Per Entity
  JSON                   Per Customer          Capture   
```

**10 Business Entities:** Customers, Products, Orders, Order Items, Suppliers, Inventory, Warehouses, Employees, Territories, Promotions

## 📁 Demo Files Structure

### **Core Demo Scripts (Run in Order):**
1. `01_setup_database.sql` - Creates database and schemas
2. `02_create_stage_tables.sql` - Creates 10 stage tables for historical data  
3. `03_create_stages.sql` - Creates internal stages for JSON file uploads
4. `04_create_snowpipes.sql` - Creates Snowpipe objects with JSON schema detection
5. `generate_initial_data.py` - Generates 100 unique records per dataset (IDs 1-100, DATA_VERSION=1)
6. `generate_update_files.py` - **Enhanced**: 10 updates (existing IDs, higher versions) + 10 new inserts (IDs 101-110) per dataset
7. `06_demo_file_upload.sql` - **Pattern-based** upload of initial JSON files
8. `06B_upload_update_files.sql` - **Pattern-based** upload of update files
9. `05_create_streams_and_tasks.sql` - Creates streams and tasks (first 3 tables)
10. `05B_remaining_streams_tasks.sql` - Creates streams and tasks (remaining 7 tables)
11. `07_demo_monitoring_validation.sql` - Monitoring queries and validation

### **Utility Scripts:**
- `03B_json_schema_detection.sql` - Demonstrates INFER_SCHEMA() function
- `08_master_demo_script.sql` - Runs all scripts in sequence
- `00_complete_reset_restart.sql` - Complete reset for fresh start
- `force_reload_pipes.sql` - Manual Snowpipe refresh utility
- `sample_data/` - Generated JSON files (initial + timestamped updates)

## 🚀 Quick Start (5 Minutes)

### **Option 1: Step-by-Step Execution**
```sql
-- 1. Setup
@01_setup_database.sql
@02_create_stage_tables.sql  
@03_create_stages.sql
@04_create_snowpipes.sql

-- 2. Generate and load initial data
-- Run: python generate_initial_data.py
@06_demo_file_upload.sql

-- 2b. Generate and load update data (after streams/tasks are created)
-- Run: python generate_update_files.py
@06B_upload_update_files.sql

-- 3. Wait 2-3 minutes for Snowpipe, then create pipeline
@05_create_streams_and_tasks.sql
@05B_remaining_streams_tasks.sql

-- 4. Monitor and validate
@07_demo_monitoring_validation.sql
```

### **Option 2: Master Script (Automated)**
```sql
@08_master_demo_script.sql  -- Runs everything in sequence
```

### **Option 3: Fresh Restart (If Issues)**
```sql
@00_complete_reset_restart.sql  -- Complete cleanup
-- Then follow Option 1 steps above
```

## 🎯 Expected Results

After successful execution, you should see:

| Table Type | Purpose | Example Count | 
|------------|---------|---------------|
| `STG_CUSTOMERS` | All historical records | 120 records (100 initial + 20 updates) |
| `LATEST_CUSTOMERS` | Current state only | 110 unique customers (IDs 1-110, latest versions) |
| `STG_PRODUCTS` | All price changes | 120 records (100 initial + 20 updates) |
| `LATEST_PRODUCTS` | Current prices only | 110 unique products (IDs 1-110, latest versions) |

**Key Validation:**
- ✅ **Different counts:** Stage tables (120) > Latest tables (110) 
- ✅ **Processing timestamps:** `STREAM_PROCESSED_AT` populated
- ✅ **Latest versions:** Highest `DATA_VERSION` per entity in latest tables
- ✅ **Automatic execution:** Tasks run every minute
- ✅ **MERGE behavior:** Both UPDATE (existing IDs) and INSERT (new IDs 101-110) demonstrated

## 🔍 Key Features Demonstrated

### **1. JSON Schema Detection**
- Automatic inference of nested JSON structures
- Support for arrays, objects, and complex data types
- Evolution of schema over time (new fields added automatically)

### **2. Snowpipe Advanced Features**
- `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` for flexible field mapping
- JSON file format with `STRIP_OUTER_ARRAY = TRUE`
- Error handling with `ON_ERROR = 'CONTINUE'`

### **3. Streams for Change Data Capture**
- `SHOW_INITIAL_ROWS = TRUE` to include existing data
- `APPEND_ONLY = FALSE` for full DML tracking
- Real-time change detection on all stage tables

### **4. Tasks with Intelligent MERGE Logic**
- ROW_NUMBER() for handling duplicate versions
- WHEN clauses based on `DATA_VERSION` comparison  
- Automatic deduplication and latest record maintenance
- Scheduled execution every minute

### **5. Complete Monitoring**
- Snowpipe load history and status
- Stream data availability checks
- Task execution performance metrics
- Data validation and count comparisons

## 🛠️ Troubleshooting

### **Common Issues:**

**1. "Table does not exist" errors:**
```sql
-- Run setup scripts in correct order
@01_setup_database.sql
@02_create_stage_tables.sql
```

**2. "MERGE operations not working" (same counts in stage vs latest):**
```sql  
-- Use fresh restart approach
@00_complete_reset_restart.sql
-- Then follow normal setup sequence
```

**3. "No data in streams":**
```sql
-- Check if data loaded first, then create streams
SELECT COUNT(*) FROM STAGE_DATA.STG_CUSTOMERS;  -- Should be > 0
-- If 0, wait for Snowpipe or re-run file upload
```

**4. "Tasks not executing":**
```sql
-- Check task status
SHOW TASKS LIKE 'PROCESS_%_STREAM';
-- Resume if needed
ALTER TASK PROCESS_CUSTOMERS_STREAM RESUME;
```

## 📊 Sample Monitoring Queries

```sql
-- Quick validation
SELECT 
    (SELECT COUNT(*) FROM STAGE_DATA.STG_CUSTOMERS) as STAGE_COUNT,
    (SELECT COUNT(*) FROM LATEST_DATA.LATEST_CUSTOMERS) as LATEST_COUNT;
    
-- Check processing lag  
SELECT 
    MAX(LOAD_TIMESTAMP) as LAST_LOADED,
    MAX(STREAM_PROCESSED_AT) as LAST_PROCESSED,
    DATEDIFF('minutes', MAX(LOAD_TIMESTAMP), MAX(STREAM_PROCESSED_AT)) as LAG_MINUTES
FROM LATEST_DATA.LATEST_CUSTOMERS;

-- View data evolution
SELECT CUSTOMER_ID, ADDRESS, DATA_VERSION, RECORD_TIMESTAMP 
FROM STAGE_DATA.STG_CUSTOMERS 
WHERE CUSTOMER_ID = 1 
ORDER BY DATA_VERSION;
```

## 🎉 Demo Value Proposition

This demo showcases how Snowflake's **Snowpipe + Streams + Tasks** architecture provides:

1. **Zero-copy data sharing** between historical and current views
2. **Real-time processing** with minimal infrastructure complexity  
3. **Automatic schema evolution** for changing JSON structures
4. **Built-in monitoring** and operational visibility
5. **Cost-effective scaling** with serverless compute
6. **Enterprise-grade reliability** with automatic error handling

Perfect for demonstrating modern data engineering capabilities to customers who need both **complete audit trails** and **current state reporting** from continuously changing data sources.

---

**Total Demo Runtime:** ~5 minutes setup + continuous processing  
**Data Volume:** 10 tables, ~2000 total records with realistic relationships  
**Key Differentiators:** JSON schema detection, nested data handling, intelligent deduplication