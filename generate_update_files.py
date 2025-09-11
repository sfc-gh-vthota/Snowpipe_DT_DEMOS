#!/usr/bin/env python3
"""
Enhanced Incremental Update Files Generator for Snowpipe + Streams + Tasks Demo

This script generates SEPARATE update files with timestamp naming to demonstrate:
1. Initial load: customers.json (100 records, ID 1-100, DATA_VERSION = 1)
2. Later updates: customers_update_timestamp.json containing:
   - 10 UPDATE records: Existing IDs (1-100) with higher DATA_VERSION (2-4)
   - 10 INSERT records: New IDs (101-110) with DATA_VERSION = 1

This showcases both MERGE paths:
- WHEN MATCHED: Updates existing records with higher DATA_VERSION
- WHEN NOT MATCHED: Inserts completely new records
"""

import json
import os
from datetime import datetime, timedelta
from faker import Faker
import random
from pathlib import Path

# Initialize Faker for generating realistic data
fake = Faker()
Faker.seed(300)  # Different seed for enhanced updates
random.seed(300)

# Create data directory
data_dir = Path("sample_data")
data_dir.mkdir(exist_ok=True)

# Generate timestamp for file naming
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

print("🔄 Generating ENHANCED INCREMENTAL UPDATE files (10 updates + 10 new records each)...")
print(f"📅 Timestamp: {timestamp}")

# =============================================================================
# 1. CUSTOMER UPDATES + NEW INSERTS
# =============================================================================
def generate_customer_updates():
    updates = []
    
    # PART 1: 10 UPDATE records (existing IDs 1-100, higher DATA_VERSION)
    customer_ids_to_update = random.sample(range(1, 101), 10)  # Pick 10 existing customers
    
    for customer_id in customer_ids_to_update:
        # Generate update with higher DATA_VERSION
        version = random.randint(2, 4)  # Version 2, 3, or 4
        update = {
            "CUSTOMER_ID": customer_id,
            "CUSTOMER_NAME": fake.name(),
            "EMAIL": fake.email(),
            "PHONE": fake.phone_number(),
            "ADDRESS": fake.street_address(),
            "CITY": fake.city(),
            "STATE": fake.state_abbr(),
            "ZIP_CODE": fake.zipcode(),
            "COUNTRY": "USA",
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(minutes=random.randint(10, 120))).isoformat(),
            "DATA_VERSION": version,
            "RECORD_STATUS": random.choice(["ACTIVE", "INACTIVE", "PENDING"])
        }
        updates.append(update)
    
    # PART 2: 10 INSERT records (new IDs 101-110, DATA_VERSION = 1)
    for customer_id in range(101, 111):  # New customers 101-110
        insert = {
            "CUSTOMER_ID": customer_id,
            "CUSTOMER_NAME": fake.name(),
            "EMAIL": fake.email(),
            "PHONE": fake.phone_number(),
            "ADDRESS": fake.street_address(),
            "CITY": fake.city(),
            "STATE": fake.state_abbr(),
            "ZIP_CODE": fake.zipcode(),
            "COUNTRY": "USA",
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(minutes=random.randint(10, 120))).isoformat(),
            "DATA_VERSION": 1,  # New records start with version 1
            "RECORD_STATUS": "ACTIVE"
        }
        updates.append(insert)
    
    return updates

# =============================================================================
# 2. PRODUCT UPDATES + NEW INSERTS
# =============================================================================
def generate_product_updates():
    updates = []
    categories = ["Electronics", "Clothing", "Home & Garden", "Books", "Sports", "Beauty", "Automotive", "Food", "Toys", "Health"]
    
    # PART 1: 10 UPDATE records (existing IDs 1-100, higher DATA_VERSION)
    product_ids_to_update = random.sample(range(1, 101), 10)
    
    for product_id in product_ids_to_update:
        version = random.randint(2, 4)
        update = {
            "PRODUCT_ID": product_id,
            "PRODUCT_NAME": fake.catch_phrase().replace(",", ""),
            "CATEGORY": random.choice(categories),
            "PRICE": round(random.uniform(9.99, 999.99), 2),
            "SUPPLIER_ID": random.randint(1, 25),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(minutes=random.randint(10, 120))).isoformat(),
            "DATA_VERSION": version,
            "RECORD_STATUS": random.choice(["ACTIVE", "DISCONTINUED", "OUT_OF_STOCK"])
        }
        updates.append(update)
    
    # PART 2: 10 INSERT records (new IDs 101-110, DATA_VERSION = 1)
    for product_id in range(101, 111):
        insert = {
            "PRODUCT_ID": product_id,
            "PRODUCT_NAME": fake.catch_phrase().replace(",", ""),
            "CATEGORY": random.choice(categories),
            "PRICE": round(random.uniform(9.99, 999.99), 2),
            "SUPPLIER_ID": random.randint(1, 25),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(minutes=random.randint(10, 120))).isoformat(),
            "DATA_VERSION": 1,
            "RECORD_STATUS": "ACTIVE"
        }
        updates.append(insert)
    
    return updates

# =============================================================================
# 3. ORDER UPDATES + NEW INSERTS
# =============================================================================
def generate_order_updates():
    updates = []
    statuses = ["PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
    
    # PART 1: 10 UPDATE records (existing IDs 1-100, higher DATA_VERSION)
    order_ids_to_update = random.sample(range(1, 101), 10)
    
    for order_id in order_ids_to_update:
        version = random.randint(2, 4)
        update = {
            "ORDER_ID": order_id,
            "CUSTOMER_ID": random.randint(1, 110),  # Can reference new customers too
            "ORDER_DATE": (datetime.now() - timedelta(days=random.randint(1, 60))).strftime('%Y-%m-%d'),
            "TOTAL_AMOUNT": round(random.uniform(25.00, 1500.00), 2),
            "ORDER_STATUS": random.choice(statuses),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(minutes=random.randint(10, 120))).isoformat(),
            "DATA_VERSION": version,
            "RECORD_STATUS": "ACTIVE"
        }
        updates.append(update)
    
    # PART 2: 10 INSERT records (new IDs 101-110, DATA_VERSION = 1)
    for order_id in range(101, 111):
        insert = {
            "ORDER_ID": order_id,
            "CUSTOMER_ID": random.randint(1, 110),
            "ORDER_DATE": (datetime.now() - timedelta(days=random.randint(1, 60))).strftime('%Y-%m-%d'),
            "TOTAL_AMOUNT": round(random.uniform(25.00, 1500.00), 2),
            "ORDER_STATUS": random.choice(statuses),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(minutes=random.randint(10, 120))).isoformat(),
            "DATA_VERSION": 1,
            "RECORD_STATUS": "ACTIVE"
        }
        updates.append(insert)
    
    return updates

# =============================================================================
# 4. ORDER ITEM UPDATES + NEW INSERTS
# =============================================================================
def generate_order_item_updates():
    updates = []
    
    # PART 1: 10 UPDATE records
    item_ids_to_update = random.sample(range(1, 101), 10)
    
    for item_id in item_ids_to_update:
        version = random.randint(2, 4)
        update = {
            "ORDER_ITEM_ID": item_id,
            "ORDER_ID": random.randint(1, 110),  # Can reference new orders
            "PRODUCT_ID": random.randint(1, 110),  # Can reference new products
            "QUANTITY": random.randint(1, 10),
            "UNIT_PRICE": round(random.uniform(9.99, 299.99), 2),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(minutes=random.randint(10, 120))).isoformat(),
            "DATA_VERSION": version,
            "RECORD_STATUS": random.choice(["ACTIVE", "CANCELLED", "RETURNED"])
        }
        updates.append(update)
    
    # PART 2: 10 INSERT records
    for item_id in range(101, 111):
        insert = {
            "ORDER_ITEM_ID": item_id,
            "ORDER_ID": random.randint(1, 110),
            "PRODUCT_ID": random.randint(1, 110),
            "QUANTITY": random.randint(1, 10),
            "UNIT_PRICE": round(random.uniform(9.99, 299.99), 2),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(minutes=random.randint(10, 120))).isoformat(),
            "DATA_VERSION": 1,
            "RECORD_STATUS": "ACTIVE"
        }
        updates.append(insert)
    
    return updates

# =============================================================================
# 5. SUPPLIER UPDATES + NEW INSERTS
# =============================================================================
def generate_supplier_updates():
    updates = []
    
    # PART 1: 10 UPDATE records
    supplier_ids_to_update = random.sample(range(1, 101), 10)
    
    for supplier_id in supplier_ids_to_update:
        version = random.randint(2, 4)
        update = {
            "SUPPLIER_ID": supplier_id,
            "SUPPLIER_NAME": fake.company(),
            "CONTACT_EMAIL": fake.company_email(),
            "CONTACT_PHONE": fake.phone_number(),
            "ADDRESS": fake.address().replace('\n', ', '),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(minutes=random.randint(10, 120))).isoformat(),
            "DATA_VERSION": version,
            "RECORD_STATUS": random.choice(["ACTIVE", "INACTIVE", "UNDER_REVIEW"])
        }
        updates.append(update)
    
    # PART 2: 10 INSERT records
    for supplier_id in range(101, 111):
        insert = {
            "SUPPLIER_ID": supplier_id,
            "SUPPLIER_NAME": fake.company(),
            "CONTACT_EMAIL": fake.company_email(),
            "CONTACT_PHONE": fake.phone_number(),
            "ADDRESS": fake.address().replace('\n', ', '),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(minutes=random.randint(10, 120))).isoformat(),
            "DATA_VERSION": 1,
            "RECORD_STATUS": "ACTIVE"
        }
        updates.append(insert)
    
    return updates

# =============================================================================
# 6. INVENTORY UPDATES + NEW INSERTS
# =============================================================================
def generate_inventory_updates():
    updates = []
    
    # PART 1: 10 UPDATE records
    inventory_ids_to_update = random.sample(range(1, 101), 10)
    
    for inventory_id in inventory_ids_to_update:
        version = random.randint(2, 4)
        update = {
            "INVENTORY_ID": inventory_id,
            "PRODUCT_ID": random.randint(1, 110),  # Can reference new products
            "WAREHOUSE_ID": random.randint(1, 20),
            "QUANTITY_ON_HAND": random.randint(0, 1000),
            "REORDER_LEVEL": random.randint(10, 50),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(minutes=random.randint(10, 120))).isoformat(),
            "DATA_VERSION": version,
            "RECORD_STATUS": "ACTIVE"
        }
        updates.append(update)
    
    # PART 2: 10 INSERT records
    for inventory_id in range(101, 111):
        insert = {
            "INVENTORY_ID": inventory_id,
            "PRODUCT_ID": random.randint(1, 110),
            "WAREHOUSE_ID": random.randint(1, 20),
            "QUANTITY_ON_HAND": random.randint(0, 1000),
            "REORDER_LEVEL": random.randint(10, 50),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(minutes=random.randint(10, 120))).isoformat(),
            "DATA_VERSION": 1,
            "RECORD_STATUS": "ACTIVE"
        }
        updates.append(insert)
    
    return updates

# =============================================================================
# 7. WAREHOUSE UPDATES + NEW INSERTS
# =============================================================================
def generate_warehouse_updates():
    updates = []
    
    # PART 1: 10 UPDATE records
    warehouse_ids_to_update = random.sample(range(1, 101), 10)
    
    for warehouse_id in warehouse_ids_to_update:
        version = random.randint(2, 4)
        update = {
            "WAREHOUSE_ID": warehouse_id,
            "WAREHOUSE_NAME": f"Warehouse {fake.city()} {warehouse_id}",
            "LOCATION": f"{fake.city()}, {fake.state_abbr()}",
            "MANAGER_ID": random.randint(1, 110),  # Can reference new employees
            "CAPACITY": random.randint(10000, 100000),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(minutes=random.randint(10, 120))).isoformat(),
            "DATA_VERSION": version,
            "RECORD_STATUS": random.choice(["ACTIVE", "MAINTENANCE", "CLOSED"])
        }
        updates.append(update)
    
    # PART 2: 10 INSERT records
    for warehouse_id in range(101, 111):
        insert = {
            "WAREHOUSE_ID": warehouse_id,
            "WAREHOUSE_NAME": f"Warehouse {fake.city()} {warehouse_id}",
            "LOCATION": f"{fake.city()}, {fake.state_abbr()}",
            "MANAGER_ID": random.randint(1, 110),
            "CAPACITY": random.randint(10000, 100000),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(minutes=random.randint(10, 120))).isoformat(),
            "DATA_VERSION": 1,
            "RECORD_STATUS": "ACTIVE"
        }
        updates.append(insert)
    
    return updates

# =============================================================================
# 8. EMPLOYEE UPDATES + NEW INSERTS
# =============================================================================
def generate_employee_updates():
    updates = []
    departments = ["Sales", "Marketing", "Engineering", "HR", "Finance", "Operations"]
    
    # PART 1: 10 UPDATE records
    employee_ids_to_update = random.sample(range(1, 101), 10)
    
    for employee_id in employee_ids_to_update:
        version = random.randint(2, 4)
        update = {
            "EMPLOYEE_ID": employee_id,
            "FIRST_NAME": fake.first_name(),
            "LAST_NAME": fake.last_name(),
            "EMAIL": fake.email(),
            "PHONE": fake.phone_number(),
            "DEPARTMENT": random.choice(departments),
            "SALARY": round(random.uniform(40000, 180000), 2),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(minutes=random.randint(10, 120))).isoformat(),
            "DATA_VERSION": version,
            "RECORD_STATUS": random.choice(["ACTIVE", "ON_LEAVE", "TERMINATED"])
        }
        updates.append(update)
    
    # PART 2: 10 INSERT records
    for employee_id in range(101, 111):
        insert = {
            "EMPLOYEE_ID": employee_id,
            "FIRST_NAME": fake.first_name(),
            "LAST_NAME": fake.last_name(),
            "EMAIL": fake.email(),
            "PHONE": fake.phone_number(),
            "DEPARTMENT": random.choice(departments),
            "SALARY": round(random.uniform(35000, 150000), 2),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(minutes=random.randint(10, 120))).isoformat(),
            "DATA_VERSION": 1,
            "RECORD_STATUS": "ACTIVE"
        }
        updates.append(insert)
    
    return updates

# =============================================================================
# 9. TERRITORY UPDATES + NEW INSERTS
# =============================================================================
def generate_territory_updates():
    updates = []
    regions = ["North", "South", "East", "West", "Central"]
    
    # PART 1: 10 UPDATE records
    territory_ids_to_update = random.sample(range(1, 101), 10)
    
    for territory_id in territory_ids_to_update:
        version = random.randint(2, 4)
        update = {
            "TERRITORY_ID": territory_id,
            "TERRITORY_NAME": f"{fake.state()} {random.choice(['North', 'South', 'Metro', 'Valley'])}",
            "REGION": random.choice(regions),
            "MANAGER_ID": random.randint(1, 110),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(minutes=random.randint(10, 120))).isoformat(),
            "DATA_VERSION": version,
            "RECORD_STATUS": random.choice(["ACTIVE", "INACTIVE", "RESTRUCTURED"])
        }
        updates.append(update)
    
    # PART 2: 10 INSERT records
    for territory_id in range(101, 111):
        insert = {
            "TERRITORY_ID": territory_id,
            "TERRITORY_NAME": f"{fake.state()} {random.choice(['North', 'South', 'Metro', 'Valley'])}",
            "REGION": random.choice(regions),
            "MANAGER_ID": random.randint(1, 110),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(minutes=random.randint(10, 120))).isoformat(),
            "DATA_VERSION": 1,
            "RECORD_STATUS": "ACTIVE"
        }
        updates.append(insert)
    
    return updates

# =============================================================================
# 10. PROMOTION UPDATES + NEW INSERTS
# =============================================================================
def generate_promotion_updates():
    updates = []
    
    # PART 1: 10 UPDATE records
    promotion_ids_to_update = random.sample(range(1, 101), 10)
    
    for promotion_id in promotion_ids_to_update:
        version = random.randint(2, 4)
        update = {
            "PROMOTION_ID": promotion_id,
            "PROMOTION_NAME": f"{fake.catch_phrase().replace(',', '')} Sale",
            "DISCOUNT_PERCENTAGE": round(random.uniform(5.0, 50.0), 2) if random.choice([True, False]) else None,
            "START_DATE": (datetime.now() - timedelta(days=random.randint(30, 90))).strftime('%Y-%m-%d'),
            "END_DATE": (datetime.now() + timedelta(days=random.randint(30, 180))).strftime('%Y-%m-%d'),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(minutes=random.randint(10, 120))).isoformat(),
            "DATA_VERSION": version,
            "RECORD_STATUS": random.choice(["ACTIVE", "EXPIRED", "PAUSED"])
        }
        updates.append(update)
    
    # PART 2: 10 INSERT records
    for promotion_id in range(101, 111):
        insert = {
            "PROMOTION_ID": promotion_id,
            "PROMOTION_NAME": f"{fake.catch_phrase().replace(',', '')} Sale",
            "DISCOUNT_PERCENTAGE": round(random.uniform(5.0, 50.0), 2) if random.choice([True, False]) else None,
            "START_DATE": (datetime.now() - timedelta(days=random.randint(30, 90))).strftime('%Y-%m-%d'),
            "END_DATE": (datetime.now() + timedelta(days=random.randint(30, 180))).strftime('%Y-%m-%d'),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(minutes=random.randint(10, 120))).isoformat(),
            "DATA_VERSION": 1,
            "RECORD_STATUS": "ACTIVE"
        }
        updates.append(insert)
    
    return updates

# =============================================================================
# GENERATE AND SAVE SEPARATE UPDATE FILES
# =============================================================================

# Generate all update datasets
update_generators = {
    "customers": generate_customer_updates,
    "products": generate_product_updates,
    "orders": generate_order_updates,
    "order_items": generate_order_item_updates,
    "suppliers": generate_supplier_updates,
    "inventory": generate_inventory_updates,
    "warehouses": generate_warehouse_updates,
    "employees": generate_employee_updates,
    "sales_territories": generate_territory_updates,
    "promotions": generate_promotion_updates
}

print("\n📊 CREATING ENHANCED UPDATE FILES (10 updates + 10 new records each):")
total_update_records = 0

# Create separate update files with timestamp
for dataset_name, generator_func in update_generators.items():
    updates = generator_func()
    
    if updates:  # Only create file if there are updates
        # Create timestamped filename
        update_filename = f"{dataset_name}_update_{timestamp}.json"
        filepath = data_dir / update_filename
        
        # Save update records to separate file
        with open(filepath, 'w') as f:
            json.dump(updates, f, indent=2)
        
        total_update_records += len(updates)
        print(f"✅ Created {update_filename}: {len(updates)} records (10 updates + 10 new)")

print(f"\n🎉 Enhanced incremental update files generation complete!")
print(f"📁 Location: {data_dir.absolute()}")
print(f"📈 Total update files: {len(update_generators)}")
print(f"📊 Total records: {total_update_records} (200 records total)")

print(f"\n🎯 PERFECT MERGE DEMO STRUCTURE:")
print("1. 📋 Initial load (per dataset):")
print("   • IDs 1-100, DATA_VERSION = 1")

print(f"\n2. 🔄 Update files (per dataset):")
print("   • 10 UPDATE records: Existing IDs (random from 1-100), DATA_VERSION 2-4")  
print("   • 10 INSERT records: New IDs (101-110), DATA_VERSION = 1")

print(f"\n💡 MERGE BEHAVIOR DEMONSTRATION:")
print("   • WHEN MATCHED + higher DATA_VERSION → UPDATE existing records")
print("   • WHEN NOT MATCHED → INSERT new records (IDs 101-110)")
print("   • Perfect showcase of both MERGE paths!")

print(f"\n📝 DEMO STEPS:")
print("1. Upload initial files: @06_demo_file_upload.sql")
print("2. Create Streams & Tasks: @05_create_streams_and_tasks.sql")  
print("3. Upload update files: @06B_upload_update_files.sql (uses patterns)")
print("4. Monitor pipeline: @07_demo_monitoring_validation.sql")

print(f"\n🎪 EXPECTED FINAL RESULTS:")
print("  • Stage tables: 120 total records per dataset (100 initial + 20 updates)")
print("  • Latest tables: 110 unique records per dataset (IDs 1-110, latest versions only)")
print("  • Demonstrates both UPDATE and INSERT merge behavior perfectly!")