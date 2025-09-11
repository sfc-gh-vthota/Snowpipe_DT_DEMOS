#!/usr/bin/env python3
"""
Initial Data Generator for Snowpipe + Streams + Tasks Demo

This script generates JSON files with 100 unique records each to demonstrate:
1. Snowpipe with JSON schema detection loading initial records
2. Streams capturing changes when update files are loaded
3. Tasks processing changes with intelligent MERGE operations

Creates 10 JSON files, each with 100 unique records (1000 total records)
"""

import json
import os
from datetime import datetime, timedelta
from faker import Faker
import random
from pathlib import Path

# Initialize Faker for generating realistic data
fake = Faker()
Faker.seed(42)  # For reproducible data
random.seed(42)

# Create data directory
data_dir = Path("sample_data")
data_dir.mkdir(exist_ok=True)

print("🚀 Generating initial JSON data files (100 records each)...")

# =============================================================================
# 1. CUSTOMERS (100 unique customers)
# =============================================================================
def generate_customers():
    customers = []
    for i in range(1, 101):  # 100 unique customers
        customer = {
            "CUSTOMER_ID": i,
            "CUSTOMER_NAME": fake.name(),
            "EMAIL": fake.email(),
            "PHONE": fake.phone_number(),
            "ADDRESS": fake.street_address(),
            "CITY": fake.city(),
            "STATE": fake.state_abbr(),
            "ZIP_CODE": fake.zipcode(),
            "COUNTRY": "USA",
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat(),
            "DATA_VERSION": 1,  # Initial version
            "RECORD_STATUS": "ACTIVE"
        }
        customers.append(customer)
    return customers

# =============================================================================
# 2. PRODUCTS (100 unique products)
# =============================================================================
def generate_products():
    products = []
    categories = ["Electronics", "Clothing", "Home & Garden", "Books", "Sports", "Beauty", "Automotive", "Food", "Toys", "Health"]
    
    for i in range(1, 101):  # 100 unique products
        product = {
            "PRODUCT_ID": i,
            "PRODUCT_NAME": fake.catch_phrase().replace(",", ""),
            "CATEGORY": random.choice(categories),
            "PRICE": round(random.uniform(9.99, 999.99), 2),
            "SUPPLIER_ID": random.randint(1, 25),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat(),
            "DATA_VERSION": 1,
            "RECORD_STATUS": "ACTIVE"
        }
        products.append(product)
    return products

# =============================================================================
# 3. ORDERS (100 unique orders)
# =============================================================================
def generate_orders():
    orders = []
    statuses = ["PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
    
    for i in range(1, 101):  # 100 unique orders
        order = {
            "ORDER_ID": i,
            "CUSTOMER_ID": random.randint(1, 100),
            "ORDER_DATE": (datetime.now() - timedelta(days=random.randint(1, 60))).strftime('%Y-%m-%d'),
            "TOTAL_AMOUNT": round(random.uniform(25.00, 1500.00), 2),
            "ORDER_STATUS": random.choice(statuses),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat(),
            "DATA_VERSION": 1,
            "RECORD_STATUS": "ACTIVE"
        }
        orders.append(order)
    return orders

# =============================================================================
# 4. ORDER ITEMS (100 unique order items)
# =============================================================================
def generate_order_items():
    order_items = []
    
    for i in range(1, 101):  # 100 unique order items
        order_item = {
            "ORDER_ITEM_ID": i,
            "ORDER_ID": random.randint(1, 100),
            "PRODUCT_ID": random.randint(1, 100),
            "QUANTITY": random.randint(1, 10),
            "UNIT_PRICE": round(random.uniform(9.99, 299.99), 2),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat(),
            "DATA_VERSION": 1,
            "RECORD_STATUS": "ACTIVE"
        }
        order_items.append(order_item)
    return order_items

# =============================================================================
# 5. SUPPLIERS (100 unique suppliers)
# =============================================================================
def generate_suppliers():
    suppliers = []
    
    for i in range(1, 101):  # 100 unique suppliers
        supplier = {
            "SUPPLIER_ID": i,
            "SUPPLIER_NAME": fake.company(),
            "CONTACT_EMAIL": fake.company_email(),
            "CONTACT_PHONE": fake.phone_number(),
            "ADDRESS": fake.address().replace('\n', ', '),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat(),
            "DATA_VERSION": 1,
            "RECORD_STATUS": "ACTIVE"
        }
        suppliers.append(supplier)
    return suppliers

# =============================================================================
# 6. INVENTORY (100 unique inventory records)
# =============================================================================
def generate_inventory():
    inventory = []
    
    for i in range(1, 101):  # 100 unique inventory records
        inventory_record = {
            "INVENTORY_ID": i,
            "PRODUCT_ID": random.randint(1, 100),
            "WAREHOUSE_ID": random.randint(1, 20),
            "QUANTITY_ON_HAND": random.randint(0, 1000),
            "REORDER_LEVEL": random.randint(10, 50),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat(),
            "DATA_VERSION": 1,
            "RECORD_STATUS": "ACTIVE"
        }
        inventory.append(inventory_record)
    return inventory

# =============================================================================
# 7. WAREHOUSES (100 unique warehouses)
# =============================================================================
def generate_warehouses():
    warehouses = []
    
    for i in range(1, 101):  # 100 unique warehouses
        warehouse = {
            "WAREHOUSE_ID": i,
            "WAREHOUSE_NAME": f"Warehouse {fake.city()} {i}",
            "LOCATION": f"{fake.city()}, {fake.state_abbr()}",
            "MANAGER_ID": random.randint(1, 100),
            "CAPACITY": random.randint(10000, 100000),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat(),
            "DATA_VERSION": 1,
            "RECORD_STATUS": "ACTIVE"
        }
        warehouses.append(warehouse)
    return warehouses

# =============================================================================
# 8. EMPLOYEES (100 unique employees)
# =============================================================================
def generate_employees():
    employees = []
    departments = ["Sales", "Marketing", "Engineering", "HR", "Finance", "Operations", "Customer Service", "IT", "Legal", "Executive"]
    
    for i in range(1, 101):  # 100 unique employees
        employee = {
            "EMPLOYEE_ID": i,
            "FIRST_NAME": fake.first_name(),
            "LAST_NAME": fake.last_name(),
            "EMAIL": fake.email(),
            "PHONE": fake.phone_number(),
            "DEPARTMENT": random.choice(departments),
            "SALARY": round(random.uniform(35000, 150000), 2),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat(),
            "DATA_VERSION": 1,
            "RECORD_STATUS": "ACTIVE"
        }
        employees.append(employee)
    return employees

# =============================================================================
# 9. TERRITORIES (100 unique territories)
# =============================================================================
def generate_territories():
    territories = []
    regions = ["North", "South", "East", "West", "Central", "Northeast", "Southeast", "Northwest", "Southwest", "Pacific"]
    
    for i in range(1, 101):  # 100 unique territories
        territory = {
            "TERRITORY_ID": i,
            "TERRITORY_NAME": f"{fake.state()} {random.choice(['North', 'South', 'Metro', 'Valley'])}",
            "REGION": random.choice(regions),
            "MANAGER_ID": random.randint(1, 100),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat(),
            "DATA_VERSION": 1,
            "RECORD_STATUS": "ACTIVE"
        }
        territories.append(territory)
    return territories

# =============================================================================
# 10. PROMOTIONS (100 unique promotions)
# =============================================================================
def generate_promotions():
    promotions = []
    promo_types = ["PERCENTAGE", "FIXED_AMOUNT", "BUY_ONE_GET_ONE", "FREE_SHIPPING", "LOYALTY_BONUS"]
    
    for i in range(1, 101):  # 100 unique promotions
        promotion = {
            "PROMOTION_ID": i,
            "PROMOTION_NAME": f"{fake.catch_phrase().replace(',', '')} Sale",
            "DISCOUNT_PERCENTAGE": round(random.uniform(5.0, 50.0), 2) if random.choice([True, False]) else None,
            "START_DATE": (datetime.now() - timedelta(days=random.randint(30, 90))).strftime('%Y-%m-%d'),
            "END_DATE": (datetime.now() + timedelta(days=random.randint(30, 180))).strftime('%Y-%m-%d'),
            "RECORD_TIMESTAMP": (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat(),
            "DATA_VERSION": 1,
            "RECORD_STATUS": "ACTIVE"
        }
        promotions.append(promotion)
    return promotions

# =============================================================================
# GENERATE AND SAVE ALL FILES
# =============================================================================

# Define all datasets
datasets = {
    "customers.json": generate_customers(),
    "products.json": generate_products(),
    "orders.json": generate_orders(),
    "order_items.json": generate_order_items(),
    "suppliers.json": generate_suppliers(),
    "inventory.json": generate_inventory(),
    "warehouses.json": generate_warehouses(),
    "employees.json": generate_employees(),
    "sales_territories.json": generate_territories(),
    "promotions.json": generate_promotions()
}

# Save all datasets to JSON files
for filename, data in datasets.items():
    filepath = data_dir / filename
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"✅ Created {filename}: {len(data)} records")

print(f"\n🎉 Initial data generation complete!")
print(f"📁 Location: {data_dir.absolute()}")
print(f"📊 Total files: {len(datasets)}")
print(f"📈 Total records: {sum(len(data) for data in datasets.values())}")
print("\n📝 Next steps:")
print("1. Run this script: python generate_initial_data.py")
print("2. Upload files: @06_demo_file_upload.sql")
print("3. Generate updates: python generate_update_data.py")
print("4. Upload updates: @06_demo_file_upload.sql (again)")
