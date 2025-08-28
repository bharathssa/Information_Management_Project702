# Setup Instructions

This document provides detailed instructions for setting up and running the Food Delivery Analytics project.

## Prerequisites

### Software Requirements
- **SQLite 3.x** or higher
- **SQL Client** (choose one):
  - DB Browser for SQLite (recommended for beginners)
  - DBeaver (advanced features)
  - SQLite command line interface
  - Any SQL IDE that supports SQLite

### Data Requirements
You'll need the following staging tables with your raw data:
- `stg_users` - Customer demographic data
- `stg_restaurant` - Restaurant information
- `stg_orders` - Order transaction data
- `stg_location` - Geographic location data

## Installation Steps

### Step 1: Clone the Repository
```bash
git clone https://github.com/yourusername/food-delivery-analytics.git
cd food-delivery-analytics
```

### Step 2: Create SQLite Database
```bash
# Using SQLite command line
sqlite3 food_delivery.db

# Or create a new database file in your SQL client
```

### Step 3: Load Staging Data
Before running the ETL pipeline, you need to create and populate your staging tables:

#### Option A: Using SQLite Command Line
```sql
-- Create staging tables
CREATE TABLE stg_users (
    user_id INTEGER,
    name TEXT,
    email TEXT,
    age INTEGER,
    gender TEXT,
    "Family size" INTEGER,
    "Monthly Income" TEXT,
    "Educational qualifications" TEXT
);

CREATE TABLE stg_restaurant (
    id INTEGER,
    name TEXT,
    city TEXT,
    rating REAL,
    cuisine TEXT
);

CREATE TABLE stg_orders (
    user_id INTEGER,
    r_id INTEGER,
    order_date TEXT,
    sales_qty INTEGER,
    sales_amount REAL,
    currency TEXT
);

CREATE TABLE stg_location (
    "Country" TEXT,
    "State" TEXT,
    "City" TEXT
);

-- Import your CSV data
.mode csv
.import your_users_data.csv stg_users
.import your_restaurant_data.csv stg_restaurant
.import your_orders_data.csv stg_orders
.import your_location_data.csv stg_location
```

#### Option B: Using DB Browser for SQLite
1. Open DB Browser for SQLite
2. Create a new database
3. Use File → Import → Table from CSV file for each staging table
4. Make sure column names match the expected schema

### Step 4: Run ETL Pipeline
Execute the main ETL script to create the star schema:

```sql
-- In your SQL client, execute:
.read etl_pipeline.sql

-- Or copy and paste the contents of etl_pipeline.sql
```

### Step 5: Verify Installation
Run the verification queries at the end of the ETL script:

```sql
-- Check table counts
SELECT 'fact_order' t, COUNT(*) c FROM fact_order
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_restaurant', COUNT(*) FROM dim_restaurant
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL SELECT 'dim_location', COUNT(*) FROM dim_location;

-- Check for data quality issues
SELECT 'date_fk_orphans' AS check_name, COUNT(*) AS rows
FROM fact_order f LEFT JOIN dim_date d ON f.date_key = d.date_key
WHERE d.date_key IS NULL;
```

## Expected Data Formats

### Staging Table Schemas

#### stg_users
| Column | Type | Description |
|--------|------|-------------|
| user_id | INTEGER | Unique customer identifier |
| name | TEXT | Customer name |
| email | TEXT | Customer email |
| age | INTEGER | Customer age |
| gender | TEXT | Customer gender |
| Family size | INTEGER | Number of family members |
| Monthly Income | TEXT | Income bracket description |
| Educational qualifications | TEXT | Education level |

#### stg_restaurant
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Unique restaurant identifier |
| name | TEXT | Restaurant name |
| city | TEXT | Restaurant city |
| rating | REAL | Restaurant rating (1-5) |
| cuisine | TEXT | Cuisine type(s) |

#### stg_orders
| Column | Type | Description |
|--------|------|-------------|
| user_id | INTEGER | Customer ID (FK to stg_users) |
| r_id | INTEGER | Restaurant ID (FK to stg_restaurant) |
| order_date | TEXT | Order timestamp |
| sales_qty | INTEGER | Number of items |
| sales_amount | REAL | Order value |
| currency | TEXT | Currency code |

#### stg_location
| Column | Type | Description |
|--------|------|-------------|
| Country | TEXT | Country name |
| State | TEXT | State/province name |
| City | TEXT | City name |

## Troubleshooting

### Common Issues

#### 1. Foreign Key Violations
If you see foreign key errors:
```sql
PRAGMA foreign_keys = OFF;
-- Run your problematic query
PRAGMA foreign_keys = ON;
```

#### 2. Data Type Mismatches
Ensure your staging data types match the expected formats:
- Dates should be in ISO format (YYYY-MM-DD HH:MM:SS)
- Numbers should not contain non-numeric characters
- Text fields should be properly quoted

#### 3. Missing Data
Check for NULL values in critical fields:
```sql
-- Check for missing customer data
SELECT COUNT(*) FROM stg_users WHERE user_id IS NULL;

-- Check for missing restaurant data  
SELECT COUNT(*) FROM stg_restaurant WHERE id IS NULL;
```

#### 4. Performance Issues
If queries are slow, ensure indexes are created:
```sql
-- The ETL script creates indexes automatically, but you can verify:
.indexes fact_order
```

### Data Quality Checks

Run these queries to validate your data quality:

```sql
-- 1. Check for duplicate customers
SELECT user_id, COUNT(*) 
FROM stg_users 
GROUP BY user_id 
HAVING COUNT(*) > 1;

-- 2. Check for invalid dates
SELECT COUNT(*) 
FROM stg_orders 
WHERE order_date IS NULL OR order_date = '';

-- 3. Check for negative amounts
SELECT COUNT(*) 
FROM stg_orders 
WHERE sales_amount < 0;
```

## Next Steps

After successful setup:
1. Run the analytics queries in `analytics_queries.sql`
2. Create visualizations using your preferred BI tool
3. Customize the queries for your specific analysis needs

## Support

If you encounter issues:
1. Check the troubleshooting section above
2. Verify your data format matches the expected schema
3. Create an issue in the GitHub repository with:
   - Error message
   - Sample data structure
   - Steps to reproduce
