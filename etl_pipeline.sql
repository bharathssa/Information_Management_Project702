/* =======================================================================
   Food Delivery Analytics - Star Schema ETL Pipeline
   
   This script creates a complete star schema data warehouse for food delivery
   analytics, including customer demographics, restaurant performance, and
   order behavior analysis.
   
   Author: [Bharath Chandran Madhaiyan]
   Created: [28-08-2025]
   Version: 1.0

-- -- =========================================================================================================================================================================
-- PHASE 1: CREATE — Schema Objects and Views
-- This section creates all dimension tables, fact table, and views.
-- Natural keys are cast to TEXT to preserve formatting. 
-- This phase is fully transactional and safe to rerun.
-- -- =========================================================================================================================================================================
PRAGMA foreign_keys = ON;


-- ======================================================================
-- Drop existing tables and views (clean slate for idempotency)
-- ======================================================================
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_location;
DROP TABLE IF EXISTS dim_restaurant;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS fact_order;

DROP VIEW IF EXISTS v_stg_orders_nk;
DROP VIEW IF EXISTS v_fact_load;
DROP VIEW IF EXISTS v_new_dates;

-- ======================================================================
-- Create dim_date table (calendar attributes from order_date)
-- ======================================================================
CREATE TABLE IF NOT EXISTS dim_date (
  date_key INTEGER PRIMARY KEY,
  date_iso TEXT UNIQUE,
  year INTEGER,
  quarter INTEGER,
  month INTEGER,
  month_name TEXT,
  day INTEGER,
  dow INTEGER,
  is_weekend INTEGER
);

-- ======================================================================
-- Create dim_customer table (with demographic and derived attributes)
-- ======================================================================
CREATE TABLE IF NOT EXISTS dim_customer (
  customer_key INTEGER PRIMARY KEY,
  user_id_nat TEXT UNIQUE NOT NULL,
  name TEXT,
  email TEXT,
  age INTEGER,
  gender TEXT,
  family_size INTEGER,
  monthly_income_raw TEXT,
  income_group TEXT,
  income_group_order INTEGER,
  education_raw TEXT,
  education_group TEXT,
  education_group_order INTEGER
);

-- ======================================================================
-- Create dim_restaurant table (with city to support location join)
-- ======================================================================
CREATE TABLE IF NOT EXISTS dim_restaurant (
  restaurant_key INTEGER PRIMARY KEY,
  restaurant_id_nat TEXT UNIQUE NOT NULL,
  name TEXT,
  city TEXT,
  rating REAL,
  cuisine TEXT
);

-- ======================================================================
-- Create dim_location table (enriched from external country/state/city)
-- ======================================================================
CREATE TABLE IF NOT EXISTS dim_location (
  location_key INTEGER PRIMARY KEY,
  location_nk TEXT UNIQUE NOT NULL,
  country TEXT NOT NULL,
  state TEXT,
  city TEXT NOT NULL
);

-- ======================================================================
-- Create fact_order table (order-level grain)
-- ======================================================================
CREATE TABLE IF NOT EXISTS fact_order (
  order_key INTEGER PRIMARY KEY,
  order_nk TEXT UNIQUE,
  date_key INTEGER NOT NULL,
  customer_key INTEGER NOT NULL,
  restaurant_key INTEGER NOT NULL,
  sales_qty INTEGER NOT NULL,
  sales_amount REAL NOT NULL,
  currency TEXT,
  location_key INTEGER,
  FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
  FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
  FOREIGN KEY (restaurant_key) REFERENCES dim_restaurant(restaurant_key),
  FOREIGN KEY (location_key) REFERENCES dim_location(location_key)
);

-- ======================================================================
-- Create staging view: v_stg_orders_nk (builds stable order_nk key)
-- ======================================================================
CREATE VIEW IF NOT EXISTS v_stg_orders_nk AS
SELECT
  printf('%s|%s|%s|%s|%s|%s',
         CAST(user_id AS TEXT),
         CAST(r_id AS TEXT),
         strftime('%Y-%m-%d %H:%M:%S', order_date),
         TRIM(IFNULL(currency, '')),
         TRIM(CAST(sales_qty AS TEXT)),
         TRIM(CAST(sales_amount AS TEXT))) AS order_nk,
  *
FROM stg_orders;

-- ======================================================================
-- Create temp view: v_new_dates (calendar breakdown from order_date)
-- ======================================================================
CREATE TEMP VIEW IF NOT EXISTS v_new_dates AS
SELECT DISTINCT
  CAST(strftime('%Y%m%d', order_date) AS INTEGER) AS date_key,
  substr(order_date, 1, 10) AS date_iso,
  CAST(strftime('%Y', order_date) AS INTEGER) AS year,
  ((CAST(strftime('%m', order_date) AS INTEGER) - 1) / 3) + 1 AS quarter,
  CAST(strftime('%m', order_date) AS INTEGER) AS month,
  CASE strftime('%m', order_date)
    WHEN '01' THEN 'Jan' WHEN '02' THEN 'Feb' WHEN '03' THEN 'Mar'
    WHEN '04' THEN 'Apr' WHEN '05' THEN 'May' WHEN '06' THEN 'Jun'
    WHEN '07' THEN 'Jul' WHEN '08' THEN 'Aug' WHEN '09' THEN 'Sep'
    WHEN '10' THEN 'Oct' WHEN '11' THEN 'Nov' WHEN '12' THEN 'Dec'
  END AS month_name,
  CAST(strftime('%d', order_date) AS INTEGER) AS day,
  CAST(strftime('%w', order_date) AS INTEGER) + 1 AS dow,
  CASE WHEN strftime('%w', order_date) IN ('0', '6') THEN 1 ELSE 0 END AS is_weekend
FROM stg_orders;

-- ======================================================================
-- Create temp view: v_fact_load (resolves surrogate keys for fact insert)
-- ======================================================================
CREATE TEMP VIEW v_fact_load AS
SELECT
  printf('%s|%s|%s|%s|%s|%s',
         TRIM(CAST(CAST(v.user_id AS INTEGER) AS TEXT)),
         TRIM(CAST(CAST(v.r_id AS INTEGER) AS TEXT)),
         strftime('%Y-%m-%d %H:%M:%S', v.order_date),
         TRIM(IFNULL(v.currency, '')),
         TRIM(CAST(v.sales_qty AS TEXT)),
         TRIM(CAST(v.sales_amount AS TEXT))) AS order_nk,
  CAST(strftime('%Y%m%d', v.order_date) AS INTEGER) AS date_key,
  dc.customer_key,
  dr.restaurant_key,
  CAST(v.sales_qty AS INTEGER) AS sales_qty,
  CAST(v.sales_amount AS REAL) AS sales_amount,
  TRIM(v.currency) AS currency
FROM v_stg_orders_nk v
JOIN dim_customer dc ON dc.user_id_nat = TRIM(CAST(CAST(v.user_id AS INTEGER) AS TEXT))
JOIN dim_restaurant dr ON dr.restaurant_id_nat = TRIM(CAST(CAST(v.r_id AS INTEGER) AS TEXT))
JOIN dim_date dd ON dd.date_key = CAST(strftime('%Y%m%d', v.order_date) AS INTEGER);



-- =========================================================================================================================================================================
-- PHASE 2: LOAD & TRANSFORM — Initial Inserts and Derived Fields
-- Load raw data from staging into dimension and fact tables.
-- Perform safe insert-or-ignore to avoid duplicates.
-- No updates to existing rows yet (patching is in Phase 3).
-- =========================================================================================================================================================================

-- ----------------------------------------------------------------------
-- Load dim_date from v_new_dates
-- ----------------------------------------------------------------------
INSERT OR IGNORE INTO dim_date
SELECT * FROM v_new_dates;

-- ----------------------------------------------------------------------
-- Load dim_customer from stg_users (basic demographic fields)
-- ----------------------------------------------------------------------
INSERT OR IGNORE INTO dim_customer (
  user_id_nat, name, email, age, gender, family_size, monthly_income_raw, education_raw
)
SELECT
  CAST(user_id AS TEXT),
  name,
  email,
  CAST(age AS INTEGER),
  gender,
  CAST("Family size" AS INTEGER),
  "Monthly Income",
  "Educational Qualifications"
FROM stg_users;

-- ----------------------------------------------------------------------
-- Load dim_restaurant from stg_restaurant
-- ----------------------------------------------------------------------
INSERT OR IGNORE INTO dim_restaurant (
  restaurant_id_nat, name, city, rating, cuisine
)
SELECT
  CAST(id AS TEXT),
  name,
  city,
  CAST(rating AS REAL),
  cuisine
FROM stg_restaurant;

-- ----------------------------------------------------------------------
-- Load dim_location from stg_location (grouped and enriched)
-- ----------------------------------------------------------------------
INSERT OR IGNORE INTO dim_location (
  location_nk, country, state, city
)
SELECT
  printf('%s|%s|%s', country, IFNULL(state, ''), city),
  country,
  state,
  city
FROM stg_location
GROUP BY country, state, city;

-- ----------------------------------------------------------------------
-- Load fact_order using resolved surrogate keys from v_fact_load
-- ----------------------------------------------------------------------
INSERT OR IGNORE INTO fact_order (
  order_nk, date_key, customer_key, restaurant_key,
  sales_qty, sales_amount, currency
)
SELECT
  order_nk, date_key, customer_key, restaurant_key,
  sales_qty, sales_amount, currency
FROM v_fact_load;


-- =========================================================================================================================================================================
-- PHASE 3: UPDATE & PATCH — Enrichments and Data Fixes
-- These updates adjust rows after load: adding derived groups, fixing currencies, and filling FKs.
-- Safe to rerun if needed.
-- =========================================================================================================================================================================

-- ----------------------------------------------------------------------
-- Patch location_key in fact_order using partial city match from restaurant.city
-- ----------------------------------------------------------------------
UPDATE fact_order AS f
SET location_key = (
  SELECT dl.location_key
  FROM dim_restaurant dr
  JOIN dim_location dl ON dr.city LIKE '%' || dl.city || '%' COLLATE NOCASE
  WHERE dr.restaurant_key = f.restaurant_key
  LIMIT 1
)
WHERE f.location_key IS NULL;

-- ----------------------------------------------------------------------
-- Enrich income_group and income_group_order in dim_customer
-- ----------------------------------------------------------------------
-- Recompute income buckets (idempotent) # tranforming income_group and income_group_order
UPDATE dim_customer
SET
  income_group = CASE
    WHEN LOWER(monthly_income_raw) LIKE 'no income%'       THEN 'None'
    WHEN LOWER(monthly_income_raw) LIKE '%below rs.10000%'   THEN 'Below 10k'
    WHEN LOWER(monthly_income_raw) LIKE '%10001 to 25000%'  THEN '10k-25k'
    WHEN LOWER(monthly_income_raw) LIKE '%25001 to 50000%'  THEN '25k-50k'
    WHEN LOWER(monthly_income_raw) LIKE '%more than 50000%' THEN '50k+'
    ELSE 'Unknown'
  END;
  
  UPDATE dim_customer
  SET
  income_group_order = CASE
    WHEN income_group = 'None'       THEN 0
    WHEN income_group = 'Below 10k'  THEN 1
    WHEN income_group = '10k-25k'    THEN 2
    WHEN income_group = '25k-50k'    THEN 3
    WHEN income_group = '50k+'       THEN 4
    ELSE 9
  END;
  
-- ----------------------------------------------------------------------
-- Enrich education_group and education_group_order in dim_customer
-- ----------------------------------------------------------------------
-- Categorize education levels into 3 groups:
--- group1. Basic Education: Uneducated, School
--- group2. Higher Education: post-graduate, graduate
--- group3. Doctoral: PhD
UPDATE dim_customer
SET education_group = CASE
  WHEN LOWER(education_raw) IN ('uneducated', 'school') THEN 'Basic Education'
  WHEN LOWER(education_raw) IN ('post graduate', 'graduate') THEN 'Higher Education'
  WHEN education_raw = 'Ph.D' THEN 'Doctoral'
  ELSE 'Unknown'
END;

-- Add ordering for consistent sorting in results
UPDATE dim_customer
SET education_group_order = CASE
  WHEN education_group = 'Basic Education' THEN 1
  WHEN education_group = 'Higher Education' THEN 2
  WHEN education_group = 'Doctoral' THEN 3
  ELSE 0
END;

-- ----------------------------------------------------------------------
-- Currency conversion from USD to INR (standardized to single currency)
-- ----------------------------------------------------------------------
UPDATE fact_order
SET sales_amount = sales_amount * 87,
    currency = 'INR'
WHERE LOWER(currency) = 'usd';

-- ----------------------------------------------------------------------
-- Clean fact_order rows with invalid sales_amount
-- ----------------------------------------------------------------------
DELETE FROM fact_order
WHERE sales_amount IN (0.0, -1.0);

-- ----------------------------------------------------------------------
-- Optional: Indexing after all updates to improve performance
-- ----------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_dim_customer_userid     ON dim_customer(user_id_nat);
CREATE INDEX IF NOT EXISTS ix_dim_restaurant_restid   ON dim_restaurant(restaurant_id_nat);
CREATE INDEX IF NOT EXISTS ix_fact_order_date         ON fact_order(date_key);
CREATE INDEX IF NOT EXISTS ix_fact_order_customer     ON fact_order(customer_key);
CREATE INDEX IF NOT EXISTS ix_fact_order_restaurant   ON fact_order(restaurant_key);
CREATE UNIQUE INDEX IF NOT EXISTS ux_fact_order_nk    ON fact_order(order_nk);
CREATE INDEX IF NOT EXISTS ix_dim_location_country    ON dim_location(country);
CREATE INDEX IF NOT EXISTS ix_dim_location_state      ON dim_location(state);
CREATE INDEX IF NOT EXISTS ix_dim_location_city       ON dim_location(city);

-- ----------------------------------------------------------------------
-- QA Checks — Validate row counts and FK consistency
-- ----------------------------------------------------------------------
SELECT 'fact_order' t, COUNT(*) c FROM fact_order
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_restaurant', COUNT(*) FROM dim_restaurant
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date;

SELECT 'date_fk_orphans' AS check_name, COUNT(*) AS rows
FROM fact_order f LEFT JOIN dim_date d ON f.date_key = d.date_key
WHERE d.date_key IS NULL
UNION ALL
SELECT 'customer_fk_orphans', COUNT(*)
FROM fact_order f LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL
UNION ALL
SELECT 'restaurant_fk_orphans', COUNT(*)
FROM fact_order f LEFT JOIN dim_restaurant r ON f.restaurant_key = r.restaurant_key
WHERE r.restaurant_key IS NULL;

SELECT COUNT(*) AS dim_location_rows FROM dim_location;

SELECT location_key, country, state, city
FROM dim_location
ORDER BY country, state, city
LIMIT 20;



