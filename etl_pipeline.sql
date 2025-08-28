/* =======================================================================
   Food Delivery Analytics - Star Schema ETL Pipeline
   
   This script creates a complete star schema data warehouse for food delivery
   analytics, including customer demographics, restaurant performance, and
   order behavior analysis.
   
   Author: [Bharath Chandran Madhaiyan]
   Created: [28-08-2025]
   Version: 1.0
   ======================================================================= */
/* =======================================================================
   0) SESSION & TRANSACTION
   ======================================================================= */
PRAGMA foreign_keys = ON;
BEGIN TRANSACTION;



/* ===================================================================================================================================
   1)  EXTRACT
   ==================================================================================================================================== */

/* =======================================================================
   1) STAR-SCHEMA TABLES (1 fact, 4 dims)
   - Natural IDs kept as TEXT for safety (leading zeros/mixed types)
   ======================================================================= */

-- DIM: Date
CREATE TABLE IF NOT EXISTS dim_date (
  date_key    INTEGER PRIMARY KEY,   -- yyyymmdd
  date_iso    TEXT UNIQUE,           -- 'YYYY-MM-DD'
  year        INTEGER,
  quarter     INTEGER,
  month       INTEGER,
  month_name  TEXT,
  day         INTEGER,
  dow         INTEGER,               -- 1=Mon..7=Sun
  is_weekend  INTEGER                -- 0/1
);

-- DIM: Customer
CREATE TABLE IF NOT EXISTS dim_customer (
  customer_key        INTEGER PRIMARY KEY,  -- surrogate
  user_id_nat         TEXT UNIQUE NOT NULL, -- stg_users.User_id
  name                TEXT,
  email               TEXT,
  age                 INTEGER,
  gender              TEXT,
  family_size         INTEGER,
  monthly_income_raw  TEXT,
  income_group        TEXT,
  income_group_order  INTEGER
);

---------------
-----Educational
------1. Data Preparation
-- First inspect distinct educational qualifications in source data
SELECT DISTINCT [Educational Qualifications]
FROM stg_users

-- Add columns for education analysis in customer dimension
ALTER TABLE dim_customer ADD COLUMN education_raw TEXT;
ALTER TABLE dim_customer ADD COLUMN education_group TEXT;
ALTER TABLE dim_customer ADD COLUMN education_group_order INTEGER;


---------------------


-- DIM: Restaurant
CREATE TABLE IF NOT EXISTS dim_restaurant (
  restaurant_key    INTEGER PRIMARY KEY,   -- surrogate
  restaurant_id_nat TEXT UNIQUE NOT NULL,  -- stg_restaurant.Id (or R_id)
  name              TEXT,
  city              TEXT,
  rating            REAL,
  cuisine           TEXT
);

-- dim_location was introduced later in the design.
-- To support this, we extracted distinct city values from dim_restaurant,
-- then used AI-assisted enrichment to infer the corresponding country and state.
-- The enriched data was used to create the new dim_location dimension table.

--Country, State, City datset (Dim: Locations)
CREATE TABLE IF NOT EXISTS dim_location (
  location_key  INTEGER PRIMARY KEY,               -- surrogate key
  location_nk   TEXT UNIQUE NOT NULL,              -- Country|State|City
  country       TEXT NOT NULL,
  state         TEXT,
  city          TEXT NOT NULL
);


-- FACT: Orders (order-level grain)
CREATE TABLE IF NOT EXISTS fact_order (
  order_key       INTEGER PRIMARY KEY,
  order_nk        TEXT UNIQUE,        -- deterministic natural key (we’ll build it)
  date_key        INTEGER NOT NULL,   -- FK -> dim_date
  customer_key    INTEGER NOT NULL,   -- FK -> dim_customer
  restaurant_key  INTEGER NOT NULL,   -- FK -> dim_restaurant
  sales_qty       INTEGER NOT NULL,
  sales_amount    REAL    NOT NULL,
  currency        TEXT,               -- degenerate attribute

  FOREIGN KEY (date_key)       REFERENCES dim_date(date_key),
  FOREIGN KEY (customer_key)   REFERENCES dim_customer(customer_key),
  FOREIGN KEY (restaurant_key) REFERENCES dim_restaurant(restaurant_key)
);

--dim_location table was added after everything was setup, hence below code references teh foreign key to fact table.
--Run Only Once
ALTER TABLE fact_order ADD COLUMN location_key INTEGER REFERENCES dim_location(location_key);


/* ===================================================================================================================================
   2) LOAD & TRANSFORMATION
   ==================================================================================================================================== */

/* =======================================================================
   2) BUILD A STABLE NATURAL KEY FOR ORDERS
   - Same input row → same key on every load; safe for re-runs
   ======================================================================= */
CREATE VIEW IF NOT EXISTS v_stg_orders_nk AS
SELECT
  printf('%s|%s|%s|%s|%s|%s',
         CAST(user_id AS TEXT),
         CAST(r_id    AS TEXT),
         strftime('%Y-%m-%d %H:%M:%S', order_date),
         TRIM(IFNULL(Currency,'')),
         TRIM(CAST(sales_qty    AS TEXT)),
         TRIM(CAST(sales_amount AS TEXT))) AS order_nk,
  *
FROM stg_orders;

/* =======================================================================
   3) DIM DATE — INSERT-OR-IGNORE then UPDATE existing rows
   ======================================================================= */

-- Precompute all needed calendar rows from staging orders
CREATE TEMP VIEW IF NOT EXISTS v_new_dates AS
SELECT DISTINCT
  CAST(strftime('%Y%m%d', o.order_date) AS INTEGER)               AS date_key,
  substr(o.order_date, 1, 10)                                      AS date_iso,
  CAST(strftime('%Y',  o.order_date) AS INTEGER)                   AS year,
  ((CAST(strftime('%m', o.order_date) AS INTEGER)-1)/3)+1          AS quarter,
  CAST(strftime('%m',  o.order_date) AS INTEGER)                   AS month,
  CASE strftime('%m', o.order_date)
    WHEN '01' THEN 'Jan' WHEN '02' THEN 'Feb' WHEN '03' THEN 'Mar'
    WHEN '04' THEN 'Apr' WHEN '05' THEN 'May' WHEN '06' THEN 'Jun'
    WHEN '07' THEN 'Jul' WHEN '08' THEN 'Aug' WHEN '09' THEN 'Sep'
    WHEN '10' THEN 'Oct' WHEN '11' THEN 'Nov' WHEN '12' THEN 'Dec'
  END                                                               AS month_name,
  CAST(strftime('%d', o.order_date) AS INTEGER)                    AS day,
  CAST(strftime('%w', o.order_date) AS INTEGER)+1                  AS dow,
  CASE WHEN CAST(strftime('%w', o.order_date) AS INTEGER) IN (0,6) THEN 1 ELSE 0 END AS is_weekend
FROM stg_orders o;

-- Insert any missing dates
INSERT OR IGNORE INTO dim_date
(date_key, date_iso, year, quarter, month, month_name, day, dow, is_weekend)
SELECT date_key, date_iso, year, quarter, month, month_name, day, dow, is_weekend
FROM v_new_dates;

-- Refresh existing date attributes (idempotent)
UPDATE dim_date
SET date_iso   = (SELECT nd.date_iso   FROM v_new_dates nd WHERE nd.date_key = dim_date.date_key),
    year       = (SELECT nd.year       FROM v_new_dates nd WHERE nd.date_key = dim_date.date_key),
    quarter    = (SELECT nd.quarter    FROM v_new_dates nd WHERE nd.date_key = dim_date.date_key),
    month      = (SELECT nd.month      FROM v_new_dates nd WHERE nd.date_key = dim_date.date_key),
    month_name = (SELECT nd.month_name FROM v_new_dates nd WHERE nd.date_key = dim_date.date_key),
    day        = (SELECT nd.day        FROM v_new_dates nd WHERE nd.date_key = dim_date.date_key),
    dow        = (SELECT nd.dow        FROM v_new_dates nd WHERE nd.date_key = dim_date.date_key),
    is_weekend = (SELECT nd.is_weekend FROM v_new_dates nd WHERE nd.date_key = dim_date.date_key)
WHERE EXISTS (SELECT 1 FROM v_new_dates nd WHERE nd.date_key = dim_date.date_key);

/* =======================================================================
   4) DIM CUSTOMER — INSERT-OR-IGNORE then UPDATE (incl. enrichment)
   ======================================================================= */

-- Insert any new customers
INSERT OR IGNORE INTO dim_customer
(user_id_nat, name, email, age, gender, family_size, monthly_income_raw)
SELECT DISTINCT
  CAST(u.user_id AS TEXT),
  u.name,
  u.email,
  CAST(u.Age AS INTEGER),
  u.Gender,
  CAST(u."Family size" AS INTEGER),
  u."Monthly Income"
FROM stg_users u;

-- Refresh changing attributes
UPDATE dim_customer
SET name               = (SELECT u.name            FROM stg_users u WHERE CAST(u.user_id AS TEXT) = dim_customer.user_id_nat),
    email              = (SELECT u.email           FROM stg_users u WHERE CAST(u.user_id AS TEXT) = dim_customer.user_id_nat),
    age                = (SELECT CAST(u.Age AS INTEGER) FROM stg_users u WHERE CAST(u.user_id AS TEXT) = dim_customer.user_id_nat),
    gender             = (SELECT u.Gender          FROM stg_users u WHERE CAST(u.user_id AS TEXT) = dim_customer.user_id_nat),
    family_size        = (SELECT CAST(u."Family size" AS INTEGER) FROM stg_users u WHERE CAST(u.user_id AS TEXT) = dim_customer.user_id_nat),
    monthly_income_raw = (SELECT u."Monthly Income" FROM stg_users u WHERE CAST(u.user_id AS TEXT) = dim_customer.user_id_nat)
WHERE EXISTS (SELECT 1 FROM stg_users u WHERE CAST(u.user_id AS TEXT) = dim_customer.user_id_nat);

-- Recompute income buckets (idempotent) # tranforming income_group and income_group_order
UPDATE dim_customer
SET
  income_group = CASE
    WHEN REPLACE(REPLACE(REPLACE(LOWER(monthly_income_raw),' ',''),'.',''),',','') LIKE 'noincome%'       THEN 'None'
    WHEN REPLACE(REPLACE(REPLACE(LOWER(monthly_income_raw),' ',''),'.',''),',','') LIKE '%below%10000%'   THEN 'Below 10k'
    WHEN REPLACE(REPLACE(REPLACE(LOWER(monthly_income_raw),' ',''),'.',''),',','') LIKE '%10001to25000%'  THEN '10k-25k'
    WHEN REPLACE(REPLACE(REPLACE(LOWER(monthly_income_raw),' ',''),'.',''),',','') LIKE '%25001to50000%'  THEN '25k-50k'
    WHEN REPLACE(REPLACE(REPLACE(LOWER(monthly_income_raw),' ',''),'.',''),',','') LIKE '%morethan50000%' THEN '50k+'
    ELSE 'Unknown'
  END,
  income_group_order = CASE
    WHEN income_group = 'None'       THEN 0
    WHEN income_group = 'Below 10k'  THEN 1
    WHEN income_group = '10k-25k'    THEN 2
    WHEN income_group = '25k-50k'    THEN 3
    WHEN income_group = '50k+'       THEN 4
    ELSE 9
  END;
  
  
  
-- Populate education data from staging table
UPDATE dim_customer
SET education_raw = (
  SELECT u.[Educational qualifications]
  FROM stg_users u
  WHERE u.User_id = dim_customer.user_id_nat
);

-- Categorize education levels into 3 groups:
--- group1. Basic Education: Uneducated, School
--- group2. Higher Education: post-graduate, graduate
--- group3. Doctoral: PhD
UPDATE dim_customer
SET education_group = CASE
  WHEN LOWER(education_raw) IN ('uneducated', 'school') THEN 'Basic Education'
  WHEN LOWER(education_raw) IN ('post-graduate', 'graduate') THEN 'Higher Education'
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

/* =======================================================================
   5) DIM RESTAURANT — INSERT-OR-IGNORE then UPDATE
   ======================================================================= */

INSERT OR IGNORE INTO dim_restaurant
(restaurant_id_nat, name, city, rating, cuisine)
SELECT DISTINCT
  CAST(r.id AS TEXT),
  r.name,
  r.city,
  CAST(r.rating AS REAL),
  r.cuisine
FROM stg_restaurant r;

UPDATE dim_restaurant
SET name   = (SELECT r.name   FROM stg_restaurant r WHERE CAST(r.id AS TEXT) = dim_restaurant.restaurant_id_nat),
    city   = (SELECT r.city   FROM stg_restaurant r WHERE CAST(r.id AS TEXT) = dim_restaurant.restaurant_id_nat),
    rating = (SELECT CAST(r.rating AS REAL) FROM stg_restaurant r WHERE CAST(r.id AS TEXT) = dim_restaurant.restaurant_id_nat),
    cuisine= (SELECT r.cuisine FROM stg_restaurant r WHERE CAST(r.id AS TEXT) = dim_restaurant.restaurant_id_nat)
WHERE EXISTS (SELECT 1 FROM stg_restaurant r WHERE CAST(r.id AS TEXT) = dim_restaurant.restaurant_id_nat);


-- Added new location_key from dim_location-- run the below code only once to create a column
ALTER table if not EXISTS dim_restaurant
ADD COLUMN location_key text

--updating the dim_restaurant using location_key on dim_restaurant from dim_location to create FOREIGN KEY
UPDATE dim_restaurant AS dr
SET location_key = (
  SELECT dl.location_key
  FROM dim_location AS dl
  WHERE dr.city LIKE '%' || dl.city || '%' COLLATE NOCASE
  LIMIT 1
)
WHERE dr.city IS NOT NULL
  AND dr.location_key IS NULL;
  
  
  ----------
  drop table dim_restaurant drop COLUMN location_key
  -------------
  
  ALTER TABLE dim_restaurant RENAME TO dim_restaurant_old;

  CREATE TABLE dim_restaurant (
  restaurant_key    INTEGER PRIMARY KEY,
  restaurant_id_nat TEXT UNIQUE NOT NULL,
  name              TEXT,
  city              TEXT,
  rating            REAL,
  cuisine           TEXT
);

INSERT INTO dim_restaurant (restaurant_key, restaurant_id_nat, name, city, rating, cuisine)
SELECT restaurant_key, restaurant_id_nat, name, city, rating, cuisine
FROM dim_restaurant_old;



/* =======================================================================
   5) DIM LOCATION — INSERT-OR-IGNORE then UPDATE
   ======================================================================= */


INSERT OR IGNORE INTO dim_location (location_nk, country, state, city)
SELECT
  printf('%s|%s|%s',
         "Country",
         IFNULL("State",''),
         "City")                                  AS location_nk,
  "Country"                                       AS country,
  "State"                                         AS state,
  "City"                                          AS city
FROM stg_location
GROUP BY "Country", "State", "City";

/* Refresh attributes for EXISTING locations (idempotent) */
UPDATE dim_location
SET country = (
      SELECT s."Country"
      FROM stg_location s
      WHERE printf('%s|%s|%s', s."Country", IFNULL(s."State",''), s."City") = dim_location.location_nk
      LIMIT 1
    ),
    state = (
      SELECT s."State"
      FROM stg_location s
      WHERE printf('%s|%s|%s', s."Country", IFNULL(s."State",''), s."City") = dim_location.location_nk
      LIMIT 1
    ),
    city = (
      SELECT s."City"
      FROM stg_location s
      WHERE printf('%s|%s|%s', s."Country", IFNULL(s."State",''), s."City") = dim_location.location_nk
      LIMIT 1
    )
WHERE EXISTS (
  SELECT 1
  FROM stg_location s
  WHERE printf('%s|%s|%s', s."Country", IFNULL(s."State",''), s."City") = dim_location.location_nk
);


   
/* =======================================================================
   6) FACT LOAD (update existing rows, then insert new ones)
   - Build a load view that already resolves surrogate keys
   ======================================================================= */

CREATE TEMP VIEW IF NOT EXISTS v_fact_load AS
SELECT
  v.order_nk,
  CAST(strftime('%Y%m%d', v.order_date) AS INTEGER) AS date_key,
  dc.customer_key,
  dr.restaurant_key,
  CAST(v.sales_qty    AS INTEGER) AS sales_qty,
  CAST(v.sales_amount AS REAL)    AS sales_amount,
  TRIM(v.currency)                AS currency   -- Transformation of Currency
FROM v_stg_orders_nk v
JOIN dim_customer   dc ON dc.user_id_nat        = CAST(v.user_id AS TEXT)
JOIN dim_restaurant dr ON dr.restaurant_id_nat  = CAST(v.r_id    AS TEXT)
JOIN dim_date       dd ON dd.date_key           = CAST(strftime('%Y%m%d', v.order_date) AS INTEGER);

-- 6A) UPDATE existing fact rows (idempotent refresh)
UPDATE fact_order
SET date_key        = (SELECT fl.date_key       FROM v_fact_load fl WHERE fl.order_nk = fact_order.order_nk),
    customer_key    = (SELECT fl.customer_key   FROM v_fact_load fl WHERE fl.order_nk = fact_order.order_nk),
    restaurant_key  = (SELECT fl.restaurant_key FROM v_fact_load fl WHERE fl.order_nk = fact_order.order_nk),
    sales_qty       = (SELECT fl.sales_qty      FROM v_fact_load fl WHERE fl.order_nk = fact_order.order_nk),
    sales_amount    = (SELECT fl.sales_amount   FROM v_fact_load fl WHERE fl.order_nk = fact_order.order_nk),
    currency        = (SELECT fl.currency       FROM v_fact_load fl WHERE fl.order_nk = fact_order.order_nk)
WHERE EXISTS (SELECT 1 FROM v_fact_load fl WHERE fl.order_nk = fact_order.order_nk);

-- 6B) INSERT new fact rows (ignore duplicates)
INSERT OR IGNORE INTO fact_order
(order_nk, date_key, customer_key, restaurant_key, sales_qty, sales_amount, currency)
SELECT order_nk, date_key, customer_key, restaurant_key, sales_qty, sales_amount, currency
FROM v_fact_load;


--6C) Loading location_key FOREIGN key to fact_order table from dim_location TABLE
UPDATE fact_order AS f
SET location_key = (
  SELECT dl.location_key
  FROM dim_restaurant AS dr
  JOIN dim_location  AS dl ON dr.city LIKE '%' || dl.city || '%' COLLATE NOCASE
  WHERE dr.restaurant_key = f.restaurant_key
  LIMIT 1
)
WHERE f.location_key IS NULL;

/* =======================================================================
   6.b)  Transformation of Currency from USD to INR
   ======================================================================= */
UPDATE fact_order
SET sales_amount = sales_amount * 87,
    currency = 'INR'
WHERE LOWER(currency) = 'usd';

/* =======================================================================
   7) INDEXES (safe to re-run)
   ======================================================================= */
CREATE INDEX IF NOT EXISTS ix_dim_customer_userid     ON dim_customer(user_id_nat);
CREATE INDEX IF NOT EXISTS ix_dim_restaurant_restid   ON dim_restaurant(restaurant_id_nat);
CREATE INDEX IF NOT EXISTS ix_fact_order_date         ON fact_order(date_key);
CREATE INDEX IF NOT EXISTS ix_fact_order_customer     ON fact_order(customer_key);
CREATE INDEX IF NOT EXISTS ix_fact_order_restaurant   ON fact_order(restaurant_key);
CREATE UNIQUE INDEX IF NOT EXISTS ux_fact_order_nk    ON fact_order(order_nk);

/* Helpful indexes for filtering */
CREATE INDEX IF NOT EXISTS ix_dim_location_country ON dim_location(country);
CREATE INDEX IF NOT EXISTS ix_dim_location_state   ON dim_location(state);
CREATE INDEX IF NOT EXISTS ix_dim_location_city    ON dim_location(city);



COMMIT;

/* =======================================================================
   8) QUICK CHECKS
   ======================================================================= */
-- Counts
SELECT 'fact_order' t, COUNT(*) c FROM fact_order
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_restaurant', COUNT(*) FROM dim_restaurant
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date;

-- FK orphans (should be zero)
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

-- How many locations loaded?
SELECT COUNT(*) AS dim_location_rows FROM dim_location;

-- Sample rows
SELECT location_key, country, state, city
FROM dim_location
ORDER BY country, state, city
LIMIT 20;

--checking whether the joining of location_key is working good with joins
select dr.location_key, dr.name, dl.country, dl.state, dl.city
from dim_restaurant dr
inner join dim_location dl
on dr.location_key = dl.location_key

--- Cleaned data where sales_amount = -1 or 0.0 INR
delete from fact_order
where sales_amount = 0.0 or sales_amount = -1.0

