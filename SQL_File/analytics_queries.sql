/* =======================================================================
   Food Delivery Analytics - Business Intelligence Queries
   
   This file contains analytical queries for the food delivery star schema,
   organized by research questions and business objectives.
   
   Author: [Your Name]
   Created: [Date]
   Version: 1.0
   ======================================================================= */

/* ===================================================================================================================================
   RESEARCH QUESTION 1: Customer Demographics Analysis
   How do demographics (income, education) correlate with order frequency, cuisine preference, and spending behavior?
   ==================================================================================================================================== */

-- RQ1.1: Customer Distribution by Income Group
SELECT 
    income_group,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dim_customer), 2) AS percentage
FROM dim_customer
GROUP BY income_group, income_group_order
ORDER BY income_group_order;

-- RQ1.2: Income vs. Order Frequency Analysis
SELECT 
    c.income_group,
    COUNT(DISTINCT c.customer_key) AS customer_count,
    COUNT(f.order_key) AS total_orders,
    ROUND(COUNT(f.order_key) * 1.0 / COUNT(DISTINCT c.customer_key), 2) AS avg_orders_per_customer
FROM dim_customer c
LEFT JOIN fact_order f ON c.customer_key = f.customer_key
GROUP BY c.income_group, c.income_group_order
ORDER BY c.income_group_order;

-- RQ1.3: Income vs. Spending Behavior
SELECT 
    c.income_group,
    COUNT(f.order_key) AS order_count,
    ROUND(SUM(f.sales_amount), 2) AS total_spending,
    ROUND(SUM(f.sales_amount) / COUNT(f.order_key), 2) AS avg_order_value,
    ROUND(SUM(f.sales_amount) / COUNT(DISTINCT c.customer_key), 2) AS avg_spending_per_customer
FROM dim_customer c
LEFT JOIN fact_order f ON c.customer_key = f.customer_key
GROUP BY c.income_group, c.income_group_order
ORDER BY c.income_group_order;

-- RQ1.4: Top 5 Cuisines by Income Group
WITH split_cuisines AS (
  SELECT 
    f.customer_key,
    trim(j.value) AS single_cuisine
  FROM fact_order f
  JOIN dim_restaurant r ON f.restaurant_key = r.restaurant_key
  JOIN json_each('["' || replace(r.cuisine, ',', '","') || '"]') j
), cuisine_counts AS (
  SELECT 
    c.income_group,
    c.income_group_order,
    sc.single_cuisine,
    COUNT(*) AS order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY c.income_group), 2) AS percentage,
    ROW_NUMBER() OVER (PARTITION BY c.income_group ORDER BY COUNT(*) DESC) AS rank_in_group
  FROM split_cuisines sc
  JOIN dim_customer c ON sc.customer_key = c.customer_key
  GROUP BY c.income_group, sc.single_cuisine, c.income_group_order
)
SELECT 
  income_group,
  single_cuisine,
  order_count,
  percentage
FROM cuisine_counts
WHERE rank_in_group <= 5
ORDER BY income_group_order, order_count DESC;

-- RQ1.5: Education Level vs. Order Behavior
SELECT 
    c.education_group,
    COUNT(DISTINCT c.customer_key) AS customer_count,
    COUNT(f.order_key) AS total_orders,
    ROUND(COUNT(f.order_key)*1.0 / NULLIF(COUNT(DISTINCT c.customer_key), 0), 2) AS avg_orders_per_customer,
    SUM(f.sales_amount) AS total_sales,
    ROUND(SUM(f.sales_amount) / NULLIF(COUNT(DISTINCT c.customer_key), 0), 2) AS avg_spend_per_customer
FROM fact_order f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.education_group, c.education_group_order
ORDER BY c.education_group_order;

-- RQ1.6: Top 5 Cuisines by Education Level (handling multi-cuisine restaurants)
WITH RECURSIVE SplitCuisines AS (
    SELECT 
        c.education_group,
        c.education_group_order,
        SUBSTR(r.cuisine, 1, INSTR(r.cuisine || ',', ',') - 1) AS single_cuisine,
        SUBSTR(r.cuisine, INSTR(r.cuisine || ',', ',') + 1) AS remaining,
        f.order_key
    FROM dim_customer c
    JOIN fact_order f ON c.customer_key = f.customer_key
    JOIN dim_restaurant r ON f.restaurant_key = r.restaurant_key
    WHERE r.cuisine IS NOT NULL
    
    UNION ALL
    
    SELECT 
        education_group,
        education_group_order,
        SUBSTR(remaining, 1, INSTR(remaining || ',', ',') - 1),
        SUBSTR(remaining, INSTR(remaining || ',', ',') + 1),
        order_key
    FROM SplitCuisines
    WHERE remaining != ''
),
RankedCuisines AS (
    SELECT 
        education_group,
        education_group_order,
        TRIM(single_cuisine) AS single_cuisine,
        COUNT(DISTINCT order_key) AS order_count,
        ROW_NUMBER() OVER (
            PARTITION BY education_group 
            ORDER BY COUNT(DISTINCT order_key) DESC
        ) AS cuisine_rank
    FROM SplitCuisines
    WHERE single_cuisine != ''
    GROUP BY education_group, education_group_order, TRIM(single_cuisine)
)
SELECT 
    education_group,
    single_cuisine AS top_cuisine,
    order_count,
    ROUND(order_count * 100.0 / SUM(order_count) OVER (
        PARTITION BY education_group
    ), 2) AS percentage
FROM RankedCuisines
WHERE cuisine_rank <= 5
ORDER BY education_group_order, cuisine_rank;

-- RQ1.7: Weekend vs Weekday Ordering by Education Level
SELECT 
    c.education_group,
    CASE WHEN d.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(f.order_key) AS order_count,
    ROUND(COUNT(f.order_key)*100.0/SUM(COUNT(f.order_key)) OVER (PARTITION BY c.education_group), 2) AS percentage
FROM fact_order f
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY c.education_group, d.is_weekend
ORDER BY c.education_group_order, d.is_weekend;

/* ===================================================================================================================================
   RESEARCH QUESTION 2: Restaurant Performance Analysis
   Top restaurants by rating & sales, with performance drivers and seasonal analysis
   ==================================================================================================================================== */

-- RQ2.1: Top 10 Restaurants by Sales and Rating
SELECT
    r.name AS restaurant_name,
    l.city,
    l.country,
    r.cuisine,
    AVG(r.rating) AS avg_rating,
    COUNT(o.order_key) AS total_orders,
    SUM(o.sales_amount) AS total_sales,
    AVG(o.sales_amount) AS avg_order_value
FROM fact_order o
JOIN dim_restaurant r ON o.restaurant_key = r.restaurant_key
JOIN dim_date d ON d.date_key = o.date_key
JOIN dim_location l ON l.location_key = o.location_key
GROUP BY r.name, l.city, l.country, r.cuisine, r.rating
ORDER BY total_sales DESC, avg_rating DESC
LIMIT 10;

-- RQ2.2: Top Performing Restaurants During Diwali Season (Oct-Nov)
WITH yearly_sales AS (
    SELECT  
        r.restaurant_key,
        r.name AS restaurant_name,
        l.city AS city,
        r.cuisine,
        r.rating,
        d.year,
        COUNT(o.order_key) AS total_orders,
        SUM(o.sales_amount) AS total_sales 
    FROM fact_order o
    JOIN dim_restaurant r ON o.restaurant_key = r.restaurant_key
    JOIN dim_date d ON o.date_key = d.date_key
    JOIN dim_location l ON l.location_key = o.location_key
    WHERE d.year IN (2017, 2018, 2019)
      AND d.month IN (10, 11)
    GROUP BY r.restaurant_key, r.name, l.city, r.cuisine, r.rating, d.year
)
SELECT *
FROM yearly_sales ys
WHERE ys.total_sales = (
    SELECT MAX(sub.total_sales)
    FROM yearly_sales sub
    WHERE sub.year = ys.year
)
ORDER BY ys.year DESC;

-- RQ2.3: Restaurant Performance by Rating Tiers
SELECT 
    CASE 
        WHEN r.rating >= 4.5 THEN 'Excellent (4.5+)'
        WHEN r.rating >= 4.0 THEN 'Very Good (4.0-4.4)'
        WHEN r.rating >= 3.5 THEN 'Good (3.5-3.9)'
        WHEN r.rating >= 3.0 THEN 'Average (3.0-3.4)'
        ELSE 'Below Average (<3.0)'
    END AS rating_tier,
    COUNT(DISTINCT r.restaurant_key) AS restaurant_count,
    COUNT(f.order_key) AS total_orders,
    SUM(f.sales_amount) AS total_revenue,
    ROUND(AVG(f.sales_amount), 2) AS avg_order_value
FROM dim_restaurant r
JOIN fact_order f ON r.restaurant_key = f.restaurant_key
GROUP BY rating_tier
ORDER BY avg_order_value DESC;

-- RQ2.4: Cuisine Performance Analysis
SELECT 
    r.cuisine,
    COUNT(DISTINCT r.restaurant_key) AS restaurant_count,
    COUNT(f.order_key) AS total_orders,
    SUM(f.sales_amount) AS total_revenue,
    ROUND(AVG(f.sales_amount), 2) AS avg_order_value,
    ROUND(AVG(r.rating), 2) AS avg_restaurant_rating
FROM dim_restaurant r
JOIN fact_order f ON r.restaurant_key = f.restaurant_key
GROUP BY r.cuisine
ORDER BY total_revenue DESC
LIMIT 15;

/* ===================================================================================================================================
   RESEARCH QUESTION 3: Family Size & Income Interaction Analysis
   Family size × income × weekday/weekend behavior patterns
   ==================================================================================================================================== */

-- RQ3.1: Create analytical view for family behavior analysis
CREATE VIEW IF NOT EXISTS v_family_analysis AS
SELECT 
    fo.order_key, 
    fo.sales_qty, 
    fo.sales_amount,
    CASE
        WHEN dc.family_size = 1           THEN '1 member'
        WHEN dc.family_size = 2           THEN '2 members'
        WHEN dc.family_size = 3           THEN '3 members (one child)'
        WHEN dc.family_size IS NULL       THEN 'Unknown'
        ELSE '4+ members (>=2 children)'
    END AS family_group,
    CASE
        WHEN dc.income_group IN ('None','Below 10k')            THEN 'Low'
        WHEN dc.income_group IN ('10k-25k','25k-50k')           THEN 'Middle'
        WHEN dc.income_group = '50k+'                           THEN 'High'
        ELSE 'Unknown'
    END AS income_level,
    dd.is_weekend,
    CASE 
        WHEN (strftime('%m', dd.date_iso) = '01' AND strftime('%d', dd.date_iso) = '01') THEN 1  -- New Year's Day
        WHEN (strftime('%m', dd.date_iso) = '02' AND strftime('%d', dd.date_iso) = '14') THEN 1  -- Valentine's Day
        WHEN (strftime('%m', dd.date_iso) = '05' AND strftime('%d', dd.date_iso) = '01') THEN 1  -- Labour Day
        WHEN (strftime('%m', dd.date_iso) = '12' AND strftime('%d', dd.date_iso) = '25') THEN 1  -- Christmas
        WHEN (strftime('%m', dd.date_iso) = '12' AND strftime('%d', dd.date_iso) = '31') THEN 1  -- New Year's Eve
        ELSE 0 
    END AS is_holiday,
    dr.cuisine
FROM fact_order fo
JOIN dim_customer dc ON fo.customer_key = dc.customer_key
JOIN dim_date dd ON fo.date_key = dd.date_key
JOIN dim_restaurant dr ON fo.restaurant_key = dr.restaurant_key;

-- RQ3.2: Spending Behavior by Family Size and Income During Weekdays/Weekends
SELECT 
    family_group, 
    income_level, 
    CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(order_key) AS orders,
    SUM(sales_qty) AS total_sales_quantity, 
    SUM(sales_amount) AS total_sales_amount,
    ROUND(AVG(sales_amount),2) AS avg_order_value
FROM v_family_analysis 
GROUP BY family_group, income_level, is_weekend 
ORDER BY total_sales_amount DESC, total_sales_quantity DESC;

-- RQ3.3: Holiday vs Regular Day Spending Patterns
SELECT 
    family_group, 
    income_level, 
    CASE WHEN is_holiday = 1 THEN 'Holiday' ELSE 'Regular Day' END AS day_type,
    COUNT(order_key) AS orders,
    SUM(sales_qty) AS total_sales_quantity, 
    SUM(sales_amount) AS total_sales_amount,
    ROUND(AVG(sales_amount),2) AS avg_order_value
FROM v_family_analysis
GROUP BY family_group, income_level, is_holiday
ORDER BY total_sales_amount DESC, total_sales_quantity DESC;

-- RQ3.4: Cuisine Preferences by Family Demographics (Weekend vs Weekday)
SELECT
    cuisine,
    family_group,
    income_level,
    CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(order_key) AS orders,
    SUM(sales_qty) AS total_sales_quantity,
    SUM(sales_amount) AS total_sales_amount,
    ROUND(AVG(sales_amount),2) AS avg_order_value
FROM v_family_analysis
WHERE cuisine IS NOT NULL 
    AND is_weekend IS NOT NULL 
    AND sales_qty IS NOT NULL 
    AND sales_amount IS NOT NULL
GROUP BY cuisine, family_group, income_level, is_weekend
ORDER BY total_sales_amount DESC
LIMIT 20;

-- RQ3.5: Top Family Segments by Revenue Contribution
SELECT 
    family_group,
    income_level,
    COUNT(DISTINCT order_key) AS total_orders,
    SUM(sales_amount) AS total_revenue,
    ROUND(AVG(sales_amount), 2) AS avg_order_value,
    ROUND(SUM(sales_amount) * 100.0 / (SELECT SUM(sales_amount) FROM v_family_analysis), 2) AS revenue_percentage
FROM v_family_analysis
GROUP BY family_group, income_level
ORDER BY total_revenue DESC;

/* ===================================================================================================================================
   BUSINESS INSIGHTS & ADVANCED ANALYTICS
   Additional queries for deeper business intelligence
   ==================================================================================================================================== */

-- BI1: Monthly Revenue Trends
SELECT 
    d.year,
    d.month,
    d.month_name,
    COUNT(f.order_key) AS total_orders,
    SUM(f.sales_amount) AS total_revenue,
    ROUND(AVG(f.sales_amount), 2) AS avg_order_value,
    COUNT(DISTINCT f.customer_key) AS unique_customers
FROM fact_order f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- BI2: Customer Loyalty Analysis (Repeat Purchase Behavior)
SELECT 
    customer_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(total_orders), 1) AS avg_orders_per_customer,
    ROUND(AVG(total_spent), 2) AS avg_total_spent,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value
FROM (
    SELECT 
        c.customer_key,
        CASE 
            WHEN COUNT(f.order_key) = 1 THEN 'One-time'
            WHEN COUNT(f.order_key) BETWEEN 2 AND 5 THEN 'Occasional'
            WHEN COUNT(f.order_key) BETWEEN 6 AND 15 THEN 'Regular'
            ELSE 'Frequent'
        END AS customer_segment,
        COUNT(f.order_key) AS total_orders,
        SUM(f.sales_amount) AS total_spent,
        AVG(f.sales_amount) AS avg_order_value
    FROM dim_customer c
    LEFT JOIN fact_order f ON c.customer_key = f.customer_key
    WHERE f.order_key IS NOT NULL
    GROUP BY c.customer_key
) customer_stats
GROUP BY customer_segment
ORDER BY avg_total_spent DESC;

-- BI3: Geographic Performance Analysis
SELECT 
    l.country,
    l.state,
    l.city,
    COUNT(DISTINCT r.restaurant_key) AS restaurant_count,
    COUNT(f.order_key) AS total_orders,
    SUM(f.sales_amount) AS total_revenue,
    ROUND(AVG(f.sales_amount), 2) AS avg_order_value,
    ROUND(AVG(r.rating), 2) AS avg_restaurant_rating
FROM fact_order f
JOIN dim_restaurant r ON f.restaurant_key = r.restaurant_key
JOIN dim_location l ON f.location_key = l.location_key
GROUP BY l.country, l.state, l.city
ORDER BY total_revenue DESC
LIMIT 20;

-- BI4: Seasonal Pattern Analysis
SELECT 
    d.quarter,
    d.month_name,
    COUNT(f.order_key) AS total_orders,
    SUM(f.sales_amount) AS total_revenue,
    ROUND(AVG(f.sales_amount), 2) AS avg_order_value,
    ROUND(SUM(f.sales_amount) * 100.0 / SUM(SUM(f.sales_amount)) OVER (), 2) AS revenue_percentage
FROM fact_order f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.quarter, d.month, d.month_name
ORDER BY d.quarter, d.month;

-- BI5: Customer Acquisition Timeline
SELECT 
    first_order_year,
    first_order_month,
    COUNT(*) AS new_customers,
    SUM(COUNT(*)) OVER (ORDER BY first_order_year, first_order_month) AS cumulative_customers
FROM (
    SELECT 
        c.customer_key,
        MIN(d.year) AS first_order_year,
        MIN(d.month) AS first_order_month
    FROM dim_customer c
    JOIN fact_order f ON c.customer_key = f.customer_key
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY c.customer_key
) first_orders
GROUP BY first_order_year, first_order_month
ORDER BY first_order_year, first_order_month;
