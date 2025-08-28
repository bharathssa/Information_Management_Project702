# Food Delivery Analytics - Star Schema Data Warehouse

A comprehensive SQLite-based data warehouse implementing a star schema design for food delivery analytics, featuring customer demographics, restaurant performance, and order behavior analysis.

## 📊 Project Overview

This project transforms raw food delivery data into a structured star schema data warehouse, enabling sophisticated analytics on customer behavior, restaurant performance, and market trends.

### Key Features
- **Star Schema Design**: Optimized for analytical queries with 1 fact table and 4 dimension tables
- **Comprehensive ETL Pipeline**: Extract, transform, and load processes with data quality checks
- **Advanced Analytics**: Customer segmentation, restaurant performance analysis, and trend identification
- **Data Enrichment**: Income grouping, education categorization, and location hierarchies

## 🏗️ Schema Architecture

### Fact Table
- **`fact_order`**: Central fact table storing order transactions with measures like sales quantity, sales amount, and currency

### Dimension Tables
- **`dim_customer`**: Customer demographics including income groups, education levels, and family size
- **`dim_restaurant`**: Restaurant information with ratings, cuisine types, and locations
- **`dim_date`**: Date dimension with calendar attributes and weekend flags
- **`dim_location`**: Geographic hierarchy with country, state, and city information

![Star Schema Diagram](schema_diagram.png)

## 📈 Analytics Capabilities

### 1. Customer Demographics Analysis
- Income vs. order frequency and spending behavior
- Education level impact on cuisine preferences
- Family size correlation with order patterns

### 2. Restaurant Performance Analysis
- Top-performing restaurants by rating and sales
- Seasonal performance during festivals (Diwali analysis)
- Cuisine popularity trends

### 3. Behavioral Pattern Analysis
- Weekend vs. weekday ordering patterns
- Holiday spending behavior
- Family size and income interaction effects

## 🚀 Getting Started

### Prerequisites
- SQLite 3.x
- SQL client (DB Browser for SQLite, DBeaver, or similar)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/food-delivery-analytics.git
cd food-delivery-analytics
```

2. Set up the database:
```bash
# Create a new SQLite database
sqlite3 food_delivery.db

# Execute the schema and ETL script
.read etl_pipeline.sql
```

3. Load your staging data:
   - Import your CSV files into the staging tables (`stg_users`, `stg_restaurant`, `stg_orders`, `stg_location`)

## 📊 Sample Queries

### Customer Segmentation by Income
```sql
SELECT 
    income_group,
    COUNT(*) AS customer_count,
    ROUND(AVG(sales_amount), 2) AS avg_spending
FROM dim_customer c
JOIN fact_order f ON c.customer_key = f.customer_key
GROUP BY income_group
ORDER BY avg_spending DESC;
```

### Top Restaurants Analysis
```sql
SELECT
    r.name AS restaurant_name,
    l.city,
    r.cuisine,
    AVG(r.rating) AS avg_rating,
    SUM(o.sales_amount) AS total_sales
FROM fact_order o
JOIN dim_restaurant r ON o.restaurant_key = r.restaurant_key
JOIN dim_location l ON l.location_key = o.location_key
GROUP BY r.name, l.city, r.cuisine
ORDER BY total_sales DESC
LIMIT 10;
```

## 📋 Data Dictionary

| Table | Column | Type | Description |
|-------|--------|------|-------------|
| fact_order | order_key | INTEGER | Primary key |
| fact_order | sales_amount | REAL | Order value in INR |
| fact_order | sales_qty | INTEGER | Number of items |
| dim_customer | income_group | TEXT | Categorized income level |
| dim_customer | education_group | TEXT | Education classification |
| dim_restaurant | rating | REAL | Restaurant rating (1-5) |
| dim_date | is_weekend | INTEGER | Weekend flag (0/1) |

## 🔄 ETL Pipeline

The ETL pipeline includes:

1. **Data Extraction**: Loading from staging tables
2. **Data Transformation**: 
   - Income categorization into groups
   - Education level standardization
   - Currency conversion (USD to INR)
   - Date dimension population
3. **Data Loading**: Incremental updates with upsert logic
4. **Quality Checks**: Foreign key validation and orphan detection

## 📊 Key Insights

### Customer Behavior
- Higher income customers show preference for premium cuisines
- Family size significantly impacts order frequency and spending
- Weekend orders tend to have higher average values

### Restaurant Performance
- Top-performing restaurants consistently maintain high ratings (4.0+)
- Seasonal variations during festivals show 40-60% sales increases
- Multi-cuisine restaurants capture broader customer segments

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📧 Contact

Your Name - your.email@example.com

Project Link: [https://github.com/yourusername/food-delivery-analytics](https://github.com/yourusername/food-delivery-analytics)

## 🙏 Acknowledgments

- Data source: Food delivery platform transaction data
- Inspiration: Modern data warehousing best practices
- Tools: SQLite, SQL, Star Schema methodology
