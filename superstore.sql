use sales;
CREATE TABLE superstore (
    order_id VARCHAR(50),
    order_date DATE,
    ship_date DATE,
    ship_mode VARCHAR(50),
    customer_name VARCHAR(100),
    segment VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    market VARCHAR(50),
    region VARCHAR(50),
    product_id VARCHAR(50),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    product_name VARCHAR(150),
    sales DECIMAL(10,2),
    quantity INT,
    discount DECIMAL(5,2),
    profit DECIMAL(10,2),
    shipping_cost DECIMAL(10,2),
    order_priority VARCHAR(20),
    year INT
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/superstore.csv'
INTO TABLE superstore
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select * from superstore;

-- Check table structure
DESCRIBE superstore;

-- Fix encoding issue in first column
ALTER TABLE superstore
RENAME COLUMN ï»¿order_id TO order_id;

-- Total number of records
SELECT COUNT(*) AS total_rows FROM superstore;

-- Sample data check
SELECT * FROM superstore LIMIT 100;

-- Check date columns
SELECT order_date, ship_date
FROM superstore
LIMIT 5;

/*
   DATE FORMAT CLEANING
*/

-- Disable safe updates
SET SQL_SAFE_UPDATES = 0;

-- Convert order_date to DATE format
UPDATE superstore
SET order_date = STR_TO_DATE(order_date, '%d-%m-%Y');

-- Enable safe updates
SET SQL_SAFE_UPDATES = 1;

/* 
   DATA QUALITY CHECKS
*/

-- Check NULL values
SELECT
    COUNT(*) AS total_rows,
    SUM(sales IS NULL) AS sales_nulls,
    SUM(profit IS NULL) AS profit_nulls
FROM superstore;

-- Check duplicate order-product combinations
SELECT order_id, product_id, COUNT(*) AS duplicate_count
FROM superstore
GROUP BY order_id, product_id
HAVING COUNT(*) > 1;


/* 
   KEY PERFORMANCE INDICATORS (KPIs)
*/

SELECT
    SUM(sales) AS total_sales,
    SUM(profit) AS total_profit,
    COUNT(DISTINCT order_id) AS total_orders
FROM superstore;


/*
   BASIC EXPLORATION
*/

-- Total number of orders, customers, products
SELECT COUNT(DISTINCT order_id) AS total_orders FROM superstore;
SELECT COUNT(DISTINCT customer_name) AS total_customers FROM superstore;
SELECT COUNT(DISTINCT product_id) AS total_products FROM superstore;

-- Unique customers per segment
SELECT segment, COUNT(DISTINCT customer_name) AS total_customers
FROM superstore
GROUP BY segment
ORDER BY total_customers DESC;

-- Unique customers per region
SELECT region, COUNT(DISTINCT customer_name) AS total_customers
FROM superstore
GROUP BY region
ORDER BY total_customers DESC;

-- Distinct values
SELECT DISTINCT ship_mode FROM superstore;
SELECT DISTINCT category FROM superstore;
SELECT DISTINCT sub_category FROM superstore;


/*
   TIME-BASED ANALYSIS
*/

-- Total sales per year
SELECT
    YEAR(order_date) AS year,
    SUM(sales) AS total_sales
FROM superstore
GROUP BY YEAR(order_date)
ORDER BY total_sales DESC;

-- Monthly sales trend (year-wise)
SELECT
    YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    SUM(sales) AS monthly_sales
FROM superstore
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year, month;

-- Average profit per month (all years combined)
SELECT
    MONTH(order_date) AS month,
    AVG(profit) AS avg_profit
FROM superstore
GROUP BY MONTH(order_date)
ORDER BY month;

-- Year-over-Year (YoY) sales growth
SELECT
    YEAR(order_date) AS year,
    SUM(sales) AS total_sales,
        ROUND((SUM(sales) - LAG(SUM(sales)) OVER (ORDER BY YEAR(order_date)))
        / LAG(SUM(sales)) OVER (ORDER BY YEAR(order_date)) * 100,2) AS yoy_growth_percent
FROM superstore
GROUP BY YEAR(order_date)
ORDER BY year;

-- Growth compared to first year
SELECT
    YEAR(order_date) AS year,
    SUM(sales) AS total_sales,
        ROUND((SUM(sales) - FIRST_VALUE(SUM(sales)) OVER (ORDER BY YEAR(order_date)))
        / FIRST_VALUE(SUM(sales)) OVER (ORDER BY YEAR(order_date)) * 100,2) AS growth_from_first_year_percent
FROM superstore
GROUP BY YEAR(order_date)
ORDER BY year;

/* =====================================================
   7️⃣ CUSTOMER ANALYSIS
   ===================================================== */

-- Top 10 customers by sales
SELECT
    customer_name,
    SUM(sales) AS total_sales
FROM superstore
GROUP BY customer_name
ORDER BY total_sales DESC
LIMIT 10;

-- Top 10 customers by profit
SELECT
    customer_name,
    SUM(profit) AS total_profit
FROM superstore
GROUP BY customer_name
ORDER BY total_profit DESC
LIMIT 10;

-- Total sales per customer
SELECT
    customer_name,
    SUM(sales) AS total_sales
FROM superstore
GROUP BY customer_name
ORDER BY total_sales DESC;

-- Total profit per customer
SELECT
    customer_name,
    SUM(profit) AS total_profit
FROM superstore
GROUP BY customer_name
ORDER BY total_profit DESC;


-- Top 5 customers by sales in each region
SELECT *
FROM (
    SELECT
        region,
        customer_name,
        SUM(sales) AS total_sales,
        DENSE_RANK() OVER (
            PARTITION BY region
            ORDER BY SUM(sales) DESC
        ) AS rank_no
    FROM superstore
    GROUP BY region, customer_name
) t
WHERE rank_no <= 5;

-- Top 3 customers by profit in each segment
SELECT *
FROM (
    SELECT
        segment,
        customer_name,
        SUM(profit) AS total_profit,
        DENSE_RANK() OVER (
            PARTITION BY segment
            ORDER BY SUM(profit) DESC
        ) AS rank_in_segment
    FROM superstore
    GROUP BY segment, customer_name
) t
WHERE rank_in_segment <= 3;

-- Region-wise average customer sales
SELECT
    region,
    customer_name,
    AVG(sales) AS avg_sales_per_customer
FROM superstore
GROUP BY region, customer_name
ORDER BY region, avg_sales_per_customer DESC;

-- Rank customers by total sales (overall)
SELECT
    customer_name,
    SUM(sales) AS sales_per_customer,
    DENSE_RANK() OVER (
        ORDER BY SUM(sales) DESC
    ) AS sales_rank
FROM superstore
GROUP BY customer_name;

-- Rank customers by total profit within each segment
SELECT
    segment,
    customer_name,
    SUM(profit) AS total_profit,
    DENSE_RANK() OVER (
        PARTITION BY segment
        ORDER BY SUM(profit) DESC
    ) AS profit_rank
FROM superstore
GROUP BY segment, customer_name;

-- Identify customers contributing to top 20% of total sales (Pareto Analysis)
SELECT *
FROM (
    SELECT
        customer_name,
        total_sales,
        percent,
        SUM(percent) OVER (
            ORDER BY percent DESC
        ) AS cumulative_percentage
    FROM (
        SELECT
            customer_name,
            SUM(sales) AS total_sales,
            SUM(sales) / SUM(SUM(sales)) OVER () * 100 AS percent
        FROM superstore
        GROUP BY customer_name
    ) t
) t2
WHERE cumulative_percentage <= 20;

-- Customers with overall negative profit
SELECT
    customer_name,
    SUM(profit) AS total_profit
FROM superstore
GROUP BY customer_name
HAVING SUM(profit) < 0
ORDER BY total_profit;

-- Customers having high sales but low or negative profit
SELECT
    customer_name,
    SUM(sales) AS total_sales,
    SUM(profit) AS total_profit
FROM superstore
GROUP BY customer_name
HAVING
    SUM(sales) >
        (
            SELECT AVG(customer_sales)
            FROM (
                SELECT SUM(sales) AS customer_sales
                FROM superstore
                GROUP BY customer_name
            ) t
        )
    AND SUM(profit) <= 0
ORDER BY total_sales DESC;

-- Customers receiving highest average discount
SELECT
    customer_name,
    AVG(discount) AS avg_discount_per_customer
FROM superstore
GROUP BY customer_name
ORDER BY avg_discount_per_customer DESC;

