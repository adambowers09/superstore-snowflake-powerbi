-- ============================================
-- SUPERTSTORE SALES PIPELINE
-- Snowflake + Power BI
-- Final version for GitHub
-- ============================================

-- ============================================
-- CLEAN LAYER
-- ============================================

CREATE OR REPLACE TABLE clean.superstore_sales_clean AS
SELECT
    order_id,
    order_date::DATE AS order_date,
    ship_date::DATE AS ship_date,
    ship_mode,
    customer_name,
    segment,
    country,
    city,
    state,
    region,
    category,
    "Sub-Category" AS sub_category,
    product_name,
    sales::FLOAT AS sales,
    quantity::INTEGER AS quantity,
    discount::FLOAT AS discount,
    profit::FLOAT AS profit
FROM raw.superstore_sales_raw;


-- ============================================
-- CALC LAYER
-- ============================================

CREATE OR REPLACE TABLE calc.superstore_sales_calc AS
SELECT
    order_id,
    order_date,
    ship_date,
    ship_mode,
    customer_name,
    segment,
    country,
    city,
    state,
    region,
    category,
    sub_category,
    product_name,
    sales,
    quantity,
    discount AS discount_pct,
    profit,

    DATEDIFF('day', order_date, ship_date) AS shipping_days,

    CASE
        WHEN sales = 0 THEN NULL
        ELSE (profit / sales)
    END AS profit_margin_pct,

    YEAR(order_date) AS order_year,
    MONTH(order_date) AS order_month,
    TO_CHAR(order_date, 'YYYY-MM') AS order_year_month,

    CASE
        WHEN sales >= 500 THEN 'High Value'
        WHEN sales >= 100 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS sales_band

FROM clean.superstore_sales_clean;


-- ============================================
-- ANALYTICS LAYER
-- ============================================

CREATE OR REPLACE VIEW analytics.revenue_by_region AS
SELECT
    region,
    SUM(sales) AS total_sales,
    SUM(profit) AS total_profit,
    AVG(profit_margin_pct) AS avg_profit_margin_pct,
    SUM(quantity) AS total_quantity
FROM calc.superstore_sales_calc
GROUP BY region
ORDER BY total_sales DESC;


CREATE OR REPLACE VIEW analytics.monthly_sales_trend AS
SELECT
    order_year_month,
    SUM(sales) AS total_sales,
    SUM(profit) AS total_profit,
    COUNT(DISTINCT order_id) AS total_orders
FROM calc.superstore_sales_calc
GROUP BY order_year_month
ORDER BY order_year_month;


CREATE OR REPLACE VIEW analytics.category_performance AS
SELECT
    category,
    sub_category,
    SUM(sales) AS total_sales,
    SUM(profit) AS total_profit,
    AVG(discount_pct) AS avg_discount_pct,
    AVG(shipping_days) AS avg_shipping_days
FROM calc.superstore_sales_calc
GROUP BY category, sub_category
ORDER BY total_sales DESC;


CREATE OR REPLACE VIEW analytics.segment_performance AS
SELECT
    segment,
    SUM(sales) AS total_sales,
    SUM(profit) AS total_profit,
    COUNT(DISTINCT order_id) AS total_orders,
    AVG(profit_margin_pct) AS avg_profit_margin_pct
FROM calc.superstore_sales_calc
GROUP BY segment
ORDER BY total_sales DESC;
