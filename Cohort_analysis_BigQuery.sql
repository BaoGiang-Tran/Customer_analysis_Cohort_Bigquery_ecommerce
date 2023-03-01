# 1. Monthly Orders and Users
# Get the total users who completed the order and total orders per month
SELECT 
  DATE_TRUNC(DATE(created_at), month) month,
  COUNT(DISTINCT(user_id)) total_user,
  COUNT(order_id) total_order
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE status = 'Complete'
GROUP BY 1
ORDER BY 1
-- Insight: As can be seen that the number of orders and users increase sharply from 2019 to 2023.

#2. Avg Order Value (AOV) and distinct users
#Get avg order value and total numnber of unique users, grouped by month 
SELECT 
  DATE_TRUNC(DATE(o.created_at),MONTH) AS month_year,
  COUNT(DISTINCT(or_item.user_id)) AS distinct_user,
  ROUND(SUM(or_item.sale_price)/COUNT(DISTINCT or_item.order_id),2) AS avg_order_value
FROM `bigquery-public-data.thelook_ecommerce.orders` o
JOIN `bigquery-public-data.thelook_ecommerce.order_items` or_item ON o.order_id = or_item.order_id
WHERE o.status = 'Complete'
GROUP BY 1
ORDER BY 1
-- Insight: It clearly that there was dymatic grownth in the total users over the year

# 3. Correlation coeffition total users and AOV
# Get AOV and total users year by year, and calculate corr
SELECT CORR(distinct_user, avg_order_value) corr_users_aov
FROM
(
SELECT 
  DATE_TRUNC(DATE(o.created_at),MONTH) AS month_year,
  COUNT(DISTINCT(or_item.user_id)) AS distinct_user,
  ROUND(SUM(or_item.sale_price)/COUNT(DISTINCT or_item.order_id),2) AS avg_order_value
FROM `bigquery-public-data.thelook_ecommerce.orders` o
JOIN `bigquery-public-data.thelook_ecommerce.order_items` or_item ON o.order_id = or_item.order_id
WHERE o.status = 'Complete'
GROUP BY 1
ORDER BY 1)
-- Insight: corr = 0.1 total users was not affect to AOV over the year

# 4.Customer age category
# Find the fist and last name of users from the youngest and oldest age of each gender
WITH youngest AS 
(
  SELECT
    gender,
    MIN(age) OVER(PARTITION BY gender) youngest
  FROM `bigquery-public-data.thelook_ecommerce.users`
  WHERE age IN (SELECT MIN(age) FROM `bigquery-public-data.thelook_ecommerce.users`)
), oldest AS 
(
  SELECT gender,
    MAX(age) OVER(PARTITION BY gender) oldest
  FROM `bigquery-public-data.thelook_ecommerce.users`
  WHERE age IN (SELECT MAX(age) FROM `bigquery-public-data.thelook_ecommerce.users`)
)
  SELECT y.gender, y.youngest, COUNT(y.youngest) cnt_youngest_user
  FROM youngest y
  GROUP BY 1,2
UNION ALL
  SELECT ol.gender, ol.oldest, COUNT(ol.oldest) cnt_oldest_uesr
  FROM oldest ol
  GROUP BY 1,2

-- Insight: As can be seen that the youngest user is 12 years old while oldest user is 70 years old. 
--          In addition, the percenatge of youngest user in female are higher than that male over the year.
--          On the order hand, the fewer number of oldest user in men than that in women from 2019 to 2023.
--          The most number of youngest users was getted data, compare to the number of oldest users in the period of five years.
 
# 5. Top 5 Monthly Product
# Get the top 5 most profitable product and its profit detail breakdown by month
SELECT *
FROM(
SELECT *,
  ROW_NUMBER() OVER(PARTITION BY s.month ORDER BY s.profit DESC) rank_profit
FROM
(
SELECT
  DISTINCT(oi.product_id) product_id, inv.product_name,
  DATE_TRUNC(DATE(oi.created_at),month) month,
  ROUND(SUM(oi.sale_price),2) sales,
  ROUND(SUM(inv.cost),2) cost,
  ROUND(SUM(oi.sale_price) - SUM(inv.cost),2) profit
FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
JOIN `bigquery-public-data.thelook_ecommerce.inventory_items` inv ON oi.product_id = inv.product_id
WHERE oi.status = 'Complete'
GROUP BY 1,2,3
ORDER BY month) s
ORDER BY month, rank_profit)
WHERE rank_profit <=5

#6. Business Growth by users and revenue
SELECT *,
  IFNULL(cal.total_order - LAG(cal.total_order) OVER (PARTITION BY cal.category ORDER BY cal.month ), 0) AS grownth_total_order,
  ROUND(IFNULL(cal.revenue - LAG(cal.revenue) OVER (PARTITION BY cal.category ORDER BY cal.month ), 0),2) AS grownth_revenue
FROM
(
SELECT
  DISTINCT (p.category) category,
  DATE_TRUNC(DATE(oi.created_at), month) month,
  COUNT(oi.order_id) total_order,
  ROUND(SUM(oi.sale_price),2) revenue
FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
JOIN `bigquery-public-data.thelook_ecommerce.products` p ON oi.product_id = p.id
WHERE oi.status = 'Complete'
GROUP BY category, month
ORDER BY category, month DESC) cal
ORDER BY category, month DESC
--  Insight: As can be seen that there was a fluctuation in the proportation of grownth orders and revenue over the months from 2019 to 2023.

# 7 Cohort Analysis
WITH cohort_month AS 
(
  SELECT
    user_id,
    MIN(DATE_TRUNC(DATE(created_at), month)) cohort_month
  FROM `bigquery-public-data.thelook_ecommerce.orders` 
  GROUP BY user_id
  ORDER BY cohort_month
),user_active AS 
(
  SELECT
      o.user_id,
      DATE_DIFF(DATE_TRUNC(DATE(created_at),month),co.cohort_month,month) recency
  FROM `bigquery-public-data.thelook_ecommerce.orders` o 
  JOIN cohort_month co ON o.user_id = co.user_id
  GROUP BY o.user_id, recency
  ORDER BY recency
), cohort_size AS 
(
  SELECT com.cohort_month,
    COUNT(com.cohort_month) num_user
  FROM cohort_month com
  GROUP BY com.cohort_month
  ORDER BY com.cohort_month
), retention AS 
(
  SELECT cm.cohort_month,
    COUNT(CASE WHEN recency = 1 THEN cohort_month END) M1,
    COUNT(CASE WHEN recency = 2 THEN cohort_month END) M2,
    COUNT(CASE WHEN recency = 3 THEN cohort_month END) M3,
  FROM cohort_month cm
  LEFT JOIN user_active ua ON cm.user_id = ua.user_id
  GROUP BY cm.cohort_month
  ORDER BY cm.cohort_month
)
SELECT 
  r.cohort_month cohort_month,
  cs.num_user AS M,M1,M2,M3
FROM retention r
LEFT JOIN cohort_size cs ON r.cohort_month = cs.cohort_month
WHERE r.cohort_month IS NOT NULL 
ORDER BY cohort_month