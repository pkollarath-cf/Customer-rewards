-- Example Queries for Demo
-- Run these in Confluent Flink SQL Workspace to explore the data

-- IMPORTANT: First set the catalog and database
-- USE CATALOG `<environment-id>`;
-- USE `<kafka-cluster-id>`;
-- Get IDs from: terraform output environment_id / kafka_cluster_id

-- 1. View Recent Recommendations (Last Hour)
SELECT
    customer_name,
    loyalty_tier,
    loyalty_points,
    old_zip,
    new_zip,
    recommendations,
    change_timestamp
FROM address_change_recommendations
WHERE change_timestamp > NOW() - INTERVAL '1' HOUR
ORDER BY change_timestamp DESC;

-- 2. Filter by Loyalty Tier
SELECT
    customer_name,
    loyalty_tier,
    new_zip,
    recommendations
FROM address_change_recommendations
WHERE loyalty_tier = 'Platinum'
ORDER BY change_timestamp DESC
LIMIT 10;

-- 3. Count Address Changes by New ZIP Code
SELECT
    new_zip,
    COUNT(*) as move_count,
    AVG(loyalty_points) as avg_points
FROM address_change_recommendations
GROUP BY new_zip
ORDER BY move_count DESC;

-- 4. High-Value Customers (Platinum/Gold with 50K+ points)
SELECT
    customer_name,
    loyalty_tier,
    loyalty_points,
    new_zip,
    recommendations
FROM address_change_recommendations
WHERE loyalty_tier IN ('Platinum', 'Gold')
    AND loyalty_points >= 50000
ORDER BY loyalty_points DESC;

-- 5. Recent Merchant Preferences Analysis
SELECT
    customer_name,
    loyalty_tier,
    recent_merchants,
    points_used_90d,
    new_zip
FROM address_change_recommendations
WHERE recent_merchants IS NOT NULL
ORDER BY points_used_90d DESC
LIMIT 20;

-- 6. Agent Success Rate
SELECT
    agent_status,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM address_change_recommendations
GROUP BY agent_status;

-- 7. Most Active Moving Period
SELECT
    DATE_FORMAT(change_timestamp, 'yyyy-MM-dd HH:00:00') as hour_bucket,
    COUNT(*) as address_changes
FROM address_change_recommendations
GROUP BY DATE_FORMAT(change_timestamp, 'yyyy-MM-dd HH:00:00')
ORDER BY hour_bucket DESC
LIMIT 24;

-- 8. Full Details for Specific Customer
SELECT *
FROM address_change_recommendations
WHERE customer_id = 'C001';

-- 9. Test Model Directly (without agent)
SELECT
    ML_PREDICT(
        'rewards_model',
        'Generate a sample recommendation for a Gold tier customer with 25000 points moving to ZIP code 90210',
        MAP['debug', 'false']
    ) as test_response;

-- 10. Latest 10 Recommendations
SELECT
    event_id,
    customer_name,
    loyalty_tier,
    new_zip,
    recommendations
FROM address_change_recommendations
ORDER BY change_timestamp DESC
LIMIT 10;
