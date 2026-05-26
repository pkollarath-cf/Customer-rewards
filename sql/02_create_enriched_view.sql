-- Create Enriched Address Changes View
-- This view joins address changes with customer loyalty history
-- Run this in Confluent Flink SQL Workspace

-- Create a view that enriches address changes with recent merchant visits
CREATE VIEW IF NOT EXISTS address_changes_enriched AS
SELECT
    ac.event_id,
    ac.customer_id,
    ac.customer_name,
    ac.customer_email,
    ac.old_zip,
    ac.new_zip,
    ac.loyalty_tier,
    ac.loyalty_points,
    ac.change_timestamp,
    -- Get recent merchant history (last 90 days)
    LISTAGG(DISTINCT lh.merchant, ', ') OVER (
        PARTITION BY ac.customer_id
        ORDER BY lh.transaction_timestamp
        RANGE BETWEEN INTERVAL '90' DAY PRECEDING AND CURRENT ROW
    ) as recent_merchants,
    -- Total points used in last 90 days
    SUM(lh.loyalty_points_used) OVER (
        PARTITION BY ac.customer_id
        ORDER BY lh.transaction_timestamp
        RANGE BETWEEN INTERVAL '90' DAY PRECEDING AND CURRENT ROW
    ) as points_used_90d
FROM address_changes ac
LEFT JOIN loyalty_usage_history lh
    ON ac.customer_id = lh.customer_id
    AND lh.transaction_timestamp >= ac.change_timestamp - INTERVAL '90' DAY;

-- Verify the view
SELECT * FROM address_changes_enriched LIMIT 10;
