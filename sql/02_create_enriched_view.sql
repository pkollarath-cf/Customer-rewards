-- Create Enriched Address Changes View
-- This view joins address changes with customer loyalty history
-- Run this in Confluent Flink SQL Workspace

-- Simplified enriched view without complex window functions
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
    '' as recent_merchants,  -- Will be populated by agent from history
    0 as points_used_90d     -- Will be calculated by agent
FROM address_changes ac;

-- Verify the view
SELECT * FROM address_changes_enriched LIMIT 10;
