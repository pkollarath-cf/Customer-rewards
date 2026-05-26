-- Create Enriched Address Changes Table
-- This joins address changes with customer details
-- Run this in Confluent Flink SQL Workspace

CREATE TABLE IF NOT EXISTS address_change_enriched AS
SELECT
    ac.event_id,
    ac.customer_id,
    ac.old_zip,
    ac.new_zip,
    cd.name AS customer_name,
    cd.email AS customer_email,
    cd.loyalty_tier,
    cd.loyalty_points,
    ac.change_timestamp
FROM address_changes ac
JOIN customer_details cd
  ON ac.customer_id = cd.customer_id;

-- Verify the enriched table
SELECT * FROM address_change_enriched LIMIT 10;
