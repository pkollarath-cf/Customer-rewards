-- Verify Tables Created by Terraform
-- Run this in Confluent Flink SQL Workspace

-- 1. Show all tables
SHOW TABLES;

-- 2. Describe customer_details table
DESCRIBE customer_details;

-- 3. Describe loyalty_usage_history table
DESCRIBE loyalty_usage_history;

-- 4. Describe address_changes table
DESCRIBE address_changes;

-- 5. Check if data is flowing (after running generate_data.py)
SELECT * FROM customer_details LIMIT 5;

SELECT * FROM loyalty_usage_history LIMIT 5;

SELECT * FROM address_changes LIMIT 5;
