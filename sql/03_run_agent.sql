-- Run AI Agent to Generate Recommendations
-- This creates a continuously running job that processes each address change
-- Run this in Confluent Flink SQL Workspace

-- IMPORTANT: First set the catalog and database
-- USE CATALOG `<environment-id>`;
-- USE `<kafka-cluster-id>`;
-- Get IDs from: terraform output environment_id / kafka_cluster_id

CREATE TABLE address_change_recommendations AS
SELECT
    ac.event_id,
    ac.customer_id,
    ac.customer_name,
    ac.old_zip,
    ac.new_zip,
    ac.loyalty_tier,
    ac.loyalty_points,
    ac.change_timestamp,
    agent_result.status as agent_status,
    agent_result.response as recommendations
FROM address_changes ac,
LATERAL TABLE(
    AI_RUN_AGENT(
        'rewards_recommendation_agent',
        CONCAT(
            'Customer Profile:
- Name: ', ac.customer_name, '
- Loyalty Tier: ', COALESCE(ac.loyalty_tier, 'Basic'), '
- Available Points: ', CAST(COALESCE(ac.loyalty_points, 0) AS STRING), '

Address Change:
- Moving from ZIP: ', COALESCE(ac.old_zip, 'Unknown'), '
- Moving to ZIP: ', ac.new_zip, '

Task: Generate 3-5 personalized recommendations for this customer''s new location.
Include specific merchant categories, loyalty point redemption suggestions, and tier-specific benefits.
'
        ),
        ac.event_id,
        MAP['debug', 'false']
    )
) as agent_result(status, response);

-- Note: This query will run continuously, processing each new address change event
-- To stop it, click "Stop" in the Flink SQL Workspace
