-- Complete Agent Pipeline
-- Run this after terraform apply and after running generate_data.py
-- This starts the AI agent processing job

-- IMPORTANT: First set the catalog and database
-- USE CATALOG `<environment-id>`;
-- USE `<kafka-cluster-id>`;
-- Get IDs from: terraform output environment_id / kafka_cluster_id

-- Run the agent on each address change and store results
CREATE TABLE IF NOT EXISTS address_change_recommendations AS
SELECT
    ac.event_id,
    ac.customer_id,
    ac.customer_name,
    ac.old_zip,
    ac.new_zip,
    ac.loyalty_tier,
    ac.loyalty_points,
    ac.change_timestamp,
    agent_result.status AS agent_status,
    agent_result.response AS recommendations
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
Include specific merchant categories, loyalty point redemption suggestions, and tier-specific benefits.'
        ),
        ac.event_id,
        MAP['debug', 'false']
    )
) AS agent_result(status, response);

-- Query to view recommendations (run this separately to check results)
-- SELECT
--     event_id,
--     customer_name,
--     old_zip,
--     new_zip,
--     agent_status,
--     recommendations,
--     change_timestamp
-- FROM address_change_recommendations
-- ORDER BY change_timestamp DESC
-- LIMIT 10;
