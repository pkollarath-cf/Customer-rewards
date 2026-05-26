-- Run AI Agent to Generate Recommendations
-- This creates a continuously running job that processes each address change
-- Run this in Confluent Flink SQL Workspace

CREATE TABLE address_change_recommendations AS
SELECT
    ace.event_id,
    ace.customer_id,
    ace.customer_name,
    ace.old_zip,
    ace.new_zip,
    ace.loyalty_tier,
    ace.loyalty_points,
    ace.change_timestamp,
    agent_result.status as agent_status,
    agent_result.response as recommendations
FROM address_change_enriched ace,
LATERAL TABLE(
    AI_RUN_AGENT(
        'rewards_recommendation_agent',
        CONCAT(
            'Customer Profile:
- Name: ', ace.customer_name, '
- Loyalty Tier: ', COALESCE(ace.loyalty_tier, 'Basic'), '
- Available Points: ', CAST(COALESCE(ace.loyalty_points, 0) AS STRING), '

Address Change:
- Moving from ZIP: ', COALESCE(ace.old_zip, 'Unknown'), '
- Moving to ZIP: ', ace.new_zip, '

Task: Generate 3-5 personalized recommendations for this customer''s new location.
Include specific merchant categories, loyalty point redemption suggestions, and tier-specific benefits.
'
        ),
        ace.event_id,
        MAP['debug', 'false']
    )
) as agent_result(status, response);

-- Note: This query will run continuously, processing each new address change event
-- To stop it, click "Stop" in the Flink SQL Workspace
