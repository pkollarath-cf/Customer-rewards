-- Complete Agent Pipeline
-- Run this after terraform apply and after running generate_data.py
-- This creates both the enrichment and agent processing in one go

-- Step 1: Create enriched address changes table (join with customer details)
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

-- Step 2: Run the agent on each address change and store results
CREATE TABLE IF NOT EXISTS address_change_recommendations AS
SELECT
    ace.event_id,
    ace.customer_id,
    ace.customer_name,
    ace.old_zip,
    ace.new_zip,
    ace.loyalty_tier,
    ace.loyalty_points,
    ace.change_timestamp,
    agent_result.status AS agent_status,
    agent_result.response AS recommendations
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
Include specific merchant categories, loyalty point redemption suggestions, and tier-specific benefits.'
        ),
        ace.event_id,
        MAP['debug', 'false']
    )
) AS agent_result(status, response);

-- Step 3: Query to view recommendations (run this separately to check results)
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
