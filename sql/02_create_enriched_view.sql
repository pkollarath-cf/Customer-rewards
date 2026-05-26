-- NOTE: This file is OPTIONAL and not needed for the demo to work.
-- The address_changes table already contains embedded customer details.
--
-- You can skip this file and go directly to sql/03_run_agent.sql
--
-- This enrichment step was used in an earlier architecture that used JOINs.
-- The current architecture embeds customer details in the address_changes events.

-- If you want to create an enriched view anyway (for reference):
CREATE TABLE IF NOT EXISTS address_change_enriched AS
SELECT
    ac.event_id,
    ac.customer_id,
    ac.customer_name,
    ac.customer_email,
    ac.old_zip,
    ac.new_zip,
    ac.loyalty_tier,
    ac.loyalty_points,
    ac.change_timestamp
FROM address_changes ac;

-- Verify (optional)
-- SELECT * FROM address_change_enriched LIMIT 10;
