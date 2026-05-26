# Live Demo Script

Step-by-step commands for presenting this demo.

## Preparation (Before Demo)

1. **Deploy infrastructure:**
   ```bash
   cd terraform
   terraform apply
   ```

2. **Configure data generator:**
   ```bash
   cd ../scripts
   cp .env.example .env
   # Edit .env with credentials
   pip install -r requirements.txt
   ```

3. **Generate initial customer data:**
   ```bash
   python generate_data.py
   # Let run for 30 seconds to populate customers & history
   # Ctrl+C to stop
   ```

## Demo Flow (15 minutes)

### Part 1: Introduction (2 minutes)

**Narrative:**
> "Today I'm showing you a real-time AI-powered customer recommendation system 
> built with Confluent Flink and AWS Bedrock. The scenario: Loyalty program 
> cardholders change their address, and we instantly generate personalized 
> merchant and loyalty recommendations for their new location."

**Show Architecture:**
```
Customer Address Changes (Kafka)
    ↓
Flink Enrichment (Join customer + loyalty data)
    ↓
AI Agent (Claude Sonnet 4.5)
    ↓
Personalized Recommendations (Kafka)
```

### Part 2: Show the Data (3 minutes)

**Open Confluent Cloud UI → Topics**

Show the three topics:
- `customer_details` - 100 customer profiles
- `loyalty_usage_history` - Transaction history
- `address_changes` - Address change events

**Open Flink SQL Workspace**

```sql
-- Show customer data
SELECT customer_id, name, loyalty_tier, loyalty_points, current_zip
FROM customer_details
LIMIT 10;
```

```sql
-- Show recent loyalty usage
SELECT customer_id, merchant, product_service, loyalty_points_used
FROM loyalty_usage_history
LIMIT 10;
```

### Part 3: Data Enrichment (2 minutes)

**Narrative:**
> "First, we enrich address change events with customer profile and purchase history."

**Run enrichment query:**
```sql
-- Create enriched view
CREATE VIEW IF NOT EXISTS address_changes_enriched AS
SELECT
    ac.event_id,
    ac.customer_id,
    ac.customer_name,
    ac.old_zip,
    ac.new_zip,
    ac.loyalty_tier,
    ac.loyalty_points,
    ac.change_timestamp,
    LISTAGG(DISTINCT lh.merchant, ', ') as recent_merchants
FROM address_changes ac
LEFT JOIN loyalty_usage_history lh
    ON ac.customer_id = lh.customer_id;
```

```sql
-- Show enriched data
SELECT * FROM address_changes_enriched LIMIT 5;
```

### Part 4: AI Agent (5 minutes)

**Narrative:**
> "Now we create an AI agent that analyzes this enriched data and generates 
> personalized recommendations. The agent uses Claude Sonnet 4.5 from AWS Bedrock."

**Show the agent:**
```sql
-- Show agent configuration
SHOW CREATE AGENT rewards_recommendation_agent;
```

**Run the agent:**
```sql
-- Create recommendations table
CREATE TABLE address_change_recommendations AS
SELECT
    ace.event_id,
    ace.customer_id,
    ace.customer_name,
    ace.old_zip,
    ace.new_zip,
    ace.loyalty_tier,
    ace.loyalty_points,
    ace.recent_merchants,
    ace.change_timestamp,
    agent_result.status,
    agent_result.response as recommendations
FROM address_changes_enriched ace,
LATERAL TABLE(
    AI_RUN_AGENT(
        'rewards_recommendation_agent',
        CONCAT(
            'Customer: ', ace.customer_name, '
Tier: ', ace.loyalty_tier, '
Points: ', CAST(ace.loyalty_points AS STRING), '
Moving from ZIP: ', ace.old_zip, ' to ZIP: ', ace.new_zip, '
Recent merchants: ', ace.recent_merchants
        ),
        ace.event_id,
        MAP['debug', 'false']
    )
) as agent_result(status, response);
```

### Part 5: Live Results (3 minutes)

**Start data generator in terminal:**
```bash
python generate_data.py
# This generates 1 address change every 5 seconds
```

**Watch recommendations in real-time:**
```sql
SELECT
    customer_name,
    loyalty_tier,
    old_zip,
    new_zip,
    recommendations,
    change_timestamp
FROM address_change_recommendations
ORDER BY change_timestamp DESC
LIMIT 10;
```

**Keep refreshing to show new recommendations appearing**

**Show high-value customers:**
```sql
SELECT
    customer_name,
    loyalty_tier,
    loyalty_points,
    new_zip,
    recommendations
FROM address_change_recommendations
WHERE loyalty_tier IN ('Platinum', 'Gold')
    AND loyalty_points >= 50000
ORDER BY change_timestamp DESC
LIMIT 5;
```

### Part 6: Analytics (2 minutes)

**Show aggregations:**
```sql
-- Most popular destination ZIP codes
SELECT
    new_zip,
    COUNT(*) as moves,
    AVG(loyalty_points) as avg_points
FROM address_change_recommendations
GROUP BY new_zip
ORDER BY moves DESC;
```

```sql
-- Agent success rate
SELECT
    status,
    COUNT(*) as count
FROM address_change_recommendations
GROUP BY status;
```

### Part 7: Wrap Up (1 minute)

**Narrative:**
> "In production, these recommendations would:
> 1. Trigger email campaigns
> 2. Update mobile app notifications
> 3. Feed into call center systems
> 4. Drive targeted marketing
> 
> All in real-time, fully streaming, with AI-powered personalization."

**Stop the data generator** (Ctrl+C)

## Demo Tips

### Before You Start
- Test the full flow once
- Have Confluent Cloud UI and terminal side-by-side
- Pre-populate some data so initial queries show results

### During Demo
- Explain each step before running it
- Let queries finish before moving on
- Show the actual recommendations text - they're impressive!
- If a query is slow, explain why (joins, AI inference)

### Common Questions & Answers

**Q: How fast does this run?**
A: Sub-second enrichment, ~2-3 seconds for AI recommendation generation

**Q: How much does this cost?**
A: Confluent: ~$1-2/hour with Flink. AWS Bedrock: ~$0.01 per recommendation. Turn off when not in use.

**Q: Can it scale?**
A: Yes - Kafka and Flink scale horizontally. Agent processing is stateless and parallelizable.

**Q: What about other AI models?**
A: Flink supports any Bedrock model. Can also use Azure OpenAI, Vertex AI, or custom models.

**Q: What if the AI is slow?**
A: Use async processing, caching, or fallback to rule-based recommendations.

## Cleanup After Demo

```bash
cd terraform
terraform destroy
# Type 'yes' to confirm
```

This removes all resources and stops billing.
