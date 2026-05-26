# Real-Time Customer Rewards Recommendation Demo

An intelligent streaming agent that detects customer address changes and generates personalized merchant and loyalty recommendations in real-time using Confluent Flink and AI.

## Architecture

```
Customer Address Changes (Kafka Topic)
    ↓
Flink Enrichment (Join with Customer & Loyalty Data)
    ↓
AI Agent (Bedrock Claude)
    ↓
Personalized Recommendations (Kafka Topic)
```

**📖 See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed component breakdown and what Terraform creates vs. what you run manually.**

## What This Demo Does

1. **Detects Address Changes**: Monitors when customers change their address
2. **Enriches Data**: Joins with customer details and loyalty usage history
3. **AI-Powered Recommendations**: Uses a Flink AI Agent to analyze:
   - New ZIP code location
   - Customer loyalty tier
   - Past merchant preferences
   - Loyalty points balance
4. **Generates Personalized Offers**: Suggests relevant local merchants and loyalty redemption options

## Prerequisites

- **Confluent Cloud Account** ([Sign up for free trial](https://confluent.cloud))
- **AWS Account** with Bedrock access ([Enable Claude Sonnet 4.5](https://console.aws.amazon.com/bedrock))
- **Terraform** >= 1.5
- **Python** >= 3.8

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd customer-rewards-demo
```

### 2. Get Confluent Cloud API Keys

1. Log in to [Confluent Cloud](https://confluent.cloud)
2. Go to **Administration** → **Cloud API Keys**
3. Click **Add Key** → Create a **Cloud API Key** (not cluster-specific)
4. Save the Key and Secret

### 3. Enable AWS Bedrock

1. Go to [AWS Bedrock Console](https://console.aws.amazon.com/bedrock)
2. Navigate to **Model access** → Request access to **Claude Sonnet 4.5**
3. Create IAM user with `AmazonBedrockFullAccess` policy
4. Generate Access Key and Secret for the IAM user

**Model Used:** Claude Sonnet 4.5 (`us.anthropic.claude-sonnet-4-5-20250929-v1:0`)
- Uses AWS Bedrock inference profile format with `us.` prefix (required for on-demand throughput)
- To use a different model or region, edit `terraform/main.tf` and update the Bedrock connection endpoint

### 4. Configure Terraform

Create `terraform/terraform.tfvars` from the example:

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
```

Edit `terraform/terraform.tfvars` with **only** these values:

```hcl
# Confluent Cloud API Key (from step 2)
confluent_cloud_api_key    = "YOUR_CLOUD_API_KEY"
confluent_cloud_api_secret = "YOUR_CLOUD_API_SECRET"

# AWS Bedrock credentials (from step 3)
aws_access_key     = "YOUR_AWS_ACCESS_KEY"
aws_secret_key     = "YOUR_AWS_SECRET_KEY"
aws_bedrock_region = "us-east-1"
```

**That's it!** Terraform will create everything else automatically.

### 5. Deploy Infrastructure

```bash
cd terraform
terraform init
terraform apply
# Review the plan and type 'yes' to confirm
```

Terraform will automatically create:

**Confluent Cloud Resources:**
- ✅ Environment (`customer-rewards-demo`)
- ✅ Service Account with proper permissions
- ✅ Kafka Cluster (Basic tier, us-east-1)
- ✅ Schema Registry (Essentials)
- ✅ API Keys (Kafka + Schema Registry)
- ✅ Flink Compute Pool (5 CFU max)
- ✅ 3 Kafka Topics (customer_details, loyalty_usage_history, address_changes)
- ✅ 3 Flink Tables
- ✅ Bedrock Connection
- ✅ AI Model (Claude Sonnet 4.5)
- ✅ AI Agent (Recommendation Engine)

**After deployment (~5-10 minutes):**
```bash
terraform output next_steps  # See what to do next
```

### 6. Configure Data Generator

Save Kafka credentials to `.env` file:

```bash
cd terraform
terraform output -raw env_file_content > ../scripts/.env
```

### 7. Install Python Dependencies

```bash
cd ../scripts
pip install -r requirements.txt
```

### 8. Generate Sample Data

```bash
python generate_data.py
```

This will:
1. **Fetch Avro schemas** from Schema Registry (automatically created by Flink tables)
2. **Produce messages** using Avro serialization:
   - 100 customer records
   - Loyalty usage history for each customer
   - Continuous stream of address change events (1 every 5 seconds)

**Note:** The data generator uses **Avro serialization with Schema Registry**. Flink tables automatically create and register Avro schemas when you run `terraform apply`.

**Tip:** Let it run for 30-60 seconds to populate initial data, then press `Ctrl+C`

## Running the Demo

### 1. Generate Sample Data FIRST

**Important:** Generate data before running SQL queries, so tables have data to join.

```bash
cd scripts
python generate_data.py
```

This will:
- Register Avro schemas in Schema Registry
- Produce 100 customers with loyalty history
- Start generating address change events (1 every 5 seconds)

**Let it run for 30-60 seconds** to populate data, then press `Ctrl+C` to stop (or leave running).

### 2. Run SQL to Start AI Agent Processing

**Open Flink SQL Workspace:**
```
https://confluent.cloud/environments/<env-id>/flink
```

**Copy and paste** the contents of `sql/03_run_agent.sql` into Flink SQL editor and run it.

This creates `address_change_recommendations` table and starts the AI agent job that:
- Reads each address change event (with embedded customer details)
- Calls the AI agent to generate personalized recommendations  
- Stores results in the recommendations table

**Check Job Status:**
- Click **"Jobs"** tab in Flink console
- You should see `address_change_recommendations` job with status **RUNNING** ✓

### 3. View Recommendations

In Flink SQL Workspace:

```sql
SELECT 
    customer_name,
    old_zip,
    new_zip,
    loyalty_tier,
    recommendations,
    change_timestamp
FROM address_change_recommendations
ORDER BY change_timestamp DESC
LIMIT 10;
```

### 3. Monitor Results

Watch recommendations appear in real-time:
- Customer information
- Old and new ZIP codes
- AI-generated merchant recommendations
- Loyalty redemption suggestions

Example output:
```
customer_id: C001
customer_name: John Smith
old_zip: 10001
new_zip: 90210
loyalty_tier: Gold
loyalty_points: 50000
recommendations: "Welcome to Beverly Hills! Based on your Gold tier status and 50,000 points, 
                  we recommend: 1) Rodeo Drive Shopping - Redeem 10,000 points for $100 credit,
                  2) Local Fine Dining - Use points at The Ivy or Spago, 
                  3) Exclusive Events - Gold tier access to local networking events"
```

## Demo Queries

Try these queries in the Flink SQL Workspace:

### View Recent Address Changes
```sql
SELECT 
  customer_name,
  old_zip,
  new_zip,
  loyalty_tier,
  loyalty_points,
  change_timestamp
FROM address_change_recommendations
WHERE change_timestamp > NOW() - INTERVAL '1' HOUR;
```

### Filter by Loyalty Tier
```sql
SELECT 
  customer_name,
  new_zip,
  recommendations
FROM address_change_recommendations
WHERE loyalty_tier = 'Platinum';
```

### Count by New ZIP Code
```sql
SELECT 
  new_zip,
  COUNT(*) as move_count
FROM address_change_recommendations
GROUP BY new_zip;
```

## Architecture Details

### Data Flow

1. **Customer Details Topic**: Compacted topic with customer profiles
2. **Loyalty Usage History**: Transaction history at various merchants
3. **Address Changes**: Stream of address change events
4. **Enrichment Join**: Flink SQL joins address changes with customer + loyalty data
5. **AI Agent**: Bedrock Claude analyzes enriched data and generates recommendations
6. **Output Topic**: Recommendations stored for downstream processing

### AI Agent Prompt

The agent uses this system prompt:

```
You are a personalized rewards recommendation assistant for loyalty cardholders.

When a customer changes their address, analyze:
1. Their new ZIP code location
2. Loyalty tier (Basic/Silver/Gold/Platinum)
3. Current loyalty points balance
4. Past merchant preferences from transaction history

Generate 3-5 personalized recommendations for:
- Local merchants in their new area
- Loyalty point redemption opportunities
- Tier-specific exclusive offers

Keep recommendations concise, relevant, and actionable.
Format as a friendly welcome message to their new location.
```

## Cleanup

To destroy all resources:

```bash
cd terraform
terraform destroy
```

## Troubleshooting

### Agent Not Generating Recommendations

1. Check Bedrock connection:
   ```sql
   SELECT ML_PREDICT('rewards_model', 'test prompt', MAP['debug', 'true']);
   ```

2. Verify data is flowing:
   ```sql
   SELECT * FROM address_changes LIMIT 10;
   ```

### No Data Appearing

1. Check topic has messages:
   - View Topics in Confluent Cloud UI
   - Verify `address_changes` topic has messages

2. Verify Flink tables are created:
   ```sql
   SHOW TABLES;
   ```

### Python Script Errors

1. Verify `.env` file has correct credentials
2. Check Kafka cluster is accessible:
   ```bash
   python -c "from kafka import KafkaProducer; print('OK')"
   ```

## Cost Estimates

**Confluent Cloud** (Basic cluster + Flink):
- ~$1-2/hour with Flink running
- Stop Flink compute pool when not in use

**AWS Bedrock**:
- ~$0.003 per 1K input tokens
- ~$0.015 per 1K output tokens
- Typical demo run: ~$5-10

## Next Steps

- Add email notifications for high-value customers
- Integrate with external merchant APIs
- Build real-time dashboard
- Add A/B testing for recommendation strategies

## License

MIT License - See LICENSE file for details

## Contributing

Pull requests welcome! Please ensure all credentials are removed before submitting.
