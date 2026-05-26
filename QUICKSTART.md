# Quick Start Guide

Get the demo running in 10 minutes!

## Prerequisites Checklist

- [ ] Confluent Cloud account ([Free trial](https://confluent.cloud))
- [ ] AWS account with Bedrock access
- [ ] Terraform installed
- [ ] Python 3.8+ installed

## Step-by-Step Setup

### 1. Clone the Repo (30 seconds)

```bash
git clone <your-repo-url>
cd customer-rewards-demo
```

### 2. Get Confluent Cloud API Key (2 minutes)

1. Go to https://confluent.cloud
2. Navigate to **Administration** → **Cloud API Keys**
3. Click **Add Key** → Create a **Cloud API Key**
4. Save the Key ID and Secret

### 3. Get AWS Bedrock Access (2 minutes)

1. Go to [AWS Bedrock Console](https://console.aws.amazon.com/bedrock)
2. Request access to **Claude Sonnet 4.5** (if not already enabled)
3. Create IAM user with **AmazonBedrockFullAccess**
4. Generate Access Key and Secret

### 4. Configure Terraform (1 minute)

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
```

Edit `terraform/terraform.tfvars` with your credentials:
```hcl
confluent_cloud_api_key    = "YOUR_CLOUD_API_KEY"
confluent_cloud_api_secret = "YOUR_CLOUD_API_SECRET"
aws_access_key             = "YOUR_AWS_ACCESS_KEY"
aws_secret_key             = "YOUR_AWS_SECRET_KEY"
```

### 5. Deploy Everything (5 minutes)

```bash
cd terraform
terraform init
terraform apply
# Review the plan and type 'yes'
```

Terraform creates everything:
- ✅ Confluent Environment
- ✅ Service Account
- ✅ Kafka Cluster (Basic)
- ✅ Schema Registry
- ✅ Flink Compute Pool
- ✅ All API Keys
- ✅ 3 Kafka Topics
- ✅ 3 Flink Tables  
- ✅ AI Model & Agent

### 6. Generate Data (2 minutes)

```bash
# Save credentials to .env file
terraform output -raw env_file_content > ../scripts/.env

# Install Python dependencies
cd ../scripts
pip install -r requirements.txt

# Generate sample data
python generate_data.py
# Let it run for 30-60 seconds to populate data, then Ctrl+C
```

### 7. Run the Agent

1. Open Confluent Cloud UI → Flink SQL Workspace
2. Copy & paste from `sql/02_create_enriched_view.sql` → Run
3. Copy & paste from `sql/03_run_agent.sql` → Run
4. View results:
   ```sql
   SELECT * FROM address_change_recommendations LIMIT 10;
   ```

## Success! 🎉

You should see AI-generated personalized recommendations for each customer address change.

Example output:
```
customer_name: Sarah Johnson
old_zip: 10001 (New York)
new_zip: 90210 (Beverly Hills)
loyalty_tier: Gold
loyalty_points: 45000
recommendations: "Welcome to Beverly Hills! Based on your Gold tier status 
                  and 45,000 points, we recommend:
                  1. Rodeo Drive Shopping - Redeem 15,000 points for $150 credit
                  2. Fine Dining at The Ivy - Use your Gold tier priority reservations
                  3. Exclusive Events - Gold member networking at local venues"
```

## Troubleshooting

### No recommendations appearing?

1. Check agent is running:
   ```sql
   SHOW JOBS;
   ```

2. Verify data is flowing:
   ```sql
   SELECT * FROM address_changes LIMIT 5;
   ```

3. Test model directly:
   ```sql
   SELECT ML_PREDICT('rewards_model', 'test', MAP['debug', 'true']);
   ```

### Terraform errors?

- Double-check all IDs in `terraform.tfvars`
- Ensure service account has Flink permissions
- Try `terraform destroy` then `terraform apply` again

### Python script errors?

- Verify `.env` file has correct credentials
- Check Kafka cluster is running
- Test connection:
  ```bash
  pip install confluent-kafka
  python -c "from confluent_kafka import Producer; print('OK')"
  ```

## Next Steps

- Try the example queries in `sql/04_query_examples.sql`
- Modify the agent prompt to generate different recommendations
- Add more merchants and loyalty tiers
- Build a dashboard to visualize results

## Need Help?

- Check main [README.md](README.md) for detailed documentation
- Review [Confluent Flink Docs](https://docs.confluent.io/cloud/current/flink/)
- Open an issue on GitHub
