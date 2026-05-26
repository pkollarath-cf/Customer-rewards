# Architecture & Component Details

## What Gets Created Where

### 🔧 Terraform Creates (Infrastructure)

Terraform provisions the **resource definitions** but doesn't start data processing:

| Resource | Type | Created By | Running? |
|----------|------|------------|----------|
| Environment | Confluent | Terraform | N/A |
| Kafka Cluster | Confluent | Terraform | ✅ Always on |
| Schema Registry | Confluent | Terraform | ✅ Always on |
| Flink Compute Pool | Confluent | Terraform | ✅ Always on (billed by CFU) |
| Service Account | Confluent | Terraform | N/A |
| API Keys | Confluent | Terraform | N/A |
| Kafka Topics (3) | Confluent | Terraform | ✅ Ready to receive data |
| Flink Tables (3) | Flink DDL | Terraform | ❌ Structure only |
| Bedrock Connection | Flink | Terraform | ✅ Ready to use |
| AI Model | Flink | Terraform | ✅ Definition ready |
| AI Agent | Flink | Terraform | ✅ Definition ready |

### ▶️ You Run Manually (Processing Jobs)

These SQL statements **start Flink jobs** that continuously process data:

| SQL File | What It Does | Creates Running Job? |
|----------|--------------|---------------------|
| `01_verify_tables.sql` | Verification queries | ❌ One-time SELECT queries |
| `02_create_enriched_view.sql` | Creates enriched view | ❌ View definition only |
| `03_run_agent.sql` | **Starts agent processing** | ✅ **YES - Continuous job** |
| `04_query_examples.sql` | Example queries | ❌ One-time SELECT queries |

## Understanding Flink Jobs vs. Definitions

### Definitions (Created by Terraform)

```sql
-- Terraform creates this (DDL = Data Definition Language)
CREATE TABLE address_changes (
  event_id STRING,
  customer_id STRING,
  ...
);

CREATE AGENT rewards_recommendation_agent
USING MODEL rewards_model
USING PROMPT '...';
```

**Result:** Table structure and agent configuration are registered, but nothing is processing yet.

### Running Jobs (You Start Manually)

```sql
-- You run this to START processing
CREATE TABLE address_change_recommendations AS
SELECT
    ...,
    agent_result.response
FROM address_changes_enriched ace,
LATERAL TABLE(
    AI_RUN_AGENT('rewards_recommendation_agent', ...)
) as agent_result(status, response);
```

**Result:** A continuously running Flink job that:
1. Reads each event from `address_changes`
2. Calls the AI agent
3. Writes results to `address_change_recommendations`

## How to See Running Jobs

### Option 1: Flink UI

1. Go to Confluent Cloud → Your Environment → Flink
2. Click **"Jobs"** tab
3. Look for:
   - `address_change_recommendations` - Status: **RUNNING** ✓
   
### Option 2: CLI

```bash
# List running statements
confluent flink statement list \
  --cloud aws \
  --region us-east-1 \
  --environment <env-id> \
  --compute-pool <pool-id>
```

### Option 3: Terraform State

```bash
# See what Terraform created
terraform state list

# Get details of a specific resource
terraform state show confluent_flink_statement.rewards_agent
```

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ TERRAFORM CREATES (One-time setup)                          │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   Kafka      │  │    Flink     │  │   Bedrock    │     │
│  │   Cluster    │  │  Compute Pool│  │  Connection  │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Tables (DDL)  │  Model  │  Agent Definition        │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘

                            ↓

┌─────────────────────────────────────────────────────────────┐
│ YOU RUN MANUALLY (Start processing)                         │
│                                                              │
│  Step 1: python generate_data.py                            │
│          ↓                                                   │
│  ┌─────────────────────────────────────┐                   │
│  │  address_changes topic (streaming)  │                   │
│  └─────────────────────────────────────┘                   │
│                                                              │
│  Step 2: Run sql/02_create_enriched_view.sql                │
│          ↓                                                   │
│  ┌─────────────────────────────────────┐                   │
│  │  address_changes_enriched (view)    │                   │
│  └─────────────────────────────────────┘                   │
│                                                              │
│  Step 3: Run sql/03_run_agent.sql                           │
│          ↓                                                   │
│  ┌──────────────────────────────────────────┐              │
│  │  FLINK JOB: address_change_recommendations│  ← RUNNING  │
│  │  • Reads from enriched view               │              │
│  │  • Calls AI_RUN_AGENT                     │              │
│  │  • Writes to output topic                 │              │
│  └──────────────────────────────────────────┘              │
└─────────────────────────────────────────────────────────────┘

                            ↓

┌─────────────────────────────────────────────────────────────┐
│ MONITOR (View results)                                       │
│                                                              │
│  SELECT * FROM address_change_recommendations;               │
│                                                              │
│  customer_name: John Smith                                   │
│  recommendations: "Welcome to Beverly Hills! ..."           │
└─────────────────────────────────────────────────────────────┘
```

## Resource Lifecycle

### Created Once (Terraform)
- Environment, Cluster, Compute Pool
- Service Account, API Keys
- Tables, Model, Agent definitions

### Runs Continuously (After you start it)
- Agent processing job
- Data generator (while script is running)

### One-Time Queries (As needed)
- Verification queries
- Example analytics queries

## Cost Implications

**What costs money:**
- ✅ Kafka Cluster (always running) - ~$0.10/hour (Basic)
- ✅ Flink Compute Pool - ~$0.35/CFU/hour × 5 CFU = ~$1.75/hour
- ✅ Bedrock API calls - ~$0.01 per recommendation
- ❌ Terraform state (free)
- ❌ Schema Registry Essentials (included)

**To minimize costs:**
1. Stop Flink jobs when not in use:
   ```sql
   -- In Flink console, click "Stop" on the job
   ```

2. Pause Flink Compute Pool:
   ```bash
   # In Confluent Cloud UI
   Flink → Compute Pools → <your-pool> → Pause
   ```

3. Delete everything when done:
   ```bash
   terraform destroy
   ```

## Troubleshooting

### "I don't see any running jobs"

You need to run `sql/03_run_agent.sql` manually. Terraform only creates the definitions.

### "Job status shows FAILED"

Check the job logs in Flink UI:
1. Click on the failed job
2. View "Exceptions" tab
3. Common issues:
   - No data in source tables yet
   - Bedrock connection not working
   - Agent definition has syntax errors

### "Agent runs but no recommendations appear"

1. Check data is flowing:
   ```sql
   SELECT * FROM address_changes LIMIT 5;
   ```

2. Check agent definition:
   ```sql
   SHOW CREATE AGENT rewards_recommendation_agent;
   ```

3. Test model directly:
   ```sql
   SELECT ML_PREDICT('rewards_model', 'test', MAP['debug', 'true']);
   ```
