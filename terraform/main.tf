terraform {
  required_version = ">= 1.5"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
  }
}

provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

data "confluent_organization" "main" {}

# Environment
resource "confluent_environment" "demo" {
  display_name = var.environment_name

  stream_governance {
    package = "ESSENTIALS"
  }
}

# Service Account
resource "confluent_service_account" "app" {
  display_name = var.service_account_name
  description  = "Service account for rewards demo"
}

# Kafka Cluster
resource "confluent_kafka_cluster" "basic" {
  display_name = var.kafka_cluster_name
  availability = "SINGLE_ZONE"
  cloud        = var.kafka_cluster_cloud
  region       = var.kafka_cluster_region
  basic {}

  environment {
    id = confluent_environment.demo.id
  }
}

# Kafka API Keys
resource "confluent_api_key" "app_kafka_api_key" {
  display_name = "app-kafka-api-key"
  description  = "Kafka API Key for app service account"

  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.basic.id
    api_version = confluent_kafka_cluster.basic.api_version
    kind        = confluent_kafka_cluster.basic.kind

    environment {
      id = confluent_environment.demo.id
    }
  }
}

# Schema Registry
data "confluent_schema_registry_cluster" "essentials" {
  environment {
    id = confluent_environment.demo.id
  }

  depends_on = [confluent_kafka_cluster.basic]
}

# Schema Registry API Keys
resource "confluent_api_key" "app_sr_api_key" {
  display_name = "app-sr-api-key"
  description  = "Schema Registry API Key for app service account"

  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.essentials.id
    api_version = data.confluent_schema_registry_cluster.essentials.api_version
    kind        = data.confluent_schema_registry_cluster.essentials.kind

    environment {
      id = confluent_environment.demo.id
    }
  }
}

# Flink Compute Pool
resource "confluent_flink_compute_pool" "main" {
  display_name = var.flink_compute_pool_name
  cloud        = var.flink_compute_pool_cloud
  region       = var.flink_compute_pool_region
  max_cfu      = var.flink_max_cfu

  environment {
    id = confluent_environment.demo.id
  }
}

# Role Bindings for Service Account
resource "confluent_role_binding" "app_kafka_cluster_admin" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.basic.rbac_crn
}

resource "confluent_role_binding" "app_environment_admin" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.demo.resource_name
}

# Kafka Topics
resource "confluent_kafka_topic" "customer_details" {
  kafka_cluster {
    id = confluent_kafka_cluster.basic.id
  }

  topic_name       = "customer_details"
  partitions_count = 6
  rest_endpoint    = confluent_kafka_cluster.basic.rest_endpoint

  credentials {
    key    = confluent_api_key.app_kafka_api_key.id
    secret = confluent_api_key.app_kafka_api_key.secret
  }

  config = {
    "cleanup.policy"    = "compact"
    "retention.ms"      = "604800000" # 7 days
    "max.message.bytes" = "2097164"   # 2MB
  }

  depends_on = [confluent_role_binding.app_kafka_cluster_admin]
}

resource "confluent_kafka_topic" "loyalty_usage_history" {
  kafka_cluster {
    id = confluent_kafka_cluster.basic.id
  }

  topic_name       = "loyalty_usage_history"
  partitions_count = 6
  rest_endpoint    = confluent_kafka_cluster.basic.rest_endpoint

  credentials {
    key    = confluent_api_key.app_kafka_api_key.id
    secret = confluent_api_key.app_kafka_api_key.secret
  }

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "2592000000" # 30 days
  }

  depends_on = [confluent_role_binding.app_kafka_cluster_admin]
}

resource "confluent_kafka_topic" "address_changes" {
  kafka_cluster {
    id = confluent_kafka_cluster.basic.id
  }

  topic_name       = "address_changes"
  partitions_count = 6
  rest_endpoint    = confluent_kafka_cluster.basic.rest_endpoint

  credentials {
    key    = confluent_api_key.app_kafka_api_key.id
    secret = confluent_api_key.app_kafka_api_key.secret
  }

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "604800000" # 7 days
  }

  depends_on = [confluent_role_binding.app_kafka_cluster_admin]
}

# Flink Table: customer_details
resource "confluent_flink_statement" "customer_details_table" {
  organization {
    id = data.confluent_organization.main.id
  }

  environment {
    id = confluent_environment.demo.id
  }

  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }

  principal {
    id = confluent_service_account.app.id
  }

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS customer_details (
      customer_id STRING NOT NULL,
      name STRING NOT NULL,
      permanent_address STRING,
      current_address STRING,
      current_zip STRING NOT NULL,
      loyalty_tier STRING,
      loyalty_points INT,
      email STRING NOT NULL,
      updated_at TIMESTAMP(3) WITH LOCAL TIME ZONE,
      PRIMARY KEY (customer_id) NOT ENFORCED
    ) WITH (
      'changelog.mode' = 'upsert',
      'kafka.retention.time' = '7 d'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.demo.id
    "sql.current-database" = confluent_kafka_cluster.basic.id
  }

  rest_endpoint = "https://flink.${var.flink_compute_pool_region}.${lower(var.flink_compute_pool_cloud)}.confluent.cloud"

  credentials {
    key    = var.confluent_cloud_api_key
    secret = var.confluent_cloud_api_secret
  }

  depends_on = [
    confluent_kafka_topic.customer_details,
    confluent_role_binding.app_environment_admin
  ]
}

# Flink Table: loyalty_usage_history
resource "confluent_flink_statement" "loyalty_usage_history_table" {
  organization {
    id = data.confluent_organization.main.id
  }

  environment {
    id = confluent_environment.demo.id
  }

  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }

  principal {
    id = confluent_service_account.app.id
  }

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS loyalty_usage_history (
      usage_id STRING NOT NULL,
      customer_id STRING NOT NULL,
      merchant STRING NOT NULL,
      product_service STRING,
      loyalty_points_used INT,
      transaction_timestamp TIMESTAMP(3) WITH LOCAL TIME ZONE,
      PRIMARY KEY (usage_id) NOT ENFORCED
    ) WITH (
      'kafka.retention.time' = '30 d'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.demo.id
    "sql.current-database" = confluent_kafka_cluster.basic.id
  }

  rest_endpoint = "https://flink.${var.flink_compute_pool_region}.${lower(var.flink_compute_pool_cloud)}.confluent.cloud"

  credentials {
    key    = var.confluent_cloud_api_key
    secret = var.confluent_cloud_api_secret
  }

  depends_on = [
    confluent_kafka_topic.loyalty_usage_history,
    confluent_role_binding.app_environment_admin
  ]
}

# Flink Table: address_changes
resource "confluent_flink_statement" "address_changes_table" {
  organization {
    id = data.confluent_organization.main.id
  }

  environment {
    id = confluent_environment.demo.id
  }

  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }

  principal {
    id = confluent_service_account.app.id
  }

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS address_changes (
      event_id STRING NOT NULL,
      customer_id STRING NOT NULL,
      customer_name STRING NOT NULL,
      customer_email STRING NOT NULL,
      loyalty_tier STRING,
      loyalty_points INT,
      old_zip STRING,
      new_zip STRING NOT NULL,
      change_timestamp TIMESTAMP(3) WITH LOCAL TIME ZONE,
      PRIMARY KEY (event_id) NOT ENFORCED
    ) WITH (
      'kafka.retention.time' = '7 d'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.demo.id
    "sql.current-database" = confluent_kafka_cluster.basic.id
  }

  rest_endpoint = "https://flink.${var.flink_compute_pool_region}.${lower(var.flink_compute_pool_cloud)}.confluent.cloud"

  credentials {
    key    = var.confluent_cloud_api_key
    secret = var.confluent_cloud_api_secret
  }

  depends_on = [
    confluent_kafka_topic.address_changes,
    confluent_role_binding.app_environment_admin
  ]
}

# Bedrock Connection
resource "confluent_flink_statement" "bedrock_connection" {
  organization {
    id = data.confluent_organization.main.id
  }

  environment {
    id = confluent_environment.demo.id
  }

  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }

  principal {
    id = confluent_service_account.app.id
  }

  rest_endpoint = "https://flink.${var.flink_compute_pool_region}.${lower(var.flink_compute_pool_cloud)}.confluent.cloud"

  credentials {
    key    = var.confluent_cloud_api_key
    secret = var.confluent_cloud_api_secret
  }

  statement_name = "bedrock-connection-create"

  statement = <<-EOT
    CREATE CONNECTION IF NOT EXISTS `${confluent_environment.demo.id}`.`${confluent_kafka_cluster.basic.id}`.`rewards-bedrock-connection`
    WITH (
      'type' = 'bedrock',
      'endpoint' = 'https://bedrock-runtime.${var.aws_bedrock_region}.amazonaws.com',
      'bedrock.region' = '${var.aws_bedrock_region}',
      'bedrock.api-key' = '${var.aws_access_key}',
      'bedrock.secret-key' = '${var.aws_secret_key}'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.demo.id
    "sql.current-database" = confluent_kafka_cluster.basic.id
  }

  lifecycle {
    ignore_changes = [statement]
  }

  depends_on = [confluent_role_binding.app_environment_admin]
}

# AI Model
resource "confluent_flink_statement" "ai_model" {
  organization {
    id = data.confluent_organization.main.id
  }

  environment {
    id = confluent_environment.demo.id
  }

  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }

  principal {
    id = confluent_service_account.app.id
  }

  statement = "CREATE MODEL IF NOT EXISTS `${confluent_environment.demo.id}`.`${confluent_kafka_cluster.basic.id}`.`rewards_model` INPUT (prompt STRING) OUTPUT (response STRING) WITH ( 'provider' = 'bedrock', 'task' = 'text_generation', 'bedrock.connection' = 'rewards-bedrock-connection', 'bedrock.params.max_tokens' = '50000' );"

  properties = {
    "sql.current-catalog"  = confluent_environment.demo.id
    "sql.current-database" = confluent_kafka_cluster.basic.id
  }

  rest_endpoint = "https://flink.${var.flink_compute_pool_region}.${lower(var.flink_compute_pool_cloud)}.confluent.cloud"

  credentials {
    key    = var.confluent_cloud_api_key
    secret = var.confluent_cloud_api_secret
  }

  depends_on = [confluent_flink_statement.bedrock_connection]
}

# AI Agent
resource "confluent_flink_statement" "rewards_agent" {
  organization {
    id = data.confluent_organization.main.id
  }

  environment {
    id = confluent_environment.demo.id
  }

  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }

  principal {
    id = confluent_service_account.app.id
  }

  statement = <<-EOT
    CREATE AGENT IF NOT EXISTS rewards_recommendation_agent
    USING MODEL rewards_model
    USING PROMPT 'You are a personalized rewards recommendation assistant for loyalty program cardholders.

When a customer changes their address, analyze:
1. Their new ZIP code location
2. Loyalty tier (Basic/Silver/Gold/Platinum)
3. Current loyalty points balance
4. Past merchant preferences from transaction history

Generate 3-5 personalized recommendations for:
- Local merchants in their new area based on their past shopping patterns
- Loyalty point redemption opportunities
- Tier-specific exclusive offers

Keep recommendations concise, relevant, and actionable.
Format as a friendly welcome message to their new location.
Be specific about point redemption values and merchant categories.

Example format:
"Welcome to [City/Area]! Based on your [Tier] status and [Points] points, we recommend:
1. [Specific merchant category] - [Point redemption suggestion]
2. [Another category] - [Benefit]
3. [Exclusive offer based on tier]"'
    COMMENT 'Agent that generates personalized merchant and loyalty recommendations when customers change address'
    WITH (
      'max_consecutive_failures' = '3',
      'MAX_ITERATIONS' = '12'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.demo.id
    "sql.current-database" = confluent_kafka_cluster.basic.id
  }

  rest_endpoint = "https://flink.${var.flink_compute_pool_region}.${lower(var.flink_compute_pool_cloud)}.confluent.cloud"

  credentials {
    key    = var.confluent_cloud_api_key
    secret = var.confluent_cloud_api_secret
  }

  depends_on = [confluent_flink_statement.ai_model]
}
