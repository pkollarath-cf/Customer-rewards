output "environment_id" {
  description = "Confluent Environment ID"
  value       = confluent_environment.demo.id
}

output "kafka_cluster_id" {
  description = "Kafka Cluster ID"
  value       = confluent_kafka_cluster.basic.id
}

output "kafka_cluster_bootstrap_servers" {
  description = "Kafka Cluster Bootstrap Servers"
  value       = confluent_kafka_cluster.basic.bootstrap_endpoint
}

output "kafka_cluster_rest_endpoint" {
  description = "Kafka Cluster REST Endpoint"
  value       = confluent_kafka_cluster.basic.rest_endpoint
}

output "kafka_api_key" {
  description = "Kafka API Key"
  value       = confluent_api_key.app_kafka_api_key.id
  sensitive   = true
}

output "kafka_api_secret" {
  description = "Kafka API Secret"
  value       = confluent_api_key.app_kafka_api_key.secret
  sensitive   = true
}

output "schema_registry_id" {
  description = "Schema Registry Cluster ID"
  value       = data.confluent_schema_registry_cluster.essentials.id
}

output "schema_registry_url" {
  description = "Schema Registry REST Endpoint"
  value       = data.confluent_schema_registry_cluster.essentials.rest_endpoint
}

output "schema_registry_api_key" {
  description = "Schema Registry API Key"
  value       = confluent_api_key.app_sr_api_key.id
  sensitive   = true
}

output "schema_registry_api_secret" {
  description = "Schema Registry API Secret"
  value       = confluent_api_key.app_sr_api_key.secret
  sensitive   = true
}

output "flink_compute_pool_id" {
  description = "Flink Compute Pool ID"
  value       = confluent_flink_compute_pool.main.id
}

output "flink_rest_endpoint" {
  description = "Flink REST Endpoint"
  value       = "https://flink.${var.flink_compute_pool_region}.${lower(var.flink_compute_pool_cloud)}.confluent.cloud"
}

output "service_account_id" {
  description = "Service Account ID"
  value       = confluent_service_account.app.id
}

output "topics_created" {
  description = "Kafka Topics Created"
  value = [
    confluent_kafka_topic.customer_details.topic_name,
    confluent_kafka_topic.loyalty_usage_history.topic_name,
    confluent_kafka_topic.address_changes.topic_name
  ]
}

output "env_file_content" {
  description = "Content for scripts/.env file"
  value       = <<-EOT
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=${replace(confluent_kafka_cluster.basic.bootstrap_endpoint, "SASL_SSL://", "")}
KAFKA_API_KEY=${confluent_api_key.app_kafka_api_key.id}
KAFKA_API_SECRET=${confluent_api_key.app_kafka_api_key.secret}

# Schema Registry Configuration
SCHEMA_REGISTRY_URL=${data.confluent_schema_registry_cluster.essentials.rest_endpoint}
SCHEMA_REGISTRY_API_KEY=${confluent_api_key.app_sr_api_key.id}
SCHEMA_REGISTRY_API_SECRET=${confluent_api_key.app_sr_api_key.secret}
  EOT
  sensitive   = true
}

output "next_steps" {
  description = "Next steps to run the demo"
  value       = <<-EOT
✅ Infrastructure deployed successfully!

📋 Created Resources:
- Environment: ${confluent_environment.demo.display_name} (${confluent_environment.demo.id})
- Kafka Cluster: ${confluent_kafka_cluster.basic.display_name} (${confluent_kafka_cluster.basic.id})
- Flink Compute Pool: ${confluent_flink_compute_pool.main.display_name} (${confluent_flink_compute_pool.main.id})
- Service Account: ${confluent_service_account.app.display_name} (${confluent_service_account.app.id})
- Topics: customer_details, loyalty_usage_history, address_changes
- Flink Tables: ✓ Created
- AI Model & Agent: ✓ Created

📝 Next Steps:

1. Save credentials to scripts/.env:
   terraform output -raw env_file_content > ../scripts/.env

2. Install Python dependencies:
   cd ../scripts
   pip install -r requirements.txt

3. Generate sample data:
   python generate_data.py

4. Run Flink SQL queries in Confluent Cloud:
   - Open: https://confluent.cloud/environments/${confluent_environment.demo.id}/flink
   - Run sql/02_create_enriched_view.sql
   - Run sql/03_run_agent.sql

5. View recommendations:
   SELECT * FROM address_change_recommendations LIMIT 10;

🔗 Quick Links:
- Confluent Cloud: https://confluent.cloud/environments/${confluent_environment.demo.id}
- Kafka Cluster: https://confluent.cloud/environments/${confluent_environment.demo.id}/clusters/${confluent_kafka_cluster.basic.id}
- Flink SQL: https://confluent.cloud/environments/${confluent_environment.demo.id}/flink
  EOT
}
