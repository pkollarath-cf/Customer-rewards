variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API Key (Cloud resource management)"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API Secret"
  type        = string
  sensitive   = true
}

variable "aws_access_key" {
  description = "AWS Access Key for Bedrock"
  type        = string
  sensitive   = true
}

variable "aws_secret_key" {
  description = "AWS Secret Key for Bedrock"
  type        = string
  sensitive   = true
}

variable "aws_bedrock_region" {
  description = "AWS Region for Bedrock (e.g., us-east-1)"
  type        = string
  default     = "us-east-1"
}

variable "environment_name" {
  description = "Name for the Confluent Cloud Environment"
  type        = string
  default     = "customer-rewards-demo"
}

variable "kafka_cluster_name" {
  description = "Name for the Kafka Cluster"
  type        = string
  default     = "rewards-cluster"
}

variable "kafka_cluster_cloud" {
  description = "Cloud provider for Kafka cluster (AWS, GCP, AZURE)"
  type        = string
  default     = "AWS"
}

variable "kafka_cluster_region" {
  description = "Cloud region for Kafka cluster"
  type        = string
  default     = "us-east-1"
}

variable "flink_compute_pool_name" {
  description = "Name for the Flink Compute Pool"
  type        = string
  default     = "rewards-flink-pool"
}

variable "flink_compute_pool_cloud" {
  description = "Cloud provider for Flink (AWS, GCP, AZURE)"
  type        = string
  default     = "AWS"
}

variable "flink_compute_pool_region" {
  description = "Cloud region for Flink"
  type        = string
  default     = "us-east-1"
}

variable "flink_max_cfu" {
  description = "Maximum CFUs for Flink compute pool"
  type        = number
  default     = 5
}

variable "service_account_name" {
  description = "Name for the service account"
  type        = string
  default     = "rewards-service-account"
}
