# =============================================================================
# Standalone Terraform Script - AWS Glue JDBC Connection
# Creates a Glue JDBC Connection with Secrets Manager credentials,
# matching the behavior of main.tf when CreateSourceConnection or
# CreateTargetConnection is set to "true".
# =============================================================================

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 4.0"
    }
  }
}

provider "aws" {}

# =============================================================================
# Variables
# =============================================================================

variable "connection_name" {
  type        = string
  description = "Name for the Glue JDBC connection (e.g., my-job-source-jdbc-connection)"
  validation {
    condition     = can(regex("^[a-zA-Z0-9_-]+$", var.connection_name)) && length(var.connection_name) >= 1 && length(var.connection_name) <= 255
    error_message = "Must contain only alphanumeric characters, hyphens, and underscores (1-255 chars)"
  }
}

variable "engine_type" {
  type        = string
  description = "Database engine type"
  validation {
    condition     = contains(["oracle", "sqlserver", "postgresql", "db2"], var.engine_type)
    error_message = "Must be one of: oracle, sqlserver, postgresql, db2"
  }
}

variable "jdbc_connection_url" {
  type        = string
  description = "JDBC connection string (e.g., jdbc:sqlserver://host:1433;databaseName=mydb)"
  validation {
    condition     = can(regex("^jdbc:[a-zA-Z0-9]+(:[a-zA-Z0-9]+)*:(//|@//|@).+$", var.jdbc_connection_url))
    error_message = "Must be a valid JDBC connection string"
  }
}

variable "jdbc_driver_s3_path" {
  type        = string
  description = "S3 path to the JDBC driver JAR file (e.g., s3://bucket/drivers/driver.jar)"
  validation {
    condition     = can(regex("^s3://[a-zA-Z0-9.-]+/.*\\.jar$", var.jdbc_driver_s3_path))
    error_message = "Must be a valid S3 path ending with .jar"
  }
}

variable "db_username" {
  type        = string
  description = "Database username"
}

variable "db_password" {
  type        = string
  description = "Database password"
  sensitive   = true
}

variable "secrets_manager_kms_key_arn" {
  type        = string
  description = "ARN of the KMS key for encrypting the Secrets Manager secret (optional, uses default aws/secretsmanager key if empty)"
  default     = ""
}

# Optional VPC / network configuration
variable "subnet_id" {
  type        = string
  description = "Subnet ID for the connection's physical network requirements (optional)"
  default     = ""
}

variable "security_group_ids" {
  type        = list(string)
  description = "List of security group IDs for the connection's physical network requirements (optional)"
  default     = []
}

variable "availability_zone" {
  type        = string
  description = "Availability zone for the connection's physical network requirements (optional)"
  default     = ""
}

# =============================================================================
# Locals
# =============================================================================

locals {
  jdbc_driver_classes = {
    oracle     = "oracle.jdbc.OracleDriver"
    sqlserver  = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    postgresql = "org.postgresql.Driver"
    db2        = "com.ibm.db2.jcc.DB2Driver"
  }

  jdbc_driver_class    = local.jdbc_driver_classes[var.engine_type]
  has_network_config   = var.subnet_id != "" && length(var.security_group_ids) > 0 && var.availability_zone != ""
  has_kms_key          = var.secrets_manager_kms_key_arn != ""
}

# =============================================================================
# Secrets Manager - Database Credentials
# =============================================================================

resource "aws_secretsmanager_secret" "db_credentials" {
  name                    = "/aws-glue/${var.connection_name}"
  description             = "Database credentials for ${var.engine_type} Glue JDBC connection - ${var.connection_name}"
  kms_key_id              = local.has_kms_key ? var.secrets_manager_kms_key_arn : null
  recovery_window_in_days = 0

  tags = {
    Name    = "${var.connection_name}-db-credentials"
    Purpose = "Database credentials for Glue JDBC Connection"
  }
}

resource "aws_secretsmanager_secret_version" "db_credentials" {
  secret_id = aws_secretsmanager_secret.db_credentials.id
  secret_string = jsonencode({
    username = var.db_username
    password = var.db_password
  })
}

# =============================================================================
# Glue JDBC Connection
# =============================================================================

resource "aws_glue_connection" "jdbc" {
  name        = var.connection_name
  description = "JDBC connection for ${var.engine_type} database - Uses AWS Secrets Manager for credentials"

  connection_type = "JDBC"

  connection_properties = {
    JDBC_CONNECTION_URL    = var.jdbc_connection_url
    SECRET_ID              = aws_secretsmanager_secret.db_credentials.name
    JDBC_DRIVER_JAR_URI    = var.jdbc_driver_s3_path
    JDBC_DRIVER_CLASS_NAME = local.jdbc_driver_class
  }

  dynamic "physical_connection_requirements" {
    for_each = local.has_network_config ? [1] : []
    content {
      security_group_id_list = var.security_group_ids
      subnet_id              = var.subnet_id
      availability_zone      = var.availability_zone
    }
  }

  tags = {
    Name = var.connection_name
  }

  depends_on = [aws_secretsmanager_secret.db_credentials]
}

# =============================================================================
# Outputs
# =============================================================================

output "connection_name" {
  description = "Name of the created Glue JDBC connection"
  value       = aws_glue_connection.jdbc.name
}

output "secret_arn" {
  description = "ARN of the Secrets Manager secret storing database credentials"
  value       = aws_secretsmanager_secret.db_credentials.arn
}

output "secret_name" {
  description = "Name of the Secrets Manager secret"
  value       = aws_secretsmanager_secret.db_credentials.name
}
