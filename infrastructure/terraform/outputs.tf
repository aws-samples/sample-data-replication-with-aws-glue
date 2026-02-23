# =============================================================================
# Terraform Outputs
# AWS Glue Data Replication - Mirrors CFN Outputs section
# =============================================================================

# -----------------------------------------------------------------------------
# Core Resource Outputs
# -----------------------------------------------------------------------------

output "GlueJobName" {
  description = "Name of the created Glue job"
  value       = aws_glue_job.this.name
}

output "GlueJobRoleArn" {
  description = "ARN of the IAM role used by the Glue job"
  value       = aws_iam_role.glue_job.arn
}

output "SourceEngineType" {
  description = "Source database engine type"
  value       = var.SourceEngineType
}

output "TargetEngineType" {
  description = "Target database engine type"
  value       = var.TargetEngineType
}

# -----------------------------------------------------------------------------
# Network Connection Outputs
# -----------------------------------------------------------------------------

output "SourceNetworkConnectionName" {
  description = "Name of the source network connection for VPC access"
  value       = local.has_source_network_config ? aws_glue_connection.source_network[0].name : null
}

output "TargetNetworkConnectionName" {
  description = "Name of the target network connection for VPC access"
  value       = local.has_target_network_config ? aws_glue_connection.target_network[0].name : null
}

# -----------------------------------------------------------------------------
# JDBC Connection Outputs
# -----------------------------------------------------------------------------

output "SourceJdbcConnectionName" {
  description = "Name of the created source JDBC connection"
  value       = local.should_create_source_conn ? aws_glue_connection.source_jdbc[0].name : null
}

output "TargetJdbcConnectionName" {
  description = "Name of the created target JDBC connection"
  value       = local.should_create_target_conn ? aws_glue_connection.target_jdbc[0].name : null
}

# -----------------------------------------------------------------------------
# Secrets Manager Outputs
# -----------------------------------------------------------------------------

output "SourceDatabaseSecretArn" {
  description = "ARN of the AWS Secrets Manager secret for source database credentials"
  value       = local.should_create_source_conn ? aws_secretsmanager_secret.source_database[0].arn : null
}

output "TargetDatabaseSecretArn" {
  description = "ARN of the AWS Secrets Manager secret for target database credentials"
  value       = local.should_create_target_conn ? aws_secretsmanager_secret.target_database[0].arn : null
}

# -----------------------------------------------------------------------------
# Network Configuration Summary
# -----------------------------------------------------------------------------

output "NetworkConfiguration" {
  description = "Summary of network configuration used"
  value       = "Source: ${local.has_source_network_config ? "Cross-VPC" : "Same-VPC"}, Target: ${local.has_target_network_config ? "Cross-VPC" : "Same-VPC"}"
}

# -----------------------------------------------------------------------------
# VPC Endpoint Outputs
# -----------------------------------------------------------------------------

output "SourceS3VpcEndpointId" {
  description = "ID of the S3 VPC endpoint created in source database VPC"
  value       = local.create_source_s3_endpoint ? aws_vpc_endpoint.source_s3[0].id : null
}

output "TargetS3VpcEndpointId" {
  description = "ID of the S3 VPC endpoint created in target database VPC"
  value       = local.create_target_s3_endpoint ? aws_vpc_endpoint.target_s3[0].id : null
}

output "S3VpcEndpointConfiguration" {
  description = "Summary of S3 VPC endpoint configuration"
  value       = "Source S3 Endpoint: ${local.create_source_s3_endpoint ? "Created" : "Not Created"}, Target S3 Endpoint: ${local.create_target_s3_endpoint ? "Created" : "Not Created"}"
}

# -----------------------------------------------------------------------------
# Glue Connection Configuration Summary
# -----------------------------------------------------------------------------

output "GlueConnectionConfiguration" {
  description = "Summary of Glue Connection configuration with Secrets Manager integration"
  value = join(", ", [
    "Source JDBC: ${local.should_create_source_conn ? "Created with Secrets Manager" : "Not Created"}",
    "Target JDBC: ${local.should_create_target_conn ? "Created with Secrets Manager" : "Not Created"}",
    "Source Existing: ${local.has_use_source_connection ? var.UseSourceConnection : "None"}",
    "Target Existing: ${local.has_use_target_connection ? var.UseTargetConnection : "None"}",
    "Secrets Manager: ${local.should_create_source_conn && local.should_create_target_conn ? "Both connections use Secrets Manager" : local.should_create_source_conn ? "Source connection uses Secrets Manager" : local.should_create_target_conn ? "Target connection uses Secrets Manager" : "Not used"}",
  ])
}

# -----------------------------------------------------------------------------
# Observability Outputs
# -----------------------------------------------------------------------------

output "GlueJobLogGroup" {
  description = "CloudWatch Log Group for Glue job execution logs"
  value       = aws_cloudwatch_log_group.glue_job.name
}

output "GlueJobOutputLogGroup" {
  description = "CloudWatch Log Group for Glue job output logs"
  value       = aws_cloudwatch_log_group.glue_job_output.name
}

output "GlueJobErrorLogGroup" {
  description = "CloudWatch Log Group for Glue job error logs"
  value       = aws_cloudwatch_log_group.glue_job_error.name
}

output "CloudWatchDashboard" {
  description = "CloudWatch Dashboard for Glue job monitoring"
  value       = local.enable_dashboard ? "https://${data.aws_region.current.name}.console.aws.amazon.com/cloudwatch/home?region=${data.aws_region.current.name}#dashboards:name=${var.JobName}-glue-monitoring" : null
}

output "LogInsightsQueries" {
  description = "CloudWatch Log Insights saved queries for analysis"
  value       = "https://${data.aws_region.current.name}.console.aws.amazon.com/cloudwatch/home?region=${data.aws_region.current.name}#logsV2:logs-insights"
}

# -----------------------------------------------------------------------------
# Alarm Outputs
# -----------------------------------------------------------------------------

output "GlueJobFailureAlarm" {
  description = "CloudWatch Alarm for Glue job failures"
  value       = local.enable_alarms ? aws_cloudwatch_metric_alarm.job_failure[0].alarm_name : null
}

output "GlueJobMemoryAlarm" {
  description = "CloudWatch Alarm for high memory usage"
  value       = local.enable_alarms ? aws_cloudwatch_metric_alarm.high_memory[0].alarm_name : null
}

output "GlueJobDurationAlarm" {
  description = "CloudWatch Alarm for long running jobs"
  value       = local.enable_alarms ? aws_cloudwatch_metric_alarm.long_duration[0].alarm_name : null
}

output "AlarmNotificationTopic" {
  description = "SNS Topic for alarm notifications"
  value       = local.create_alarm_topic ? aws_sns_topic.alarm_notifications[0].arn : null
}

output "ObservabilityConfiguration" {
  description = "Summary of observability components created"
  value       = "Log Groups: 3, Dashboard: ${local.enable_dashboard ? "1" : "0"}, Alarms: ${local.enable_alarms ? "3" : "0"}, Log Insights Queries: 3"
}
