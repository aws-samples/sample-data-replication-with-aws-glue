# =============================================================================
# Terraform Main Configuration
# AWS Glue Data Replication - All resources, locals, and data sources
# =============================================================================

# -----------------------------------------------------------------------------
# Data Sources
# -----------------------------------------------------------------------------

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# -----------------------------------------------------------------------------
# Locals — Condition Booleans (mirrors CFN Conditions 1:1)
# -----------------------------------------------------------------------------

locals {
  # Engine Type Conditions
  is_source_iceberg = var.SourceEngineType == "iceberg"
  is_target_iceberg = var.TargetEngineType == "iceberg"
  is_iceberg_job    = local.is_source_iceberg || local.is_target_iceberg

  # Worker Type Conditions
  is_standard_worker   = var.WorkerType == "Standard"
  has_glue_job_modules = var.GlueJobModulesS3Path != ""

  # Security Conditions
  has_secrets_manager_kms_key = var.SecretsManagerKmsKeyArn != ""

  # JDBC Driver Conditions
  has_source_jdbc_driver = !local.is_source_iceberg && var.SourceJdbcDriverS3Path != ""
  has_target_jdbc_driver = !local.is_target_iceberg && var.TargetJdbcDriverS3Path != ""

  # Network Configuration Conditions
  has_source_network_config      = var.SourceVpcId != ""
  has_target_network_config      = var.TargetVpcId != ""
  has_both_network_configs       = local.has_source_network_config && local.has_target_network_config
  has_only_source_network_config = local.has_source_network_config && !local.has_target_network_config
  has_only_target_network_config = local.has_target_network_config && !local.has_source_network_config

  # VPC Endpoint Conditions
  create_source_s3_endpoint   = local.has_source_network_config && var.CreateSourceS3VpcEndpoint == "YES"
  create_target_s3_endpoint   = local.has_target_network_config && var.CreateTargetS3VpcEndpoint == "YES"
  create_source_glue_endpoint = local.has_source_network_config && var.CreateSourceGlueVpcEndpoint == "YES"
  create_target_glue_endpoint = local.has_target_network_config && var.CreateTargetGlueVpcEndpoint == "YES"
  create_any_glue_endpoint    = local.create_source_glue_endpoint || local.create_target_glue_endpoint

  # Kerberos Authentication Conditions
  has_source_kerberos = var.SourceKerberosSPN != "" && var.SourceKerberosDomain != "" && var.SourceKerberosKDC != ""
  has_target_kerberos = var.TargetKerberosSPN != "" && var.TargetKerberosDomain != "" && var.TargetKerberosKDC != ""

  # Keytab Authentication Conditions
  has_source_keytab = var.SourceKerberosKeytabS3Path != ""
  has_target_keytab = var.TargetKerberosKeytabS3Path != ""

  # Glue Connection Conditions
  should_create_source_conn = !local.is_source_iceberg && var.CreateSourceConnection == "true" && !local.has_source_kerberos
  should_create_target_conn = !local.is_target_iceberg && var.CreateTargetConnection == "true" && !local.has_target_kerberos
  has_use_source_connection  = !local.is_source_iceberg && var.UseSourceConnection != ""
  has_use_target_connection  = !local.is_target_iceberg && var.UseTargetConnection != ""

  # Kerberos Connection Conditions
  should_create_source_kerberos = !local.is_source_iceberg && var.CreateSourceConnection == "true" && local.has_source_kerberos
  should_create_target_kerberos = !local.is_target_iceberg && var.CreateTargetConnection == "true" && local.has_target_kerberos

  # Observability Conditions
  enable_dashboard   = var.EnableCloudWatchDashboard == "YES"
  enable_alarms      = var.EnableCloudWatchAlarms == "YES"
  has_alarm_email    = var.AlarmNotificationEmail != ""
  create_alarm_topic = local.enable_alarms && local.has_alarm_email

  # Password Conditions — password is only needed for direct JDBC (no Kerberos, no Glue connections)
  source_needs_password = !local.is_source_iceberg && !local.has_source_kerberos && !local.should_create_source_conn && !local.has_use_source_connection
  target_needs_password = !local.is_target_iceberg && !local.has_target_kerberos && !local.should_create_target_conn && !local.has_use_target_connection

  # JDBC Driver Class Mapping
  jdbc_driver_classes = {
    oracle     = "oracle.jdbc.OracleDriver"
    sqlserver  = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    postgresql = "org.postgresql.Driver"
    db2        = "com.ibm.db2.jcc.DB2Driver"
  }
  source_jdbc_driver_class = lookup(local.jdbc_driver_classes, var.SourceEngineType, "")
  target_jdbc_driver_class = lookup(local.jdbc_driver_classes, var.TargetEngineType, "")

  # S3 Bucket Name Extraction (split on "/" take element [2])
  script_bucket_name         = split("/", var.GlueJobScriptS3Path)[2]
  source_jdbc_driver_bucket  = local.has_source_jdbc_driver ? split("/", var.SourceJdbcDriverS3Path)[2] : ""
  target_jdbc_driver_bucket  = local.has_target_jdbc_driver ? split("/", var.TargetJdbcDriverS3Path)[2] : ""
  modules_bucket_name        = local.has_glue_job_modules ? split("/", var.GlueJobModulesS3Path)[2] : ""
  source_warehouse_bucket    = local.is_source_iceberg ? split("/", var.SourceWarehouseLocation)[2] : ""
  target_warehouse_bucket    = local.is_target_iceberg ? split("/", var.TargetWarehouseLocation)[2] : ""

  # Connection List Computation
  connection_list = compact([
    local.has_source_network_config ? "${var.JobName}-source-network-connection" : "",
    local.has_target_network_config ? "${var.JobName}-target-network-connection" : "",
  ])
}


# =============================================================================
# IAM Role and Policy
# =============================================================================

resource "aws_iam_role" "glue_job" {
  name = "${var.JobName}-glue-job-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name    = "${var.JobName}-glue-job-role"
    JobName = var.JobName
  }
}

resource "aws_iam_role_policy" "glue_job" {
  name = "${var.JobName}-glue-job-policy"
  role = aws_iam_role.glue_job.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [
        # S3 permissions for job scripts (READ-ONLY)
        {
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:GetObjectVersion"
          ]
          Resource = [
            "arn:aws:s3:::${local.script_bucket_name}/*"
          ]
        },
        # S3 permissions for job script bucket (list and location only)
        {
          Effect = "Allow"
          Action = [
            "s3:ListBucket",
            "s3:GetBucketLocation"
          ]
          Resource = [
            "arn:aws:s3:::${local.script_bucket_name}"
          ]
        },
        # S3 permissions for bookmarks (read-write, scoped to bookmarks prefix)
        {
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:GetObjectVersion",
            "s3:PutObject",
            "s3:DeleteObject"
          ]
          Resource = [
            "arn:aws:s3:::${local.script_bucket_name}/bookmarks/*"
          ]
        },
        # S3 permissions for Kerberos config files (read-write, scoped to kerberos prefix)
        # The Glue job generates krb5.conf and jaas.conf at runtime and uploads them
        # to S3 for distribution to Spark executors
        {
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:GetObjectVersion",
            "s3:PutObject"
          ]
          Resource = [
            "arn:aws:s3:::${local.script_bucket_name}/kerberos/*"
          ]
        },
        # Explicit deny: prevent Glue job from modifying its own scripts
        {
          Effect = "Deny"
          Action = [
            "s3:PutObject",
            "s3:DeleteObject"
          ]
          Resource = [
            "arn:aws:s3:::${local.script_bucket_name}/scripts/*"
          ]
        },
        # S3 permissions for Glue temporary files and assets
        {
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject",
            "s3:ListBucket"
          ]
          Resource = [
            "arn:aws:s3:::aws-glue-assets-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.id}",
            "arn:aws:s3:::aws-glue-assets-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.id}/*",
            "arn:aws:s3:::aws-glue-temporary-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.id}",
            "arn:aws:s3:::aws-glue-temporary-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.id}/*"
          ]
        },
        # CloudWatch Logs permissions
        {
          Effect = "Allow"
          Action = [
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents",
            "logs:DescribeLogGroups",
            "logs:DescribeLogStreams",
            "logs:DescribeLogEvents",
            "logs:GetLogEvents",
            "logs:FilterLogEvents"
          ]
          Resource = [
            "arn:aws:logs:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/jobs/*",
            "arn:aws:logs:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/jobs/${var.JobName}:*",
            "arn:aws:logs:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/jobs/output/${var.JobName}:*",
            "arn:aws:logs:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/jobs/error/${var.JobName}:*"
          ]
        },
        # Glue Job Bookmark permissions
        {
          Effect = "Allow"
          Action = [
            "glue:GetJobBookmark",
            "glue:UpdateJobBookmark",
            "glue:ResetJobBookmark"
          ]
          Resource = "arn:aws:glue:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:job/${var.JobName}"
        },
        # Additional Glue permissions for job execution
        {
          Effect = "Allow"
          Action = [
            "glue:GetJob",
            "glue:GetJobRun",
            "glue:GetJobRuns",
            "glue:BatchStopJobRun"
          ]
          Resource = "arn:aws:glue:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:job/${var.JobName}"
        },
        # CloudWatch Metrics permissions
        {
          Effect = "Allow"
          Action = [
            "cloudwatch:PutMetricData",
            "cloudwatch:GetMetricStatistics",
            "cloudwatch:ListMetrics"
          ]
          Resource  = "*"
          Condition = {
            StringEquals = {
              "cloudwatch:namespace" = [
                "AWS/Glue",
                "AWS/Glue/DataReplication",
                "Glue"
              ]
            }
          }
        },
        # CloudWatch Dashboard permissions
        {
          Effect = "Allow"
          Action = [
            "cloudwatch:PutDashboard",
            "cloudwatch:GetDashboard",
            "cloudwatch:ListDashboards"
          ]
          Resource = [
            "arn:aws:cloudwatch::${data.aws_caller_identity.current.account_id}:dashboard/${var.JobName}*"
          ]
        },
        # CloudWatch Insights permissions
        {
          Effect = "Allow"
          Action = [
            "logs:StartQuery",
            "logs:StopQuery",
            "logs:GetQueryResults",
            "logs:DescribeQueries",
            "logs:PutQueryDefinition",
            "logs:DescribeQueryDefinitions"
          ]
          Resource = [
            "arn:aws:logs:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/jobs/*"
          ]
        },
        # Glue Connection permissions
        {
          Effect = "Allow"
          Action = [
            "glue:GetConnection",
            "glue:GetConnections",
            "glue:CreateConnection",
            "glue:UpdateConnection",
            "glue:DeleteConnection",
            "glue:TestConnection"
          ]
          Resource = compact([
            "arn:aws:glue:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:catalog",
            "arn:aws:glue:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:connection/${var.JobName}-source-network-connection",
            "arn:aws:glue:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:connection/${var.JobName}-target-network-connection",
            "arn:aws:glue:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:connection/${var.JobName}-source-jdbc-connection",
            "arn:aws:glue:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:connection/${var.JobName}-target-jdbc-connection",
            local.has_use_source_connection ? "arn:aws:glue:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:connection/${var.UseSourceConnection}" : "",
            local.has_use_target_connection ? "arn:aws:glue:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:connection/${var.UseTargetConnection}" : "",
          ])
        },
        # EC2 permissions for VPC network interface management
        {
          Effect = "Allow"
          Action = [
            "ec2:CreateNetworkInterface",
            "ec2:DeleteNetworkInterface",
            "ec2:AttachNetworkInterface",
            "ec2:DetachNetworkInterface",
            "ec2:ModifyNetworkInterfaceAttribute",
            "ec2:CreateTags",
            "ec2:DeleteTags"
          ]
          Resource = [
            "arn:aws:ec2:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:network-interface/*",
            "arn:aws:ec2:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:subnet/*",
            "arn:aws:ec2:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:security-group/*"
          ]
          Condition = {
            StringEquals = {
              "aws:RequestedRegion" = data.aws_region.current.id
            }
          }
        },
        # EC2 read-only describe permissions
        {
          Effect = "Allow"
          Action = [
            "ec2:DescribeNetworkInterfaces",
            "ec2:DescribeTags",
            "ec2:DescribeVpcEndpoints",
            "ec2:DescribeVpcEndpointServices",
            "ec2:DescribeRouteTables",
            "ec2:DescribeSecurityGroups",
            "ec2:DescribeSubnets",
            "ec2:DescribeVpcs",
            "ec2:DescribeAvailabilityZones"
          ]
          Resource  = "*"
          Condition = {
            StringEquals = {
              "aws:RequestedRegion" = data.aws_region.current.id
            }
          }
        },
        # VPC Endpoint S3 access permissions
        {
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:GetObjectVersion",
            "s3:ListBucket",
            "s3:GetBucketLocation",
            "s3:PutObject"
          ]
          Resource = [
            "arn:aws:s3:::aws-glue-assets-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.id}",
            "arn:aws:s3:::aws-glue-assets-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.id}/*",
            "arn:aws:s3:::aws-glue-temporary-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.id}",
            "arn:aws:s3:::aws-glue-temporary-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.id}/*"
          ]
        },
        # Secrets Manager permissions
        {
          Effect = "Allow"
          Action = [
            "secretsmanager:ListSecrets"
          ]
          Resource  = "*"
          Condition = {
            StringEquals = {
              "aws:RequestedRegion" = data.aws_region.current.id
            }
          }
        },
        {
          Effect = "Allow"
          Action = [
            "secretsmanager:CreateSecret",
            "secretsmanager:GetSecretValue",
            "secretsmanager:PutSecretValue",
            "secretsmanager:DescribeSecret",
            "secretsmanager:UpdateSecret",
            "secretsmanager:DeleteSecret",
            "secretsmanager:TagResource"
          ]
          Resource = [
            "arn:aws:secretsmanager:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:secret:/aws-glue/*"
          ]
        },
      ],
      # Conditional: S3 permissions for source JDBC driver
      local.has_source_jdbc_driver ? [
        {
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:GetObjectVersion"
          ]
          Resource = [
            "arn:aws:s3:::${local.source_jdbc_driver_bucket}/*"
          ]
        },
        # Additional S3 access for source JDBC drivers through VPC endpoints
        {
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:GetObjectVersion",
            "s3:ListBucket",
            "s3:GetBucketLocation"
          ]
          Resource = [
            "arn:aws:s3:::${local.source_jdbc_driver_bucket}",
            "arn:aws:s3:::${local.source_jdbc_driver_bucket}/*"
          ]
        },
        # S3 bookmark permissions for source JDBC driver bucket
        # (job auto-detects bookmark bucket from JDBC driver path)
        {
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:GetObjectVersion",
            "s3:PutObject",
            "s3:DeleteObject"
          ]
          Resource = [
            "arn:aws:s3:::${local.source_jdbc_driver_bucket}/bookmarks/*"
          ]
        },
      ] : [],
      # Conditional: S3 permissions for target JDBC driver
      local.has_target_jdbc_driver ? [
        {
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:GetObjectVersion"
          ]
          Resource = [
            "arn:aws:s3:::${local.target_jdbc_driver_bucket}/*"
          ]
        },
        # Additional S3 access for target JDBC drivers through VPC endpoints
        {
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:GetObjectVersion",
            "s3:ListBucket",
            "s3:GetBucketLocation"
          ]
          Resource = [
            "arn:aws:s3:::${local.target_jdbc_driver_bucket}",
            "arn:aws:s3:::${local.target_jdbc_driver_bucket}/*"
          ]
        },
        # S3 bookmark permissions for target JDBC driver bucket
        {
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:GetObjectVersion",
            "s3:PutObject",
            "s3:DeleteObject"
          ]
          Resource = [
            "arn:aws:s3:::${local.target_jdbc_driver_bucket}/bookmarks/*"
          ]
        },
      ] : [],
      # Conditional: S3 permissions for Glue job modules
      local.has_glue_job_modules ? [
        {
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:GetObjectVersion"
          ]
          Resource = [
            "arn:aws:s3:::${local.modules_bucket_name}/*"
          ]
        },
        {
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:GetObjectVersion",
            "s3:PutObject",
            "s3:DeleteObject"
          ]
          Resource = [
            "arn:aws:s3:::${local.modules_bucket_name}/bookmarks/*"
          ]
        },
        {
          Effect = "Allow"
          Action = [
            "s3:ListBucket",
            "s3:GetBucketLocation"
          ]
          Resource = [
            "arn:aws:s3:::${local.modules_bucket_name}"
          ]
        },
        # Explicit deny: prevent Glue job from modifying modules
        {
          Effect = "Deny"
          Action = [
            "s3:PutObject",
            "s3:DeleteObject"
          ]
          Resource = [
            "arn:aws:s3:::${local.modules_bucket_name}/scripts/*",
            "arn:aws:s3:::${local.modules_bucket_name}/modules/*"
          ]
        },
      ] : [],
      # Conditional: S3 permissions for source Iceberg warehouse
      local.is_source_iceberg ? [
        {
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:GetObjectVersion",
            "s3:PutObject",
            "s3:DeleteObject",
            "s3:ListBucket",
            "s3:GetBucketLocation"
          ]
          Resource = [
            "arn:aws:s3:::${local.source_warehouse_bucket}",
            "arn:aws:s3:::${local.source_warehouse_bucket}/*"
          ]
        },
        # Glue Data Catalog permissions for source Iceberg tables (read-only)
        {
          Effect = "Allow"
          Action = [
            "glue:GetDatabase",
            "glue:GetDatabases",
            "glue:GetTable",
            "glue:GetTables",
            "glue:GetPartition",
            "glue:GetPartitions",
            "glue:BatchGetPartition"
          ]
          Resource = [
            "arn:aws:glue:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:catalog",
            "arn:aws:glue:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:database/${var.SourceDatabase}",
            "arn:aws:glue:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:table/${var.SourceDatabase}/*"
          ]
        },
      ] : [],
      # Conditional: S3 permissions for target Iceberg warehouse
      local.is_target_iceberg ? [
        {
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:GetObjectVersion",
            "s3:PutObject",
            "s3:DeleteObject",
            "s3:ListBucket",
            "s3:GetBucketLocation"
          ]
          Resource = [
            "arn:aws:s3:::${local.target_warehouse_bucket}",
            "arn:aws:s3:::${local.target_warehouse_bucket}/*"
          ]
        },
        # Glue Data Catalog permissions for target Iceberg tables (read-write)
        {
          Effect = "Allow"
          Action = [
            "glue:GetDatabase",
            "glue:GetDatabases",
            "glue:CreateDatabase",
            "glue:UpdateDatabase",
            "glue:GetTable",
            "glue:GetTables",
            "glue:CreateTable",
            "glue:UpdateTable",
            "glue:DeleteTable",
            "glue:GetPartition",
            "glue:GetPartitions",
            "glue:CreatePartition",
            "glue:UpdatePartition",
            "glue:DeletePartition",
            "glue:BatchCreatePartition",
            "glue:BatchUpdatePartition",
            "glue:BatchDeletePartition",
            "glue:BatchGetPartition"
          ]
          Resource = [
            "arn:aws:glue:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:catalog",
            "arn:aws:glue:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:database/${var.TargetDatabase}",
            "arn:aws:glue:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:table/${var.TargetDatabase}/*"
          ]
        },
      ] : [],
      # Conditional: KMS permissions for Secrets Manager encryption
      [
        local.has_secrets_manager_kms_key ? {
          Effect = "Allow"
          Action = [
            "kms:Decrypt",
            "kms:GenerateDataKey",
            "kms:CreateGrant"
          ]
          Resource = [
            var.SecretsManagerKmsKeyArn
          ]
          Condition = {
            StringEquals = {
              "kms:ViaService" = "secretsmanager.${data.aws_region.current.id}.amazonaws.com"
            }
          }
        } : {
          Effect = "Allow"
          Action = [
            "kms:Decrypt",
            "kms:GenerateDataKey"
          ]
          Resource = [
            "arn:aws:kms:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:alias/aws/secretsmanager"
          ]
          Condition = {
            StringEquals = {
              "kms:ViaService" = "secretsmanager.${data.aws_region.current.id}.amazonaws.com"
            }
          }
        }
      ],
    )
  })
}


# =============================================================================
# CloudWatch Log Groups
# =============================================================================

resource "aws_cloudwatch_log_group" "glue_job" {
  name              = "/aws-glue/jobs/${var.JobName}"
  retention_in_days = var.LogRetentionDays

  tags = {
    Name    = "${var.JobName}-glue-job-logs"
    Purpose = "Glue job execution logs"
    JobName = var.JobName
  }
}

resource "aws_cloudwatch_log_group" "glue_job_output" {
  name              = "/aws-glue/jobs/output/${var.JobName}"
  retention_in_days = var.LogRetentionDays

  tags = {
    Name    = "${var.JobName}-glue-job-output-logs"
    Purpose = "Glue job output and application logs"
    JobName = var.JobName
  }
}

resource "aws_cloudwatch_log_group" "glue_job_error" {
  name              = "/aws-glue/jobs/error/${var.JobName}"
  retention_in_days = var.ErrorLogRetentionDays

  tags = {
    Name    = "${var.JobName}-glue-job-error-logs"
    Purpose = "Glue job error and exception logs"
    JobName = var.JobName
  }
}


# =============================================================================
# Secrets Manager Resources
# =============================================================================

resource "aws_secretsmanager_secret" "source_database" {
  count = local.should_create_source_conn ? 1 : 0

  name                    = "/aws-glue/${var.JobName}-source-jdbc-connection"
  description             = "Database credentials for source ${var.SourceEngineType} database - ${var.JobName}"
  kms_key_id              = local.has_secrets_manager_kms_key ? var.SecretsManagerKmsKeyArn : null
  recovery_window_in_days = 0

  tags = {
    Name    = "${var.JobName}-source-db-credentials"
    Purpose = "Source database credentials for Glue Connection"
    JobName = var.JobName
  }
}

resource "aws_secretsmanager_secret_version" "source_database" {
  count = local.should_create_source_conn ? 1 : 0

  secret_id = aws_secretsmanager_secret.source_database[0].id
  secret_string = jsonencode({
    username = var.SourceDbUser
    password = var.SourceDbPassword
  })
}

resource "aws_secretsmanager_secret" "target_database" {
  count = local.should_create_target_conn ? 1 : 0

  name                    = "/aws-glue/${var.JobName}-target-jdbc-connection"
  description             = "Database credentials for target ${var.TargetEngineType} database - ${var.JobName}"
  kms_key_id              = local.has_secrets_manager_kms_key ? var.SecretsManagerKmsKeyArn : null
  recovery_window_in_days = 0

  tags = {
    Name    = "${var.JobName}-target-db-credentials"
    Purpose = "Target database credentials for Glue Connection"
    JobName = var.JobName
  }
}

resource "aws_secretsmanager_secret_version" "target_database" {
  count = local.should_create_target_conn ? 1 : 0

  secret_id = aws_secretsmanager_secret.target_database[0].id
  secret_string = jsonencode({
    username = var.TargetDbUser
    password = var.TargetDbPassword
  })
}


# =============================================================================
# Glue Connection Resources
# =============================================================================

# --- Network Connections ---

resource "aws_glue_connection" "source_network" {
  count = local.has_source_network_config ? 1 : 0

  name        = "${var.JobName}-source-network-connection"
  description = "Network connection for source VPC access - ${var.SourceEngineType} database"

  connection_type = "NETWORK"

  physical_connection_requirements {
    security_group_id_list = split(",", var.SourceSecurityGroupIds)
    subnet_id              = split(",", var.SourceSubnetIds)[0]
    availability_zone      = var.SourceAvailabilityZone
  }

  tags = {
    Name    = "${var.JobName}-source-network-connection"
    JobName = var.JobName
  }
}

resource "aws_glue_connection" "target_network" {
  count = local.has_target_network_config ? 1 : 0

  name        = "${var.JobName}-target-network-connection"
  description = "Network connection for target VPC access - ${var.TargetEngineType} database"

  connection_type = "NETWORK"

  physical_connection_requirements {
    security_group_id_list = split(",", var.TargetSecurityGroupIds)
    subnet_id              = split(",", var.TargetSubnetIds)[0]
    availability_zone      = var.TargetAvailabilityZone
  }

  tags = {
    Name    = "${var.JobName}-target-network-connection"
    JobName = var.JobName
  }
}

# --- JDBC Connections ---

resource "aws_glue_connection" "source_jdbc" {
  count = local.should_create_source_conn ? 1 : 0

  name        = "${var.JobName}-source-jdbc-connection"
  description = "Universal JDBC connection for source ${var.SourceEngineType} database - Compatible with both programmatic jobs and Visual ETL - Uses AWS Secrets Manager for credentials"

  connection_type = "JDBC"

  connection_properties = {
    JDBC_CONNECTION_URL  = var.SourceConnectionString
    SECRET_ID            = aws_secretsmanager_secret.source_database[0].name
    JDBC_DRIVER_JAR_URI  = var.SourceJdbcDriverS3Path
    JDBC_DRIVER_CLASS_NAME = local.source_jdbc_driver_class
  }

  dynamic "physical_connection_requirements" {
    for_each = local.has_source_network_config ? [1] : []
    content {
      security_group_id_list = split(",", var.SourceSecurityGroupIds)
      subnet_id              = split(",", var.SourceSubnetIds)[0]
      availability_zone      = var.SourceAvailabilityZone
    }
  }

  tags = {
    Name    = "${var.JobName}-source-jdbc-connection"
    JobName = var.JobName
  }

  depends_on = [aws_secretsmanager_secret.source_database]
}

resource "aws_glue_connection" "target_jdbc" {
  count = local.should_create_target_conn ? 1 : 0

  name        = "${var.JobName}-target-jdbc-connection"
  description = "Universal JDBC connection for target ${var.TargetEngineType} database - Compatible with both programmatic jobs and Visual ETL - Uses AWS Secrets Manager for credentials"

  connection_type = "JDBC"

  connection_properties = {
    JDBC_CONNECTION_URL  = var.TargetConnectionString
    SECRET_ID            = aws_secretsmanager_secret.target_database[0].name
    JDBC_DRIVER_JAR_URI  = var.TargetJdbcDriverS3Path
    JDBC_DRIVER_CLASS_NAME = local.target_jdbc_driver_class
  }

  dynamic "physical_connection_requirements" {
    for_each = local.has_target_network_config ? [1] : []
    content {
      security_group_id_list = split(",", var.TargetSecurityGroupIds)
      subnet_id              = split(",", var.TargetSubnetIds)[0]
      availability_zone      = var.TargetAvailabilityZone
    }
  }

  tags = {
    Name    = "${var.JobName}-target-jdbc-connection"
    JobName = var.JobName
  }

  depends_on = [aws_secretsmanager_secret.target_database]
}


# =============================================================================
# VPC Endpoint Resources
# =============================================================================

# --- S3 VPC Endpoints ---

resource "aws_vpc_endpoint" "source_s3" {
  count = local.create_source_s3_endpoint ? 1 : 0

  vpc_id            = var.SourceVpcId
  service_name      = "com.amazonaws.${data.aws_region.current.id}.s3"
  vpc_endpoint_type = "Interface"
  subnet_ids        = split(",", var.SourceSubnetIds)
  security_group_ids = split(",", var.SourceSecurityGroupIds)

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = "*"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "arn:aws:s3:::${local.source_jdbc_driver_bucket}",
          "arn:aws:s3:::${local.source_jdbc_driver_bucket}/*",
          "arn:aws:s3:::${local.script_bucket_name}",
          "arn:aws:s3:::${local.script_bucket_name}/*",
          "arn:aws:s3:::aws-glue-assets-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.id}",
          "arn:aws:s3:::aws-glue-assets-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.id}/*",
          "arn:aws:s3:::aws-glue-temporary-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.id}",
          "arn:aws:s3:::aws-glue-temporary-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.id}/*"
        ]
      }
    ]
  })

  tags = {
    Name    = "${var.JobName}-source-s3-vpc-endpoint"
    Purpose = "JDBC driver access for Glue job"
    JobName = var.JobName
  }
}

resource "aws_vpc_endpoint" "target_s3" {
  count = local.create_target_s3_endpoint ? 1 : 0

  vpc_id            = var.TargetVpcId
  service_name      = "com.amazonaws.${data.aws_region.current.id}.s3"
  vpc_endpoint_type = "Interface"
  subnet_ids        = split(",", var.TargetSubnetIds)
  security_group_ids = split(",", var.TargetSecurityGroupIds)

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = "*"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "arn:aws:s3:::${local.target_jdbc_driver_bucket}",
          "arn:aws:s3:::${local.target_jdbc_driver_bucket}/*",
          "arn:aws:s3:::${local.script_bucket_name}",
          "arn:aws:s3:::${local.script_bucket_name}/*",
          "arn:aws:s3:::aws-glue-assets-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.id}",
          "arn:aws:s3:::aws-glue-assets-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.id}/*",
          "arn:aws:s3:::aws-glue-temporary-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.id}",
          "arn:aws:s3:::aws-glue-temporary-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.id}/*"
        ]
      }
    ]
  })

  tags = {
    Name    = "${var.JobName}-target-s3-vpc-endpoint"
    Purpose = "JDBC driver access for Glue job"
    JobName = var.JobName
  }
}

# --- Glue VPC Endpoints ---

resource "aws_vpc_endpoint" "source_glue" {
  count = local.create_source_glue_endpoint ? 1 : 0

  vpc_id            = var.SourceVpcId
  service_name      = "com.amazonaws.${data.aws_region.current.id}.glue"
  vpc_endpoint_type = "Interface"
  subnet_ids        = split(",", var.SourceSubnetIds)
  security_group_ids = split(",", var.SourceSecurityGroupIds)

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = "*"
        Action = [
          "glue:GetConnection",
          "glue:GetConnections",
          "glue:GetJob",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:GetJobBookmark",
          "glue:UpdateJobBookmark",
          "glue:ResetJobBookmark"
        ]
        Resource = "*"
      }
    ]
  })

  tags = {
    Name    = "${var.JobName}-source-glue-vpc-endpoint"
    Purpose = "Glue API access for Glue job"
    JobName = var.JobName
  }
}

resource "aws_vpc_endpoint" "target_glue" {
  count = local.create_target_glue_endpoint ? 1 : 0

  vpc_id            = var.TargetVpcId
  service_name      = "com.amazonaws.${data.aws_region.current.id}.glue"
  vpc_endpoint_type = "Interface"
  subnet_ids        = split(",", var.TargetSubnetIds)
  security_group_ids = split(",", var.TargetSecurityGroupIds)

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = "*"
        Action = [
          "glue:GetConnection",
          "glue:GetConnections",
          "glue:GetJob",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:GetJobBookmark",
          "glue:UpdateJobBookmark",
          "glue:ResetJobBookmark"
        ]
        Resource = "*"
      }
    ]
  })

  tags = {
    Name    = "${var.JobName}-target-glue-vpc-endpoint"
    Purpose = "Glue API access for Glue job"
    JobName = var.JobName
  }
}


# =============================================================================
# Glue Job Resource
# =============================================================================

resource "aws_glue_job" "this" {
  name        = var.JobName
  description = "Data replication job from ${var.SourceEngineType} to ${var.TargetEngineType}"
  role_arn    = aws_iam_role.glue_job.arn

  command {
    name            = "glueetl"
    script_location = var.GlueJobScriptS3Path
    python_version  = "3"
  }

  default_arguments = merge(
    {
      "--job-bookmark-option"              = "job-bookmark-enable"
      "--enable-metrics"                   = "true"
      "--enable-observability-metrics"     = "true"
      "--enable-continuous-cloudwatch-log" = "true"
      "--enable-spark-ui"                  = "true"
      "--spark-event-logs-path"            = "s3://aws-glue-assets-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.id}/sparkHistoryLogs/"
      "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_job.name
      "--continuous-log-logStreamPrefix"   = var.JobName
      "--continuous-log-conversionPattern" = "%d{yyyy-MM-dd HH:mm:ss} [%t] %-5p %c{1} - %m%n"
      "--enable-auto-scaling"              = "true"
      "--enable-glue-datacatalog"          = "true"
      "--job-language"                     = "python"
      "--JOB_NAME"                         = var.JobName
      "--SOURCE_ENGINE_TYPE"               = var.SourceEngineType
      "--TARGET_ENGINE_TYPE"               = var.TargetEngineType
      "--SOURCE_DATABASE"                  = var.SourceDatabase
      "--TARGET_DATABASE"                  = var.TargetDatabase
      "--SOURCE_SCHEMA"                    = var.SourceSchema
      "--TARGET_SCHEMA"                    = var.TargetSchema
      "--TABLE_NAMES"                      = var.TableNames
      "--SOURCE_DB_USER"                   = var.SourceDbUser
      "--SOURCE_DB_PASSWORD"               = local.source_needs_password ? var.SourceDbPassword : ""
      "--TARGET_DB_USER"                   = var.TargetDbUser
      "--TARGET_DB_PASSWORD"               = local.target_needs_password ? var.TargetDbPassword : ""
      "--SOURCE_CONNECTION_STRING"         = var.SourceConnectionString
      "--TARGET_CONNECTION_STRING"         = var.TargetConnectionString
      "--SOURCE_JDBC_DRIVER_S3_PATH"       = var.SourceJdbcDriverS3Path
      "--TARGET_JDBC_DRIVER_S3_PATH"       = var.TargetJdbcDriverS3Path
      "--SOURCE_WAREHOUSE_LOCATION"        = var.SourceWarehouseLocation
      "--TARGET_WAREHOUSE_LOCATION"        = var.TargetWarehouseLocation
      "--SOURCE_CATALOG_ID"                = var.SourceCatalogId
      "--TARGET_CATALOG_ID"                = var.TargetCatalogId
      "--SOURCE_FORMAT_VERSION"            = var.SourceFormatVersion
      "--TARGET_FORMAT_VERSION"            = var.TargetFormatVersion
      "--SOURCE_VPC_ID"                    = var.SourceVpcId
      "--SOURCE_SUBNET_IDS"                = var.SourceSubnetIds
      "--SOURCE_SECURITY_GROUP_IDS"        = var.SourceSecurityGroupIds
      "--CREATE_SOURCE_S3_VPC_ENDPOINT"    = var.CreateSourceS3VpcEndpoint
      "--TARGET_VPC_ID"                    = var.TargetVpcId
      "--TARGET_SUBNET_IDS"                = var.TargetSubnetIds
      "--TARGET_SECURITY_GROUP_IDS"        = var.TargetSecurityGroupIds
      "--CREATE_TARGET_S3_VPC_ENDPOINT"    = var.CreateTargetS3VpcEndpoint
      "--SOURCE_GLUE_CONNECTION_NAME"      = local.has_source_network_config ? "${var.JobName}-source-network-connection" : ""
      "--TARGET_GLUE_CONNECTION_NAME"      = local.has_target_network_config ? "${var.JobName}-target-network-connection" : ""
      "--SOURCE_S3_VPC_ENDPOINT_ID"        = local.create_source_s3_endpoint ? aws_vpc_endpoint.source_s3[0].id : ""
      "--TARGET_S3_VPC_ENDPOINT_ID"        = local.create_target_s3_endpoint ? aws_vpc_endpoint.target_s3[0].id : ""
      "--CREATE_SOURCE_CONNECTION"         = var.CreateSourceConnection
      "--CREATE_TARGET_CONNECTION"         = var.CreateTargetConnection
      "--USE_SOURCE_CONNECTION"            = var.UseSourceConnection
      "--USE_TARGET_CONNECTION"            = var.UseTargetConnection
      "--SOURCE_JDBC_CONNECTION_NAME"      = local.should_create_source_conn ? "${var.JobName}-source-jdbc-connection" : ""
      "--TARGET_JDBC_CONNECTION_NAME"      = local.should_create_target_conn ? "${var.JobName}-target-jdbc-connection" : ""
      "--SOURCE_DATABASE_SECRET_ARN"       = local.should_create_source_conn ? aws_secretsmanager_secret.source_database[0].arn : ""
      "--TARGET_DATABASE_SECRET_ARN"       = local.should_create_target_conn ? aws_secretsmanager_secret.target_database[0].arn : ""
      "--VALIDATE_CONNECTIONS"             = "true"
      "--CONNECTION_TIMEOUT_SECONDS"       = "30"
      "--MANUAL_BOOKMARK_CONFIG"           = var.ManualBookmarkConfig
      "--BOOKMARK_S3_BUCKET"               = local.script_bucket_name
      "--COUNTING_STRATEGY"                = var.CountingStrategy
      "--PROGRESS_UPDATE_INTERVAL"         = tostring(var.ProgressUpdateInterval)
      "--BATCH_SIZE_THRESHOLD"             = tostring(var.BatchSizeThreshold)
      "--ENABLE_DETAILED_METRICS"          = var.EnableDetailedMetrics
      "--ENABLE_PARTITIONED_READS"         = var.EnablePartitionedReads
      "--PARTITIONED_READ_CONFIG"          = var.PartitionedReadConfig
      "--DEFAULT_NUM_PARTITIONS"           = tostring(var.DefaultNumPartitions)
      "--DEFAULT_FETCH_SIZE"               = tostring(var.DefaultFetchSize)
      "--SOURCE_KERBEROS_SPN"              = var.SourceKerberosSPN
      "--SOURCE_KERBEROS_DOMAIN"           = var.SourceKerberosDomain
      "--SOURCE_KERBEROS_KDC"              = var.SourceKerberosKDC
      "--SOURCE_KERBEROS_KEYTAB_S3_PATH"   = var.SourceKerberosKeytabS3Path
      "--TARGET_KERBEROS_SPN"              = var.TargetKerberosSPN
      "--TARGET_KERBEROS_DOMAIN"           = var.TargetKerberosDomain
      "--TARGET_KERBEROS_KDC"              = var.TargetKerberosKDC
      "--TARGET_KERBEROS_KEYTAB_S3_PATH"   = var.TargetKerberosKeytabS3Path
    },
    # Conditional: --datalake-formats for Iceberg jobs
    local.is_iceberg_job ? {
      "--datalake-formats" = "iceberg"
    } : {},
    # Conditional: --conf for Spark configuration
    {
      "--conf" = local.is_iceberg_job ? "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=false --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO" : "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=false"
    },
    # Conditional: --java-options for Kerberos
    local.has_source_kerberos ? {
      "--java-options" = "-Djava.security.krb5.conf=/tmp/krb5.conf -Djava.security.auth.login.config=/tmp/jaas.conf -Djavax.security.auth.useSubjectCredsOnly=false"
    } : {},
    # Conditional: --extra-jars for JDBC drivers
    local.has_source_jdbc_driver && local.has_target_jdbc_driver ? {
      "--extra-jars" = "${var.SourceJdbcDriverS3Path},${var.TargetJdbcDriverS3Path}"
    } : local.has_source_jdbc_driver ? {
      "--extra-jars" = var.SourceJdbcDriverS3Path
    } : local.has_target_jdbc_driver ? {
      "--extra-jars" = var.TargetJdbcDriverS3Path
    } : {},
    # Conditional: --extra-py-files for Glue modules
    local.has_glue_job_modules ? {
      "--extra-py-files" = var.GlueJobModulesS3Path
    } : {},
  )

  execution_property {
    max_concurrent_runs = var.MaxConcurrentRuns
  }

  max_retries  = var.MaxRetries
  timeout      = var.Timeout
  glue_version = "4.0"

  # Connections list — compact removes empty strings
  connections = length(local.connection_list) > 0 ? local.connection_list : null

  # Worker configuration: Standard uses MaxCapacity, others use WorkerType + NumberOfWorkers
  worker_type       = local.is_standard_worker ? null : var.WorkerType
  number_of_workers = local.is_standard_worker ? null : var.NumberOfWorkers
  max_capacity      = local.is_standard_worker ? 10 : null

  tags = {
    Environment  = var.JobName
    JobType      = "DataReplication"
    SourceEngine = var.SourceEngineType
    TargetEngine = var.TargetEngineType
  }

  depends_on = [
    aws_cloudwatch_log_group.glue_job,
    aws_cloudwatch_log_group.glue_job_output,
    aws_cloudwatch_log_group.glue_job_error,
  ]
}


# =============================================================================
# SNS Topic for Alarm Notifications
# =============================================================================

resource "aws_sns_topic" "alarm_notifications" {
  count = local.create_alarm_topic ? 1 : 0

  name         = "${var.JobName}-glue-alarms"
  display_name = "Glue Job Alarms - ${var.JobName}"

  tags = {
    Name    = "${var.JobName}-glue-alarms"
    Purpose = "Alarm notifications for Glue data replication job"
    JobName = var.JobName
  }
}

resource "aws_sns_topic_subscription" "alarm_email" {
  count = local.create_alarm_topic ? 1 : 0

  topic_arn = aws_sns_topic.alarm_notifications[0].arn
  protocol  = "email"
  endpoint  = var.AlarmNotificationEmail
}

# =============================================================================
# CloudWatch Dashboard
# =============================================================================

resource "aws_cloudwatch_dashboard" "glue_job" {
  count = local.enable_dashboard ? 1 : 0

  dashboard_name = "${var.JobName}-glue-monitoring"
  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          metrics = [
            [{ expression = "SEARCH('{AWS/Glue/DataReplication,JobName,TableName,LoadType} MetricName=\"MigrationRowsProcessed\" JobName=\"${var.JobName}\"', 'Sum', 300)", id = "m1", label = "Migration Rows" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = data.aws_region.current.id
          title   = "Migration - Rows Processed"
          period  = 300
          yAxis = {
            left = {
              label = "Rows"
            }
          }
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          metrics = [
            [{ expression = "SEARCH('{AWS/Glue/DataReplication,JobName,TableName,LoadType} MetricName=\"MigrationRowsPerSecond\" JobName=\"${var.JobName}\"', 'Average', 300)", id = "m1", label = "Rows/Second" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = data.aws_region.current.id
          title   = "Migration - Throughput (Rows/Second)"
          period  = 300
          yAxis = {
            left = {
              label = "Rows/Second"
            }
          }
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 8
        height = 6
        properties = {
          metrics = [
            [{ expression = "SEARCH('{AWS/Glue/DataReplication,JobName,TableName,LoadType} MetricName=\"MigrationSuccess\" JobName=\"${var.JobName}\"', 'Sum', 300)", id = "m1", label = "Success", color = "#2ca02c" }],
            [{ expression = "SEARCH('{AWS/Glue/DataReplication,JobName,TableName,LoadType} MetricName=\"MigrationFailed\" JobName=\"${var.JobName}\"', 'Sum', 300)", id = "m2", label = "Failed", color = "#d62728" }],
            [{ expression = "SEARCH('{AWS/Glue/DataReplication,JobName,TableName,LoadType} MetricName=\"MigrationInProgress\" JobName=\"${var.JobName}\"', 'Sum', 300)", id = "m3", label = "In Progress", color = "#ff7f0e" }]
          ]
          view    = "timeSeries"
          stacked = true
          region  = data.aws_region.current.id
          title   = "Migration - Status by Table"
          period  = 300
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 6
        width  = 8
        height = 6
        properties = {
          metrics = [
            [{ expression = "SEARCH('{AWS/Glue/DataReplication,JobName,TableName,LoadType} MetricName=\"MigrationProgressPercentage\" JobName=\"${var.JobName}\"', 'Average', 60)", id = "m1", label = "Progress %" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = data.aws_region.current.id
          title   = "Migration - Progress Percentage"
          period  = 60
          yAxis = {
            left = {
              min   = 0
              max   = 100
              label = "Percent"
            }
          }
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 6
        width  = 8
        height = 6
        properties = {
          metrics = [
            [{ expression = "SEARCH('{AWS/Glue/DataReplication,JobName,TableName,LoadType,Phase} MetricName=\"MigrationReadDuration\" JobName=\"${var.JobName}\"', 'Average', 300)", id = "m1", label = "Read Duration" }],
            [{ expression = "SEARCH('{AWS/Glue/DataReplication,JobName,TableName,LoadType,Phase} MetricName=\"MigrationWriteDuration\" JobName=\"${var.JobName}\"', 'Average', 300)", id = "m2", label = "Write Duration" }],
            [{ expression = "SEARCH('{AWS/Glue/DataReplication,JobName,TableName,LoadType,Phase} MetricName=\"MigrationCountDuration\" JobName=\"${var.JobName}\"', 'Average', 300)", id = "m3", label = "Count Duration" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = data.aws_region.current.id
          title   = "Migration - Phase Durations"
          period  = 300
          yAxis = {
            left = {
              label = "Seconds"
            }
          }
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 12
        width  = 12
        height = 6
        properties = {
          metrics = [
            [{ expression = "SUM(SEARCH('{AWS/Glue/DataReplication,JobName,TableName,LoadType} MetricName=\"MigrationSuccess\" JobName=\"${var.JobName}\"', 'Sum', 86400))", id = "success", label = "Tables Succeeded", color = "#2ca02c" }],
            [{ expression = "SUM(SEARCH('{AWS/Glue/DataReplication,JobName,TableName,LoadType} MetricName=\"MigrationFailed\" JobName=\"${var.JobName}\"', 'Sum', 86400))", id = "failed", label = "Tables Failed", color = "#d62728" }]
          ]
          view    = "singleValue"
          stacked = false
          region  = data.aws_region.current.id
          title   = "Tables Migrated"
          period  = 86400
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 12
        width  = 12
        height = 6
        properties = {
          metrics = [
            [{ expression = "SEARCH('{AWS/Glue/DataReplication,JobName} MetricName=\"ErrorOccurred\" JobName=\"${var.JobName}\"', 'Sum', 300)", id = "m1", label = "Errors" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = data.aws_region.current.id
          title   = "Error Count"
          period  = 300
          yAxis = {
            left = {
              min = 0
            }
          }
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 18
        width  = 12
        height = 6
        properties = {
          metrics = [
            [{ expression = "SEARCH('{Glue,JobName,JobRunId,Type} MetricName=\"glue.driver.aggregate.numCompletedTasks\" JobName=\"${var.JobName}\"', 'Sum', 300)", id = "m1" }],
            [{ expression = "SEARCH('{Glue,JobName,JobRunId,Type} MetricName=\"glue.driver.aggregate.numFailedTasks\" JobName=\"${var.JobName}\"', 'Sum', 300)", id = "m2" }],
            [{ expression = "SEARCH('{Glue,JobName,JobRunId,Type} MetricName=\"glue.driver.aggregate.numKilledTasks\" JobName=\"${var.JobName}\"', 'Sum', 300)", id = "m3" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = data.aws_region.current.id
          title   = "Glue Job Task Metrics"
          period  = 300
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 18
        width  = 12
        height = 6
        properties = {
          metrics = [
            [{ expression = "SEARCH('{Glue,ObservabilityGroup,JobRunId,JobName,Type} MetricName=\"glue.driver.memory.non-heap.used\" JobName=\"${var.JobName}\"', 'Average', 300)", id = "m1" }],
            [{ expression = "SEARCH('{Glue,ObservabilityGroup,JobRunId,JobName,Type} MetricName=\"glue.ALL.memory.non-heap.used\" JobName=\"${var.JobName}\"', 'Average', 300)", id = "m2" }],
            [{ expression = "SEARCH('{Glue,ObservabilityGroup,JobRunId,JobName,Type} MetricName=\"glue.driver.jvm.heap.used\" JobName=\"${var.JobName}\"', 'Average', 300)", id = "m3" }],
            [{ expression = "SEARCH('{Glue,ObservabilityGroup,JobRunId,JobName,Type} MetricName=\"glue.ALL.jvm.heap.used\" JobName=\"${var.JobName}\"', 'Average', 300)", id = "m4" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = data.aws_region.current.id
          title   = "Glue Job Resource Utilization"
          period  = 300
        }
      },
      {
        type   = "log"
        x      = 0
        y      = 24
        width  = 24
        height = 6
        properties = {
          query  = "SOURCE '/aws-glue/jobs/${var.JobName}' | SOURCE '/aws-glue/jobs/error/${var.JobName}'\n| fields @timestamp, @message\n| filter (@message like /ERROR/ or @message like /Migration completed/) and @message not like /getResourceAsStream/ and @message not like /FailureAnalysisRules/ and @message not like /AnalyzerLogHelper/\n| sort @timestamp desc\n| limit 100"
          region = data.aws_region.current.id
          title  = "Recent Errors and Completions"
          view   = "table"
        }
      }
    ]
  })
}


# =============================================================================
# CloudWatch Alarms
# =============================================================================

# Alarm for Job Failures
resource "aws_cloudwatch_metric_alarm" "job_failure" {
  count = local.enable_alarms ? 1 : 0

  alarm_name          = "${var.JobName}-job-failures"
  alarm_description   = "Alarm when Glue job ${var.JobName} fails"
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "AWS/Glue"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    JobName = var.JobName
  }

  alarm_actions = local.create_alarm_topic ? [aws_sns_topic.alarm_notifications[0].arn] : []
  ok_actions    = local.create_alarm_topic ? [aws_sns_topic.alarm_notifications[0].arn] : []

  tags = {
    Name    = "${var.JobName}-failure-alarm"
    Purpose = "Monitor Glue job failures"
    JobName = var.JobName
  }
}

# Alarm for High Memory Usage
resource "aws_cloudwatch_metric_alarm" "high_memory" {
  count = local.enable_alarms ? 1 : 0

  alarm_name          = "${var.JobName}-high-memory-usage"
  alarm_description   = "Alarm when Glue job ${var.JobName} has high memory usage"
  metric_name         = "glue.driver.jvm.heap.usage"
  namespace           = "AWS/Glue"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 2
  threshold           = 0.85
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    JobName = var.JobName
  }

  alarm_actions = local.create_alarm_topic ? [aws_sns_topic.alarm_notifications[0].arn] : []

  tags = {
    Name    = "${var.JobName}-memory-alarm"
    Purpose = "Monitor Glue job memory usage"
    JobName = var.JobName
  }
}

# Alarm for Long Running Jobs
resource "aws_cloudwatch_metric_alarm" "long_duration" {
  count = local.enable_alarms ? 1 : 0

  alarm_name          = "${var.JobName}-long-duration"
  alarm_description   = "Alarm when Glue job ${var.JobName} runs longer than expected"
  metric_name         = "glue.driver.aggregate.elapsedTime"
  namespace           = "AWS/Glue"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = var.JobDurationThresholdMinutes * 60000 # Convert minutes to milliseconds
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    JobName = var.JobName
  }

  alarm_actions = local.create_alarm_topic ? [aws_sns_topic.alarm_notifications[0].arn] : []

  tags = {
    Name    = "${var.JobName}-duration-alarm"
    Purpose = "Monitor Glue job execution duration"
    JobName = var.JobName
  }
}


# =============================================================================
# CloudWatch Log Insights Saved Queries
# =============================================================================

resource "aws_cloudwatch_query_definition" "error_analysis" {
  name = "${var.JobName}-error-analysis"

  log_group_names = [
    aws_cloudwatch_log_group.glue_job.name,
    aws_cloudwatch_log_group.glue_job_error.name,
  ]

  query_string = <<-EOT
    fields @timestamp, @message, @logStream
    | filter @message like /ERROR/ or @message like /Exception/ or @message like /Failed/
    | sort @timestamp desc
    | limit 100
  EOT
}

resource "aws_cloudwatch_query_definition" "performance_analysis" {
  name = "${var.JobName}-performance-analysis"

  log_group_names = [
    aws_cloudwatch_log_group.glue_job.name,
    aws_cloudwatch_log_group.glue_job_output.name,
  ]

  query_string = <<-EOT
    fields @timestamp, @message
    | filter @message like /records processed/ or @message like /execution time/ or @message like /memory usage/
    | sort @timestamp desc
    | limit 50
  EOT
}

resource "aws_cloudwatch_query_definition" "data_flow_analysis" {
  name = "${var.JobName}-data-flow-analysis"

  log_group_names = [
    aws_cloudwatch_log_group.glue_job_output.name,
  ]

  query_string = <<-EOT
    fields @timestamp, @message
    | filter @message like /table/ or @message like /rows/ or @message like /processed/
    | stats count() by bin(5m)
    | sort @timestamp desc
  EOT
}