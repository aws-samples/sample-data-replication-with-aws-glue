# =============================================================================
# Terraform and Provider Configuration
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

provider "aws" {
  # Region and credentials configured via environment or shared credentials
}

# =============================================================================
# Core Job Parameters
# =============================================================================

variable "JobName" {
  type        = string
  description = "Unique name for the Glue data replication job instance"
  validation {
    condition     = can(regex("^[a-zA-Z0-9_-]+$", var.JobName)) && length(var.JobName) >= 1 && length(var.JobName) <= 255
    error_message = "Job name must contain only alphanumeric characters, hyphens, and underscores (1-255 chars)"
  }
}

variable "SourceEngineType" {
  type        = string
  description = "Source database engine type"
  validation {
    condition     = contains(["oracle", "sqlserver", "postgresql", "db2", "iceberg"], var.SourceEngineType)
    error_message = "Must be one of: oracle, sqlserver, postgresql, db2, iceberg"
  }
}

variable "TargetEngineType" {
  type        = string
  description = "Target database engine type"
  validation {
    condition     = contains(["oracle", "sqlserver", "postgresql", "db2", "iceberg"], var.TargetEngineType)
    error_message = "Must be one of: oracle, sqlserver, postgresql, db2, iceberg"
  }
}

variable "SourceDatabase" {
  type        = string
  description = "Source database name"
  validation {
    condition     = can(regex("^[a-zA-Z0-9_.-]+$", var.SourceDatabase)) && length(var.SourceDatabase) >= 1 && length(var.SourceDatabase) <= 128
    error_message = "Database name must contain only alphanumeric characters, hyphens, underscores, and periods (1-128 chars)"
  }
}

variable "TargetDatabase" {
  type        = string
  description = "Target database name"
  validation {
    condition     = can(regex("^[a-zA-Z0-9_.-]+$", var.TargetDatabase)) && length(var.TargetDatabase) >= 1 && length(var.TargetDatabase) <= 128
    error_message = "Database name must contain only alphanumeric characters, hyphens, underscores, and periods (1-128 chars)"
  }
}

variable "SourceSchema" {
  type        = string
  description = "Source database schema name (for Iceberg engines, this represents the table name; not required for Iceberg engines)"
  default     = ""
  validation {
    condition     = can(regex("^[a-zA-Z0-9_.-]*$", var.SourceSchema)) && length(var.SourceSchema) <= 128
    error_message = "Schema name must contain only alphanumeric characters, hyphens, underscores, and periods (0-128 chars)"
  }
}

variable "TargetSchema" {
  type        = string
  description = "Target database schema name (for Iceberg engines, this represents the table name; not required for Iceberg engines)"
  default     = ""
  validation {
    condition     = can(regex("^[a-zA-Z0-9_.-]*$", var.TargetSchema)) && length(var.TargetSchema) <= 128
    error_message = "Schema name must contain only alphanumeric characters, hyphens, underscores, and periods (0-128 chars)"
  }
}

variable "TableNames" {
  type        = string
  description = "Comma-separated list of table names to replicate (e.g., table1,table2,table3)"
}

# =============================================================================
# Database Credential Parameters
# =============================================================================

variable "SourceDbUser" {
  type        = string
  description = "Source database username (not required for Iceberg engines)"
  default     = ""
  validation {
    condition     = length(var.SourceDbUser) <= 128
    error_message = "Username must be between 0 and 128 characters"
  }
}

variable "SourceDbPassword" {
  type        = string
  description = "Source database password (not required for Iceberg engines)"
  default     = ""
  sensitive   = true
  validation {
    condition     = length(var.SourceDbPassword) <= 128
    error_message = "Password must be between 0 and 128 characters"
  }
}

variable "TargetDbUser" {
  type        = string
  description = "Target database username (not required for Iceberg engines)"
  default     = ""
  validation {
    condition     = length(var.TargetDbUser) <= 128
    error_message = "Username must be between 0 and 128 characters"
  }
}

variable "TargetDbPassword" {
  type        = string
  description = "Target database password (not required for Iceberg engines)"
  default     = ""
  sensitive   = true
  validation {
    condition     = length(var.TargetDbPassword) <= 128
    error_message = "Password must be between 0 and 128 characters"
  }
}

# =============================================================================
# JDBC Connection Parameters
# =============================================================================

variable "SourceConnectionString" {
  type        = string
  description = "JDBC connection string for source database (not required for Iceberg engines)"
  default     = ""
  validation {
    condition     = can(regex("^(jdbc:[a-zA-Z0-9]+(:[a-zA-Z0-9]+)*:(//|@//|@).+)?$", var.SourceConnectionString)) && length(var.SourceConnectionString) <= 512
    error_message = "Must be empty or a valid JDBC connection string (jdbc:engine://host:port, jdbc:oracle:thin:@//host:port, or jdbc:oracle:thin:@host:port:SID)"
  }
}

variable "TargetConnectionString" {
  type        = string
  description = "JDBC connection string for target database (not required for Iceberg engines)"
  default     = ""
  validation {
    condition     = can(regex("^(jdbc:[a-zA-Z0-9]+(:[a-zA-Z0-9]+)*:(//|@//|@).+)?$", var.TargetConnectionString)) && length(var.TargetConnectionString) <= 512
    error_message = "Must be empty or a valid JDBC connection string (jdbc:engine://host:port, jdbc:oracle:thin:@//host:port, or jdbc:oracle:thin:@host:port:SID)"
  }
}

variable "SourceJdbcDriverS3Path" {
  type        = string
  description = "S3 path to source database JDBC driver JAR file (not required for Iceberg engines)"
  default     = ""
  validation {
    condition     = can(regex("^(s3://[a-zA-Z0-9.-]+/.*\\.jar)?$", var.SourceJdbcDriverS3Path)) && length(var.SourceJdbcDriverS3Path) <= 512
    error_message = "Must be empty or a valid S3 path ending with .jar (s3://bucket/path/driver.jar)"
  }
}

variable "TargetJdbcDriverS3Path" {
  type        = string
  description = "S3 path to target database JDBC driver JAR file (not required for Iceberg engines)"
  default     = ""
  validation {
    condition     = can(regex("^(s3://[a-zA-Z0-9.-]+/.*\\.jar)?$", var.TargetJdbcDriverS3Path)) && length(var.TargetJdbcDriverS3Path) <= 512
    error_message = "Must be empty or a valid S3 path ending with .jar (s3://bucket/path/driver.jar)"
  }
}

# =============================================================================
# Kerberos Authentication Parameters (Optional)
# =============================================================================

variable "SourceKerberosSPN" {
  type        = string
  description = "Service Principal Name (SPN) for source database Kerberos authentication. Required for Kerberos authentication along with SourceKerberosDomain and SourceKerberosKDC. Supports Oracle, SQL Server, PostgreSQL, and DB2 engines only."
  default     = ""
  validation {
    condition     = can(regex("^([a-zA-Z0-9._/:-]+(@[a-zA-Z0-9.-]+)?)?$", var.SourceKerberosSPN)) && length(var.SourceKerberosSPN) <= 256
    error_message = "Must be empty or a valid SPN format (service/hostname, service/hostname:port, service/hostname@REALM, service/hostname:port@REALM, or service@REALM)"
  }
}

variable "SourceKerberosDomain" {
  type        = string
  description = "Kerberos domain/realm for source database authentication. Required for Kerberos authentication along with SourceKerberosSPN and SourceKerberosKDC. Must be a valid Kerberos realm name."
  default     = ""
  validation {
    condition     = can(regex("^([A-Z0-9.-]+)?$", var.SourceKerberosDomain)) && length(var.SourceKerberosDomain) <= 128
    error_message = "Must be empty or a valid Kerberos realm name (uppercase letters, numbers, dots, and hyphens)"
  }
}

variable "SourceKerberosKDC" {
  type        = string
  description = "Key Distribution Center (KDC) hostname or IP address for source database Kerberos authentication. Required for Kerberos authentication along with SourceKerberosSPN and SourceKerberosDomain. Can include optional port number."
  default     = ""
  validation {
    condition     = can(regex("^([a-zA-Z0-9.-]+(:[0-9]+)?)?$", var.SourceKerberosKDC)) && length(var.SourceKerberosKDC) <= 256
    error_message = "Must be empty or a valid hostname/IP address with optional port (hostname:port or ip:port)"
  }
}

variable "TargetKerberosSPN" {
  type        = string
  description = "Service Principal Name (SPN) for target database Kerberos authentication. Required for Kerberos authentication along with TargetKerberosDomain and TargetKerberosKDC. Supports Oracle, SQL Server, PostgreSQL, and DB2 engines only."
  default     = ""
  validation {
    condition     = can(regex("^([a-zA-Z0-9._/:-]+(@[a-zA-Z0-9.-]+)?)?$", var.TargetKerberosSPN)) && length(var.TargetKerberosSPN) <= 256
    error_message = "Must be empty or a valid SPN format (service/hostname, service/hostname:port, service/hostname@REALM, service/hostname:port@REALM, or service@REALM)"
  }
}

variable "TargetKerberosDomain" {
  type        = string
  description = "Kerberos domain/realm for target database authentication. Required for Kerberos authentication along with TargetKerberosSPN and TargetKerberosKDC. Must be a valid Kerberos realm name."
  default     = ""
  validation {
    condition     = can(regex("^([A-Z0-9.-]+)?$", var.TargetKerberosDomain)) && length(var.TargetKerberosDomain) <= 128
    error_message = "Must be empty or a valid Kerberos realm name (uppercase letters, numbers, dots, and hyphens)"
  }
}

variable "TargetKerberosKDC" {
  type        = string
  description = "Key Distribution Center (KDC) hostname or IP address for target database Kerberos authentication. Required for Kerberos authentication along with TargetKerberosSPN and TargetKerberosDomain. Can include optional port number."
  default     = ""
  validation {
    condition     = can(regex("^([a-zA-Z0-9.-]+(:[0-9]+)?)?$", var.TargetKerberosKDC)) && length(var.TargetKerberosKDC) <= 256
    error_message = "Must be empty or a valid hostname/IP address with optional port (hostname:port or ip:port)"
  }
}

# =============================================================================
# Kerberos Keytab Parameters (Optional)
# =============================================================================

variable "SourceKerberosKeytabS3Path" {
  type        = string
  description = "S3 path to keytab file for source database Kerberos authentication. Optional alternative to username/password authentication. If provided, takes precedence over username/password credentials."
  default     = ""
  validation {
    condition     = can(regex("^(s3://[a-zA-Z0-9.-]+/.*\\.keytab)?$", var.SourceKerberosKeytabS3Path)) && length(var.SourceKerberosKeytabS3Path) <= 512
    error_message = "Must be empty or a valid S3 path ending with .keytab (s3://bucket/path/user.keytab)"
  }
}

variable "TargetKerberosKeytabS3Path" {
  type        = string
  description = "S3 path to keytab file for target database Kerberos authentication. Optional alternative to username/password authentication. If provided, takes precedence over username/password credentials."
  default     = ""
  validation {
    condition     = can(regex("^(s3://[a-zA-Z0-9.-]+/.*\\.keytab)?$", var.TargetKerberosKeytabS3Path)) && length(var.TargetKerberosKeytabS3Path) <= 512
    error_message = "Must be empty or a valid S3 path ending with .keytab (s3://bucket/path/user.keytab)"
  }
}

# =============================================================================
# Iceberg-Specific Parameters (Required when using Iceberg engines)
# =============================================================================

variable "SourceWarehouseLocation" {
  type        = string
  description = "S3 warehouse location for source Iceberg tables (required when SourceEngineType is iceberg)"
  default     = ""
  validation {
    condition     = can(regex("^(s3://[a-zA-Z0-9.-]+/.*/?)?$", var.SourceWarehouseLocation)) && length(var.SourceWarehouseLocation) <= 512
    error_message = "Must be empty or a valid S3 path (s3://bucket/path/)"
  }
}

variable "TargetWarehouseLocation" {
  type        = string
  description = "S3 warehouse location for target Iceberg tables (required when TargetEngineType is iceberg)"
  default     = ""
  validation {
    condition     = can(regex("^(s3://[a-zA-Z0-9.-]+/.*/?)?$", var.TargetWarehouseLocation)) && length(var.TargetWarehouseLocation) <= 512
    error_message = "Must be empty or a valid S3 path (s3://bucket/path/)"
  }
}

variable "SourceCatalogId" {
  type        = string
  description = "AWS Glue Data Catalog ID for source Iceberg tables (optional, defaults to current account)"
  default     = ""
  validation {
    condition     = can(regex("^([0-9]{12})?$", var.SourceCatalogId)) && length(var.SourceCatalogId) <= 12
    error_message = "Must be empty or a 12-digit AWS account ID"
  }
}

variable "TargetCatalogId" {
  type        = string
  description = "AWS Glue Data Catalog ID for target Iceberg tables (optional, defaults to current account)"
  default     = ""
  validation {
    condition     = can(regex("^([0-9]{12})?$", var.TargetCatalogId)) && length(var.TargetCatalogId) <= 12
    error_message = "Must be empty or a 12-digit AWS account ID"
  }
}

variable "SourceFormatVersion" {
  type        = string
  description = "Iceberg format version for source tables (optional, defaults to 2)"
  default     = "2"
  validation {
    condition     = contains(["1", "2"], var.SourceFormatVersion)
    error_message = "Must be 1 or 2"
  }
}

variable "TargetFormatVersion" {
  type        = string
  description = "Iceberg format version for target tables (optional, defaults to 2)"
  default     = "2"
  validation {
    condition     = contains(["1", "2"], var.TargetFormatVersion)
    error_message = "Must be 1 or 2"
  }
}

# =============================================================================
# Glue Job Configuration Parameters
# =============================================================================

variable "GlueJobScriptS3Path" {
  type        = string
  description = "S3 path to the PySpark Glue job main script (s3://bucket/path/main.py)"
  validation {
    condition     = can(regex("^s3://[a-zA-Z0-9.-]+/.*\\.py$", var.GlueJobScriptS3Path)) && length(var.GlueJobScriptS3Path) >= 10 && length(var.GlueJobScriptS3Path) <= 512
    error_message = "Must be a valid S3 path ending with .py (s3://bucket/path/main.py)"
  }
}

variable "GlueJobModulesS3Path" {
  type        = string
  description = "S3 path to the Python modules zip file for --extra-py-files (s3://bucket/path/modules.zip)"
  default     = ""
  validation {
    condition     = can(regex("^(s3://[a-zA-Z0-9.-]+/.*\\.zip)?$", var.GlueJobModulesS3Path)) && length(var.GlueJobModulesS3Path) <= 512
    error_message = "Must be empty or a valid S3 path ending with .zip (s3://bucket/path/modules.zip)"
  }
}

variable "ManualBookmarkConfig" {
  type        = string
  description = "JSON string with manual bookmark configurations per table (optional)"
  default     = ""
  validation {
    condition     = length(var.ManualBookmarkConfig) <= 4096
    error_message = "Must be empty or a valid JSON string with table-to-column mappings (max 4096 chars)"
  }
}

variable "MaxRetries" {
  type        = number
  description = "Maximum number of retries for the Glue job"
  default     = 1
  validation {
    condition     = var.MaxRetries >= 0 && var.MaxRetries <= 10
    error_message = "Must be between 0 and 10"
  }
}

variable "Timeout" {
  type        = number
  description = "Timeout for the Glue job in minutes"
  default     = 2880
  validation {
    condition     = var.Timeout >= 1 && var.Timeout <= 2880
    error_message = "Must be between 1 and 2880 minutes (48 hours)"
  }
}

variable "MaxConcurrentRuns" {
  type        = number
  description = "Maximum number of concurrent runs for the Glue job"
  default     = 1
  validation {
    condition     = var.MaxConcurrentRuns >= 1 && var.MaxConcurrentRuns <= 1000
    error_message = "Must be between 1 and 1000"
  }
}

variable "WorkerType" {
  type        = string
  description = "Type of predefined worker for the Glue job"
  default     = "G.1X"
  validation {
    condition     = contains(["Standard", "G.1X", "G.2X", "G.025X"], var.WorkerType)
    error_message = "Must be one of: Standard, G.1X, G.2X, G.025X"
  }
}

variable "NumberOfWorkers" {
  type        = number
  description = "Number of workers for the Glue job"
  default     = 2
  validation {
    condition     = var.NumberOfWorkers >= 2 && var.NumberOfWorkers <= 299
    error_message = "Must be between 2 and 299"
  }
}

# =============================================================================
# Source Network Configuration Parameters (Optional)
# =============================================================================

variable "SourceVpcId" {
  type        = string
  description = "VPC ID where source database resides (optional for cross-VPC access)"
  default     = ""
  validation {
    condition     = can(regex("^(vpc-[a-zA-Z0-9]{8,17})?$", var.SourceVpcId))
    error_message = "Must be a valid VPC ID (vpc-xxxxxxxx) or empty for same-VPC access"
  }
}

variable "SourceSubnetIds" {
  type        = string
  description = "Comma-separated list of subnet IDs for source database access (required if SourceVpcId is specified)"
  default     = ""
}

variable "SourceSecurityGroupIds" {
  type        = string
  description = "Comma-separated list of security group IDs for source database access (required if SourceVpcId is specified)"
  default     = ""
}

variable "SourceAvailabilityZone" {
  type        = string
  description = "Availability Zone for source subnet (required if SourceVpcId is specified)"
  default     = ""
  validation {
    condition     = can(regex("^([a-z]{2}-[a-z]+-[0-9][a-z])?$", var.SourceAvailabilityZone))
    error_message = "Must be a valid availability zone (e.g., us-east-1a) or empty"
  }
}

variable "CreateSourceS3VpcEndpoint" {
  type        = string
  description = "Create S3 VPC endpoint in source VPC for private subnet access to JDBC drivers"
  default     = "NO"
  validation {
    condition     = contains(["YES", "NO"], var.CreateSourceS3VpcEndpoint)
    error_message = "Must be YES or NO"
  }
}

variable "CreateSourceGlueVpcEndpoint" {
  type        = string
  description = "Create Glue VPC endpoint in source VPC for private subnet access to Glue API"
  default     = "YES"
  validation {
    condition     = contains(["YES", "NO"], var.CreateSourceGlueVpcEndpoint)
    error_message = "Must be YES or NO"
  }
}

# =============================================================================
# Target Network Configuration Parameters (Optional)
# =============================================================================

variable "TargetVpcId" {
  type        = string
  description = "VPC ID where target database resides (optional for cross-VPC access)"
  default     = ""
  validation {
    condition     = can(regex("^(vpc-[a-zA-Z0-9]{8,17})?$", var.TargetVpcId))
    error_message = "Must be a valid VPC ID (vpc-xxxxxxxx) or empty for same-VPC access"
  }
}

variable "TargetSubnetIds" {
  type        = string
  description = "Comma-separated list of subnet IDs for target database access (required if TargetVpcId is specified)"
  default     = ""
}

variable "TargetSecurityGroupIds" {
  type        = string
  description = "Comma-separated list of security group IDs for target database access (required if TargetVpcId is specified)"
  default     = ""
}

variable "TargetAvailabilityZone" {
  type        = string
  description = "Availability Zone for target subnet (required if TargetVpcId is specified)"
  default     = ""
  validation {
    condition     = can(regex("^([a-z]{2}-[a-z]+-[0-9][a-z])?$", var.TargetAvailabilityZone))
    error_message = "Must be a valid availability zone (e.g., us-east-1a) or empty"
  }
}

variable "CreateTargetS3VpcEndpoint" {
  type        = string
  description = "Create S3 VPC endpoint in target VPC for private subnet access to JDBC drivers"
  default     = "NO"
  validation {
    condition     = contains(["YES", "NO"], var.CreateTargetS3VpcEndpoint)
    error_message = "Must be YES or NO"
  }
}

variable "CreateTargetGlueVpcEndpoint" {
  type        = string
  description = "Create Glue VPC endpoint in target VPC for private subnet access to Glue API"
  default     = "YES"
  validation {
    condition     = contains(["YES", "NO"], var.CreateTargetGlueVpcEndpoint)
    error_message = "Must be YES or NO"
  }
}

# =============================================================================
# Glue Connection Configuration Parameters (Optional)
# =============================================================================

variable "CreateSourceConnection" {
  type        = string
  description = "Create a new Glue Connection for source database with AWS Secrets Manager integration (JDBC databases only, not applicable for Iceberg). Credentials will be securely stored in Secrets Manager at /aws-glue/{JobName}-source-jdbc-connection"
  default     = "false"
  validation {
    condition     = contains(["true", "false"], var.CreateSourceConnection)
    error_message = "Must be true or false"
  }
}

variable "CreateTargetConnection" {
  type        = string
  description = "Create a new Glue Connection for target database with AWS Secrets Manager integration (JDBC databases only, not applicable for Iceberg). Credentials will be securely stored in Secrets Manager at /aws-glue/{JobName}-target-jdbc-connection"
  default     = "false"
  validation {
    condition     = contains(["true", "false"], var.CreateTargetConnection)
    error_message = "Must be true or false"
  }
}

variable "UseSourceConnection" {
  type        = string
  description = "Name of existing Glue Connection to use for source database (JDBC databases only, optional). Mutually exclusive with CreateSourceConnection parameter"
  default     = ""
  validation {
    condition     = can(regex("^[a-zA-Z0-9_-]*$", var.UseSourceConnection)) && length(var.UseSourceConnection) <= 255
    error_message = "Must be empty or contain only alphanumeric characters, hyphens, and underscores"
  }
}

variable "UseTargetConnection" {
  type        = string
  description = "Name of existing Glue Connection to use for target database (JDBC databases only, optional). Mutually exclusive with CreateTargetConnection parameter"
  default     = ""
  validation {
    condition     = can(regex("^[a-zA-Z0-9_-]*$", var.UseTargetConnection)) && length(var.UseTargetConnection) <= 255
    error_message = "Must be empty or contain only alphanumeric characters, hyphens, and underscores"
  }
}

# =============================================================================
# Observability Configuration Parameters (Optional)
# =============================================================================

variable "LogRetentionDays" {
  type        = number
  description = "Number of days to retain CloudWatch logs"
  default     = 30
  validation {
    condition     = contains([1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653], var.LogRetentionDays)
    error_message = "Must be a valid CloudWatch log retention period"
  }
}

variable "ErrorLogRetentionDays" {
  type        = number
  description = "Number of days to retain error logs (typically longer than regular logs)"
  default     = 90
  validation {
    condition     = contains([1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653], var.ErrorLogRetentionDays)
    error_message = "Must be a valid CloudWatch log retention period"
  }
}

variable "EnableCloudWatchDashboard" {
  type        = string
  description = "Create CloudWatch Dashboard for monitoring"
  default     = "YES"
  validation {
    condition     = contains(["YES", "NO"], var.EnableCloudWatchDashboard)
    error_message = "Must be YES or NO"
  }
}

variable "EnableCloudWatchAlarms" {
  type        = string
  description = "Create CloudWatch Alarms for monitoring"
  default     = "YES"
  validation {
    condition     = contains(["YES", "NO"], var.EnableCloudWatchAlarms)
    error_message = "Must be YES or NO"
  }
}

variable "AlarmNotificationEmail" {
  type        = string
  description = "Email address for alarm notifications (optional)"
  default     = ""
  validation {
    condition     = can(regex("^$|^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$", var.AlarmNotificationEmail))
    error_message = "Must be a valid email address or empty"
  }
}

variable "JobDurationThresholdMinutes" {
  type        = number
  description = "Threshold in minutes for long-running job alarm"
  default     = 60
  validation {
    condition     = var.JobDurationThresholdMinutes >= 5 && var.JobDurationThresholdMinutes <= 2880
    error_message = "Must be between 5 and 2880 minutes"
  }
}

# =============================================================================
# Performance Optimization Parameters (Optional)
# =============================================================================

variable "CountingStrategy" {
  type        = string
  description = "Strategy for counting rows during migration: auto (recommended - uses fast SQL COUNT for all sizes), immediate (explicit SQL COUNT before write), or deferred (count after write from target - fallback only)"
  default     = "auto"
  validation {
    condition     = contains(["immediate", "deferred", "auto"], var.CountingStrategy)
    error_message = "Must be one of: immediate, deferred, auto"
  }
}

variable "ProgressUpdateInterval" {
  type        = number
  description = "Interval in seconds for emitting progress updates during data transfer operations"
  default     = 60
  validation {
    condition     = var.ProgressUpdateInterval >= 10 && var.ProgressUpdateInterval <= 600
    error_message = "Must be between 10 and 600 seconds"
  }
}

variable "BatchSizeThreshold" {
  type        = number
  description = "Row count threshold for automatic counting strategy selection (datasets larger than this use deferred counting)"
  default     = 1000000
  validation {
    condition     = var.BatchSizeThreshold >= 100000 && var.BatchSizeThreshold <= 10000000
    error_message = "Must be between 100,000 and 10,000,000 rows"
  }
}

variable "EnableDetailedMetrics" {
  type        = string
  description = "Enable detailed CloudWatch metrics for migration progress, counting strategy, and phase-specific performance"
  default     = "true"
  validation {
    condition     = contains(["true", "false"], var.EnableDetailedMetrics)
    error_message = "Must be true or false"
  }
}

# =============================================================================
# Partitioned Read Parameters (Optional)
# =============================================================================

variable "EnablePartitionedReads" {
  type        = string
  description = "Enable parallel JDBC reads for large datasets. auto=detect partition column from PK/indexes or use PartitionedReadConfig, disabled=single-threaded reads (default)"
  default     = "disabled"
  validation {
    condition     = contains(["auto", "disabled"], var.EnablePartitionedReads)
    error_message = "Must be auto or disabled"
  }
}

variable "PartitionedReadConfig" {
  type        = string
  description = "JSON configuration to override partition settings per table. Format: {\"table_name\": {\"partition_column\": \"id\", \"num_partitions\": 20, \"fetch_size\": 10000}}. Only used when EnablePartitionedReads=auto."
  default     = ""
  validation {
    condition     = length(var.PartitionedReadConfig) <= 4096
    error_message = "Must be empty or a valid JSON configuration string (max 4096 chars)"
  }
}

variable "DefaultNumPartitions" {
  type        = number
  description = "Default number of parallel read partitions when auto-detecting. 0 = calculate based on row count (~500K rows per partition)."
  default     = 0
  validation {
    condition     = var.DefaultNumPartitions >= 0 && var.DefaultNumPartitions <= 200
    error_message = "Must be between 0 (auto) and 200"
  }
}

variable "DefaultFetchSize" {
  type        = number
  description = "Default number of rows to fetch per JDBC round-trip. Higher values = faster reads but more memory."
  default     = 10000
  validation {
    condition     = var.DefaultFetchSize >= 100 && var.DefaultFetchSize <= 100000
    error_message = "Must be between 100 and 100,000"
  }
}

# =============================================================================
# Security Configuration Parameters (Optional)
# =============================================================================

variable "SecretsManagerKmsKeyArn" {
  type        = string
  description = "ARN of the KMS key used to encrypt Secrets Manager secrets. When provided, KMS permissions are scoped to this specific key and secrets are encrypted with it. When empty, the default aws/secretsmanager key is used with broader KMS permissions."
  default     = ""
  validation {
    condition     = can(regex("^(arn:aws:kms:[a-z0-9-]+:[0-9]{12}:key/[a-f0-9-]+)?$", var.SecretsManagerKmsKeyArn)) && length(var.SecretsManagerKmsKeyArn) <= 2048
    error_message = "Must be empty or a valid KMS key ARN (arn:aws:kms:region:account:key/key-id)"
  }
}
