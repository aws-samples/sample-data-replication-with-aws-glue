# Parameter Reference

This document provides a comprehensive reference for all parameters available in the AWS Glue Data Replication deployment, along with configuration examples for various database replication scenarios. These parameters are used by both the CloudFormation template and the Terraform module.

## Parameter File Formats

The project supports two parameter file formats. The deploy script auto-detects the format and converts as needed.

### Flat JSON Format (`.tfvars.json`) — Recommended

The canonical format is a flat JSON object. This format works natively with Terraform and is auto-converted to CloudFormation format by the deploy script.

```json
{
  "JobName": "my-replication-job",
  "SourceEngineType": "sqlserver",
  "TargetEngineType": "postgresql",
  "SourceDatabase": "source_db",
  "TargetDatabase": "target_db",
  "TableNames": "customers,orders,products",
  "GlueJobScriptS3Path": "s3://my-bucket/glue-scripts/main.py"
}
```

### CloudFormation Array Format (Legacy)

The legacy format uses an array of `ParameterKey`/`ParameterValue` objects. This format is still accepted by the deploy script for backward compatibility.

```json
[
  {"ParameterKey": "JobName", "ParameterValue": "my-replication-job"},
  {"ParameterKey": "SourceEngineType", "ParameterValue": "sqlserver"}
]
```

All example files in `examples/` use the `.tfvars.json` flat format.

---

## Table of Contents

- [Required Parameters](#required-parameters)
- [Optional Parameters](#optional-parameters)
- [Iceberg Configuration](#iceberg-configuration)
- [Glue Connection Configuration](#glue-connection-configuration)
- [Kerberos Authentication](#kerberos-authentication)
- [Network Configuration](#network-configuration)
- [Performance Optimization](#performance-optimization)
- [Observability Configuration](#observability-configuration)
- [Manual Bookmark Configuration](#manual-bookmark-configuration)
- [Parameter Dependencies](#parameter-dependencies)
- [Configuration Examples](#configuration-examples)
- [Troubleshooting](#troubleshooting)

---

## Required Parameters

These parameters must be provided for every deployment.

### Job Configuration

| Parameter | Type | Description | Constraints | Example |
|-----------|------|-------------|-------------|---------|
| `JobName` | String | Unique name for the Glue data replication job instance | 1-255 characters, alphanumeric, hyphens, underscores only | `oracle-to-postgres-replication` |

### Database Engine Configuration

| Parameter | Type | Description | Allowed Values | Example |
|-----------|------|-------------|----------------|---------|
| `SourceEngineType` | String | Source database engine type | `oracle`, `sqlserver`, `postgresql`, `db2`, `iceberg` | `oracle` |
| `TargetEngineType` | String | Target database engine type | `oracle`, `sqlserver`, `postgresql`, `db2`, `iceberg` | `postgresql` |

### Database Connection Configuration

| Parameter | Type | Description | Constraints | Example |
|-----------|------|-------------|-------------|---------|
| `SourceDatabase` | String | Source database name | 1-128 characters, alphanumeric, hyphens, underscores, periods | `ORCL` |
| `TargetDatabase` | String | Target database name | 1-128 characters, alphanumeric, hyphens, underscores, periods | `target_db` |
| `SourceSchema` | String | Source database schema name | 0-128 characters (optional for Iceberg) | `HR` |
| `TargetSchema` | String | Target database schema name | 0-128 characters (optional for Iceberg) | `public` |
| `TableNames` | CommaDelimitedList | Comma-separated list of table names to replicate | Valid table names | `employees,departments,locations` |

### Database Credentials (Required for JDBC engines)

| Parameter | Type | Description | Constraints | Example |
|-----------|------|-------------|-------------|---------|
| `SourceDbUser` | String | Source database username | 0-128 characters | `hr_user` |
| `SourceDbPassword` | String | Source database password | 0-128 characters, NoEcho=true | `********` |
| `TargetDbUser` | String | Target database username | 0-128 characters | `postgres` |
| `TargetDbPassword` | String | Target database password | 0-128 characters, NoEcho=true | `********` |

### JDBC Connection Strings (Required for JDBC engines)

| Parameter | Type | Description | Format | Example |
|-----------|------|-------------|--------|---------|
| `SourceConnectionString` | String | JDBC connection string for source database | `jdbc:engine://host:port/database` | `jdbc:oracle:thin:@//host:1521/ORCL` |
| `TargetConnectionString` | String | JDBC connection string for target database | `jdbc:engine://host:port/database` | `jdbc:postgresql://host:5432/target_db` |

### S3 Asset Paths

| Parameter | Type | Description | Format | Example |
|-----------|------|-------------|--------|---------|
| `SourceJdbcDriverS3Path` | String | S3 path to source JDBC driver JAR (not required for Iceberg) | `s3://bucket/path/driver.jar` | `s3://bucket/drivers/ojdbc11.jar` |
| `TargetJdbcDriverS3Path` | String | S3 path to target JDBC driver JAR (not required for Iceberg) | `s3://bucket/path/driver.jar` | `s3://bucket/drivers/postgresql.jar` |

> **Note**: `GlueJobScriptS3Path` and `GlueJobModulesS3Path` are automatically derived by `deploy.sh` from the `--bucket` argument. You do not need to specify them in your parameter files. The script sets them to `s3://<bucket>/glue-scripts/main.py` and `s3://<bucket>/glue-modules/glue-job-modules.zip` respectively.

---

## Optional Parameters

These parameters have default values and can be omitted if the defaults are acceptable.

### Glue Job Configuration

| Parameter | Type | Description | Default | Range/Values | Example |
|-----------|------|-------------|---------|--------------|---------|
| `MaxRetries` | Number | Maximum number of retries for the Glue job | `1` | 0-10 | `3` |
| `Timeout` | Number | Timeout for the Glue job in minutes | `2880` | 1-2880 (48 hours) | `1440` |
| `MaxConcurrentRuns` | Number | Maximum number of concurrent runs | `1` | 1-1000 | `2` |
| `WorkerType` | String | Type of predefined worker for the Glue job | `G.1X` | `Standard`, `G.1X`, `G.2X`, `G.025X` | `G.2X` |
| `NumberOfWorkers` | Number | Number of workers for the Glue job | `2` | 2-299 | `4` |

#### Worker Type Selection Guide

| Type | vCPU | Memory | Use Case |
|------|------|--------|----------|
| `G.025X` | 2 | 4 GB | Development, small datasets |
| `G.1X` | 4 | 16 GB | Standard workloads (default) |
| `G.2X` | 8 | 32 GB | Memory-intensive, large datasets |
| `Standard` | 4 | 16 GB | Legacy compatibility |

---

## Iceberg Configuration

Apache Iceberg is supported as both source and target database engine. These parameters are required when using Iceberg engines.

### Iceberg Parameters

| Parameter | Type | Description | Default | Example |
|-----------|------|-------------|---------|---------|
| `SourceWarehouseLocation` | String | S3 warehouse location for source Iceberg tables | `''` | `s3://my-datalake/warehouse/` |
| `TargetWarehouseLocation` | String | S3 warehouse location for target Iceberg tables | `''` | `s3://analytics-lake/warehouse/` |
| `SourceCatalogId` | String | AWS account ID for cross-account Glue Data Catalog access (source) | Current account | `123456789012` |
| `TargetCatalogId` | String | AWS account ID for cross-account Glue Data Catalog access (target) | Current account | `987654321098` |
| `SourceFormatVersion` | String | Iceberg format version for source tables | `2` | `1` or `2` |
| `TargetFormatVersion` | String | Iceberg format version for target tables | `2` | `1` or `2` |

### Iceberg Configuration Notes

- **Automatic Incremental Detection**: Iceberg sources automatically detect and use identifier-field-ids for incremental loading when available
- **Cross-Account Access**: When using `CatalogId`, ensure proper IAM permissions and resource-based policies
- **Format Version**: Version 2 is recommended for better performance and features
- **JDBC Parameters**: Traditional JDBC parameters are not applicable when using Iceberg engine type

---

## Glue Connection Configuration

AWS Glue Connections provide managed connection capabilities for JDBC databases with centralized credential management and AWS Secrets Manager integration.

### Glue Connection Parameters

| Parameter | Type | Description | Default | Example |
|-----------|------|-------------|---------|---------|
| `CreateSourceConnection` | String | Create new Glue Connection for source with Secrets Manager | `false` | `true` |
| `CreateTargetConnection` | String | Create new Glue Connection for target with Secrets Manager | `false` | `true` |
| `UseSourceConnection` | String | Name of existing Glue Connection for source | `''` | `my-oracle-connection` |
| `UseTargetConnection` | String | Name of existing Glue Connection for target | `''` | `my-postgres-connection` |

### Connection Strategy Options

| Strategy | Parameters | Description |
|----------|------------|-------------|
| **Create New** | `CreateSourceConnection=true` | Creates connection with Secrets Manager integration |
| **Use Existing** | `UseSourceConnection=name` | Uses pre-configured Glue Connection |
| **Direct JDBC** | Neither specified | Traditional direct database connection |

### AWS Secrets Manager Integration

When creating new Glue Connections (`Create*Connection=true`):

1. **Secret Path**: `/aws-glue/{connection-name}`
2. **Secret Format**: `{"username": "user", "password": "pass"}`
3. **Benefits**:
   - Credentials encrypted at rest using AWS KMS
   - Fine-grained access control through IAM policies
   - Audit trail via AWS CloudTrail
   - Support for credential rotation

### Required IAM Permissions for Secrets Manager

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:CreateSecret",
        "secretsmanager:GetSecretValue",
        "secretsmanager:PutSecretValue",
        "secretsmanager:DescribeSecret"
      ],
      "Resource": "arn:aws:secretsmanager:*:*:secret:/aws-glue/*"
    }
  ]
}
```

### Validation Rules

- `CreateSourceConnection` and `UseSourceConnection` are **mutually exclusive**
- `CreateTargetConnection` and `UseTargetConnection` are **mutually exclusive**
- Glue Connection parameters only apply to JDBC engines (`oracle`, `sqlserver`, `postgresql`, `db2`)
- Parameters are **ignored** for Iceberg engines (with warnings logged)
- When Kerberos authentication is configured (all three Kerberos parameters provided), `CreateSourceConnection`/`CreateTargetConnection` are **ignored** — the job uses direct JDBC with Kerberos instead of Glue Connections

---

## Kerberos Authentication

Configure Kerberos authentication for enterprise database connections that require Active Directory or Kerberos-based authentication.

### Kerberos Parameters

| Parameter | Type | Description | Default | Example |
|-----------|------|-------------|---------|---------|
| `SourceKerberosSPN` | String | Service Principal Name for source database | `''` | `MSSQLSvc/sqlserver.domain.com:1433` |
| `SourceKerberosDomain` | String | Kerberos realm/domain for source | `''` | `CORP.EXAMPLE.COM` |
| `SourceKerberosKDC` | String | Key Distribution Center hostname for source | `''` | `dc01.corp.example.com` |
| `TargetKerberosSPN` | String | Service Principal Name for target database | `''` | `MSSQLSvc/sqlserver2.domain.com:1433` |
| `TargetKerberosDomain` | String | Kerberos realm/domain for target | `''` | `CORP.EXAMPLE.COM` |
| `TargetKerberosKDC` | String | Key Distribution Center hostname for target | `''` | `dc01.corp.example.com` |

### Keytab Authentication

Keytab files provide passwordless Kerberos authentication. The keytab must be uploaded to S3 before deployment — the deploy scripts do not handle keytab uploads.

| Parameter | Type | Description | Default | Example |
|-----------|------|-------------|---------|---------|
| `SourceKerberosKeytabS3Path` | String | S3 path to keytab file for source | `''` | `s3://bucket/kerberos/job-name/user.keytab` |
| `TargetKerberosKeytabS3Path` | String | S3 path to keytab file for target | `''` | `s3://bucket/kerberos/job-name/user.keytab` |

#### Keytab Upload

Upload the keytab to S3 using the convention `s3://<bucket>/kerberos/<job-name>/<username>.keytab`:

```bash
aws s3 cp /path/to/user.keytab s3://my-bucket/kerberos/my-job-name/user.keytab
```

At runtime, the Glue job downloads the keytab to `/tmp/krb5.keytab` and uses it for authentication. The Glue job's IAM role must have `s3:GetObject` on the keytab path.

When a keytab S3 path is provided, the password parameter (`SourceDbPassword` / `TargetDbPassword`) is not required.

For detailed setup instructions, see [KERBEROS_AUTHENTICATION_GUIDE.md](KERBEROS_AUTHENTICATION_GUIDE.md).

### Supported Engines

Kerberos authentication is supported for:
- **SQL Server** - Windows Integrated Authentication
- **Oracle** - Kerberos authentication
- **PostgreSQL** - GSSAPI authentication
- **DB2** - Kerberos authentication

**Note**: Kerberos is **not supported** for Iceberg engines.

### Configuration Requirements

All three Kerberos parameters (SPN, Domain, KDC) must be provided together:
- If any parameter is missing, Kerberos authentication is disabled
- Partial configuration generates a warning in logs

### Connection Behavior with Kerberos

When Kerberos parameters are provided, the `CreateSourceConnection` and `CreateTargetConnection` parameters are effectively ignored — no Glue JDBC Connection or Secrets Manager secret is created for that side. Instead, the Glue job connects via direct JDBC using Kerberos authentication configured through the job's default arguments (SPN, domain, KDC, keytab). Network connections (created when `SourceVpcId`/`TargetVpcId` are set) are still used for VPC routing.

### SPN Format Examples

| Database | SPN Format | Example |
|----------|------------|---------|
| SQL Server | `MSSQLSvc/hostname:port` | `MSSQLSvc/sqlserver.corp.com:1433` |
| Oracle | `oracle/hostname@REALM` | `oracle/oradb.corp.com@CORP.COM` |
| PostgreSQL | `postgres/hostname@REALM` | `postgres/pgdb.corp.com@CORP.COM` |

For detailed Kerberos setup instructions, see [KERBEROS_AUTHENTICATION_GUIDE.md](KERBEROS_AUTHENTICATION_GUIDE.md).

---

## Network Configuration

Configure cross-VPC access for databases in different VPCs or private subnets.

### Source Network Parameters

| Parameter | Type | Description | Default | Example |
|-----------|------|-------------|---------|---------|
| `SourceVpcId` | String | VPC ID where source database resides | `''` | `vpc-12345678` |
| `SourceSubnetIds` | CommaDelimitedList | Subnet IDs for source database access | `''` | `subnet-123,subnet-456` |
| `SourceSecurityGroupIds` | CommaDelimitedList | Security group IDs for source access | `''` | `sg-12345678` |
| `SourceAvailabilityZone` | String | Availability zone for source subnet | `''` | `us-east-1a` |
| `CreateSourceS3VpcEndpoint` | String | Create S3 VPC endpoint in source VPC | `NO` | `YES` |
| `CreateSourceGlueVpcEndpoint` | String | Create Glue VPC endpoint in source VPC | `YES` | `YES` |

### Target Network Parameters

| Parameter | Type | Description | Default | Example |
|-----------|------|-------------|---------|---------|
| `TargetVpcId` | String | VPC ID where target database resides | `''` | `vpc-87654321` |
| `TargetSubnetIds` | CommaDelimitedList | Subnet IDs for target database access | `''` | `subnet-789,subnet-012` |
| `TargetSecurityGroupIds` | CommaDelimitedList | Security group IDs for target access | `''` | `sg-87654321` |
| `TargetAvailabilityZone` | String | Availability zone for target subnet | `''` | `us-east-1b` |
| `CreateTargetS3VpcEndpoint` | String | Create S3 VPC endpoint in target VPC | `NO` | `YES` |
| `CreateTargetGlueVpcEndpoint` | String | Create Glue VPC endpoint in target VPC | `YES` | `YES` |

### Network Configuration Scenarios

| Configuration | VPC Endpoints Required |
|---------------|----------------------|
| Same VPC | None |
| Cross-VPC | Glue VPC endpoint |
| Private Subnets | Glue + S3 VPC endpoints |

---

## Performance Optimization

Parameters for optimizing migration performance, especially for large datasets.

### Counting Strategy Parameters

| Parameter | Type | Description | Default | Values |
|-----------|------|-------------|---------|--------|
| `CountingStrategy` | String | Strategy for counting rows during migration | `auto` | `immediate`, `deferred`, `auto` |
| `ProgressUpdateInterval` | Number | Interval in seconds for progress updates | `60` | 10-600 |
| `BatchSizeThreshold` | Number | Row count threshold for strategy selection (deprecated) | `1000000` | 100000-10000000 |
| `EnableDetailedMetrics` | String | Enable detailed CloudWatch metrics | `true` | `true`, `false` |

#### Counting Strategy Values

| Value | Behavior | Use Case |
|-------|----------|----------|
| `auto` | Uses SQL COUNT(*) for all sources (recommended) | All scenarios |
| `immediate` | Explicit SQL COUNT before write | When explicit control needed |
| `deferred` | Count from target after write | Fallback when SQL COUNT fails |

### Partitioned Read Parameters

Enable parallel JDBC reads for large datasets by splitting data extraction across multiple connections.

#### Quick Reference

- `EnablePartitionedReads = "auto"` — the job auto-detects partition columns (primary keys or indexed numeric columns) and auto-calculates partition counts based on row count. Tables with explicit config in `PartitionedReadConfig` use those values instead.
- `EnablePartitionedReads = "disabled"` — all tables use a single JDBC connection (default).
- `DefaultNumPartitions = 0` — auto-calculate partition count: `MIN(100, MAX(2, row_count / 500000))`, targeting ~500K rows per partition. If auto-detection fails, falls back to 10 partitions (full load) or 4 partitions (incremental load).
- `PartitionedReadConfig` — JSON object with per-table overrides. Tables listed here use the specified `partition_column` and `num_partitions` directly, bypassing auto-detection.

| Parameter | Type | Description | Default | Range |
|-----------|------|-------------|---------|-------|
| `EnablePartitionedReads` | String | Enable parallel JDBC reads | `disabled` | `auto`, `disabled` |
| `PartitionedReadConfig` | String | JSON configuration for per-table partition settings | `''` | JSON object |
| `DefaultNumPartitions` | Number | Default number of parallel partitions (0=auto) | `0` | 0-200 |
| `DefaultFetchSize` | Number | JDBC fetch size (rows per round-trip) | `10000` | 100-100000 |

#### EnablePartitionedReads Values

| Value | Behavior |
|-------|----------|
| `disabled` | Single-connection JDBC reads (default, reliable for all tables) |
| `auto` | Auto-detect partition columns from primary keys or indexed numeric columns |

#### PartitionedReadConfig JSON Format

```json
{
  "table_name": {
    "partition_column": "column_name",
    "num_partitions": 20,
    "fetch_size": 10000,
    "lower_bound": 1,
    "upper_bound": 1000000
  }
}
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `partition_column` | No | Auto-detected | A numeric column used to split the table into non-overlapping ranges for parallel reading. Spark divides the value range of this column evenly across `num_partitions` ranges, and each range is read by a separate JDBC connection. If omitted, the job auto-detects a suitable column from primary keys or indexed numeric columns. |
| `num_partitions` | No | Auto-calculated | The number of parallel JDBC connections opened against the source database. Each connection reads a distinct, non-overlapping range of the `partition_column`. For example, with `num_partitions: 4` and a column range of 1–1000, Spark creates 4 connections reading ranges ~1–250, ~251–500, ~501–750, ~751–1000. If omitted or set to `0`, it is auto-calculated as `MIN(100, MAX(2, row_count / 500000))`. |
| `fetch_size` | No | `10000` (or `DefaultFetchSize`) | The JDBC `fetchSize` — the number of rows the JDBC driver retrieves from the database in a single network round-trip, **per connection**. This does **not** limit the total rows read by a partition; each partition reads **all** rows in its assigned range, but fetches them in batches of `fetch_size` rows at a time. A higher value reduces the number of network round-trips but increases memory usage per connection. For example, if a partition contains 50,000 rows and `fetch_size` is 5000, that connection makes ~10 round-trips to the database. Minimum value: `100`. |
| `lower_bound` | No | Auto-detected | The minimum value of the `partition_column`, used by Spark to calculate how to split the range across partitions. This is a **hint for range calculation, not a filter** — rows with values below `lower_bound` are still read; they are assigned to the first partition. If omitted, the job queries `MIN(partition_column)` from the table (or from the incremental subset during incremental loads). |
| `upper_bound` | No | Auto-detected | The maximum value of the `partition_column`, used by Spark to calculate how to split the range across partitions. This is a **hint for range calculation, not a filter** — rows with values above `upper_bound` are still read; they are assigned to the last partition. If omitted, the job queries `MAX(partition_column)` from the table (or from the incremental subset during incremental loads). |

#### How Partitioned Reads Work

When partitioned reads are enabled for a table, the job opens `num_partitions` parallel JDBC connections to the source database. Each connection reads a different range of the `partition_column`:

1. Spark divides the range `[lower_bound, upper_bound]` into `num_partitions` equal-sized ranges
2. Each range becomes a separate JDBC query with a `WHERE partition_column BETWEEN x AND y` clause
3. Each query runs on its own JDBC connection in parallel
4. Within each connection, rows are fetched in batches of `fetch_size` rows per network round-trip
5. The resulting DataFrames are combined into a single distributed DataFrame

For **incremental loads**, the job wraps the table in a subquery with the incremental filter (e.g., `WHERE updated_at > last_bookmark_value`) so that each parallel connection only reads the new/changed rows, not the full table.

#### Auto-Detection Logic

**Full Load**: Queries database metadata for numeric primary key columns, falls back to indexed numeric columns.

**Incremental Load**: Uses the bookmark's incremental column as the partition column (more efficient). Bounds are detected from the incremental data subset only.

#### Partition Count Calculation

When `num_partitions` is `0` (auto):
```
optimal_partitions = MIN(100, MAX(2, row_count / 500000))
```

| Row Count | Calculated Partitions |
|-----------|----------------------|
| < 1M | 2 |
| 1M - 5M | 2-10 |
| 5M - 50M | 10-100 |
| > 50M | 100 (max) |

If auto-detection fails to find a suitable partition column or cannot determine row count, the job falls back to:
- **Full load**: 10 partitions (default fallback)
- **Incremental load**: 4 partitions (default fallback)
- If no partition column can be detected at all, the table is read with a single JDBC connection (non-partitioned)

#### When to Use Partitioned Reads

| Scenario | Recommendation |
|----------|----------------|
| Tables < 1M rows | Not needed |
| Tables 1M - 10M rows | Optional |
| Tables 10M - 100M rows | Recommended |
| Tables > 100M rows | Strongly recommended |
| Tables > 1B rows | Required |

### Write Parallelism

Write operations are automatically parallelized based on estimated row count:

| Row Count | Target Write Partitions |
|-----------|------------------------|
| > 5M rows | 20 |
| > 1M rows | 10 |
| > 100K rows | 5 |
| ≤ 100K rows | 2 |

---

## Observability Configuration

Parameters for monitoring, logging, and alerting.

### Logging Parameters

| Parameter | Type | Description | Default | Values |
|-----------|------|-------------|---------|--------|
| `LogRetentionDays` | Number | Days to retain CloudWatch logs | `30` | 1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653 |
| `ErrorLogRetentionDays` | Number | Days to retain error logs | `90` | Same as above |

### Dashboard and Alarm Parameters

| Parameter | Type | Description | Default | Values |
|-----------|------|-------------|---------|--------|
| `EnableCloudWatchDashboard` | String | Create CloudWatch Dashboard | `YES` | `YES`, `NO` |
| `EnableCloudWatchAlarms` | String | Create CloudWatch Alarms | `YES` | `YES`, `NO` |
| `AlarmNotificationEmail` | String | Email for alarm notifications | `''` | Valid email or empty |
| `JobDurationThresholdMinutes` | Number | Threshold for long-running job alarm | `60` | 5-2880 |

---

## Manual Bookmark Configuration

Override automatic incremental column detection for specific tables.

### ManualBookmarkConfig Parameter

| Parameter | Type | Description | Default | Max Length |
|-----------|------|-------------|---------|------------|
| `ManualBookmarkConfig` | String | JSON string with manual bookmark configurations | `''` | 4096 |

### JSON Format

```json
{
  "table_name": "column_name"
}
```

### Examples

**Single Table:**
```json
{
  "employees": "last_modified_date"
}
```

**Multiple Tables:**
```json
{
  "employees": "updated_at",
  "orders": "order_id",
  "products": "last_change_timestamp"
}
```

### JDBC Data Type to Strategy Mapping

| JDBC Data Type | Bookmark Strategy | Use Case |
|----------------|-------------------|----------|
| `TIMESTAMP`, `DATE`, `DATETIME`, `TIME` | `timestamp` | Change tracking with temporal data |
| `INTEGER`, `BIGINT`, `SMALLINT`, `SERIAL` | `primary_key` | Append-only tables with sequential IDs |
| `VARCHAR`, `DECIMAL`, `BOOLEAN`, etc. | `hash` | Tables without suitable timestamp or ID |

### When to Use Manual Configuration

- Tables with non-standard timestamp column names
- Tables with multiple timestamp columns
- Tables requiring specific primary key columns for performance
- Business logic requirements for specific incremental columns

---

## Parameter Dependencies

### Network Configuration Dependencies

- If `SourceVpcId` is specified, `SourceSubnetIds` and `SourceSecurityGroupIds` are required
- If `TargetVpcId` is specified, `TargetSubnetIds` and `TargetSecurityGroupIds` are required
- S3/Glue VPC endpoints only effective when corresponding VPC configuration is provided

### Glue Connection Dependencies

- `CreateSourceConnection` and `UseSourceConnection` are mutually exclusive
- `CreateTargetConnection` and `UseTargetConnection` are mutually exclusive
- When creating connections, all standard JDBC parameters must be provided
- When using existing connections, the named connection must exist in AWS Glue

### Iceberg Configuration Dependencies

- If `SourceEngineType` is `iceberg`: `SourceWarehouseLocation` is required, JDBC parameters are ignored
- If `TargetEngineType` is `iceberg`: `TargetWarehouseLocation` is required, JDBC parameters are ignored
- Glue Connection parameters are ignored for Iceberg engines

---

## Configuration Examples

Parameter files use the flat JSON format (`.tfvars.json`) — a single-level JSON object where keys are parameter names and values are parameter values. This format works natively with both Terraform and the deploy script (which auto-converts to CloudFormation format when needed).

```json
{
  "ParameterName": "value"
}
```

### Traditional Database Examples

#### Oracle to PostgreSQL Migration
```json
{
  "JobName": "oracle-to-postgres-migration",
  "SourceEngineType": "oracle",
  "TargetEngineType": "postgresql",
  "SourceDatabase": "ORCL",
  "SourceSchema": "HR",
  "TargetDatabase": "analytics",
  "TargetSchema": "public",
  "TableNames": "employees,departments,locations",
  "SourceConnectionString": "jdbc:oracle:thin:@//oracle-host:1521/ORCL",
  "TargetConnectionString": "jdbc:postgresql://postgres-host:5432/analytics",
  "SourceDbUser": "hr_user",
  "SourceDbPassword": "oracle_password",
  "TargetDbUser": "postgres_user",
  "TargetDbPassword": "postgres_password",
  "SourceJdbcDriverS3Path": "s3://my-bucket/drivers/ojdbc11.jar",
  "TargetJdbcDriverS3Path": "s3://my-bucket/drivers/postgresql-42.6.0.jar",
  "WorkerType": "G.1X",
  "NumberOfWorkers": "4"
}
```

#### SQL Server to SQL Server Replication
```json
{
  "JobName": "sqlserver-replication",
  "SourceEngineType": "sqlserver",
  "TargetEngineType": "sqlserver",
  "SourceDatabase": "ProductionDB",
  "SourceSchema": "dbo",
  "TargetDatabase": "ReportingDB",
  "TargetSchema": "dbo",
  "TableNames": "orders,customers,products",
  "SourceConnectionString": "jdbc:sqlserver://source-server:1433;databaseName=ProductionDB",
  "TargetConnectionString": "jdbc:sqlserver://target-server:1433;databaseName=ReportingDB",
  "SourceDbUser": "etl_user",
  "SourceDbPassword": "source_password",
  "TargetDbUser": "etl_user",
  "TargetDbPassword": "target_password",
  "SourceJdbcDriverS3Path": "s3://my-bucket/drivers/mssql-jdbc-12.2.0.jre11.jar",
  "TargetJdbcDriverS3Path": "s3://my-bucket/drivers/mssql-jdbc-12.2.0.jre11.jar"
}
```

### Iceberg Examples

#### SQL Server to Iceberg (Data Lake Ingestion)
```json
{
  "JobName": "sqlserver-to-iceberg",
  "SourceEngineType": "sqlserver",
  "TargetEngineType": "iceberg",
  "SourceDatabase": "Operations",
  "SourceSchema": "dbo",
  "TargetDatabase": "analytics_db",
  "TableNames": "transactions,customer_events",
  "SourceConnectionString": "jdbc:sqlserver://source-server:1433;databaseName=Operations",
  "SourceDbUser": "etl_user",
  "SourceDbPassword": "source_password",
  "SourceJdbcDriverS3Path": "s3://my-bucket/drivers/mssql-jdbc-12.2.0.jre11.jar",
  "TargetWarehouseLocation": "s3://my-datalake/warehouse/",
  "TargetFormatVersion": "2",
  "WorkerType": "G.2X",
  "NumberOfWorkers": "5"
}
```

#### Iceberg to PostgreSQL (Data Lake Export)
```json
{
  "JobName": "iceberg-to-postgres",
  "SourceEngineType": "iceberg",
  "TargetEngineType": "postgresql",
  "SourceDatabase": "processed_analytics",
  "TargetDatabase": "reporting",
  "TargetSchema": "public",
  "TableNames": "aggregated_metrics",
  "SourceWarehouseLocation": "s3://analytics-lake/warehouse/",
  "SourceFormatVersion": "2",
  "TargetConnectionString": "jdbc:postgresql://reporting-db:5432/reporting",
  "TargetDbUser": "reporting_user",
  "TargetDbPassword": "postgres_password",
  "TargetJdbcDriverS3Path": "s3://my-bucket/drivers/postgresql-42.6.0.jar"
}
```

#### Cross-Account Iceberg Access
```json
{
  "JobName": "cross-account-iceberg",
  "SourceEngineType": "iceberg",
  "TargetEngineType": "iceberg",
  "SourceDatabase": "shared_analytics",
  "TargetDatabase": "local_analytics",
  "TableNames": "customer_events",
  "SourceWarehouseLocation": "s3://shared-datalake/warehouse/",
  "SourceCatalogId": "123456789012",
  "TargetWarehouseLocation": "s3://local-datalake/warehouse/"
}
```

### Glue Connection Examples

#### Create New Connections with Secrets Manager
```json
{
  "JobName": "oracle-to-postgres-with-secrets",
  "SourceEngineType": "oracle",
  "TargetEngineType": "postgresql",
  "CreateSourceConnection": "true",
  "CreateTargetConnection": "true",
  "SourceDatabase": "ORCL",
  "SourceSchema": "HR",
  "TargetDatabase": "target_db",
  "TargetSchema": "public",
  "TableNames": "employees,departments",
  "SourceConnectionString": "jdbc:oracle:thin:@//oracle-host:1521/ORCL",
  "SourceDbUser": "hr_user",
  "SourceDbPassword": "oracle_password",
  "SourceJdbcDriverS3Path": "s3://my-bucket/drivers/ojdbc11.jar",
  "TargetConnectionString": "jdbc:postgresql://postgres-host:5432/target_db",
  "TargetDbUser": "postgres_user",
  "TargetDbPassword": "postgres_password",
  "TargetJdbcDriverS3Path": "s3://my-bucket/drivers/postgresql.jar"
}
```

#### Use Existing Glue Connections
```json
{
  "JobName": "sqlserver-existing-connections",
  "SourceEngineType": "sqlserver",
  "TargetEngineType": "sqlserver",
  "UseSourceConnection": "production-sqlserver-connection",
  "UseTargetConnection": "reporting-sqlserver-connection",
  "SourceDatabase": "ProductionDB",
  "SourceSchema": "dbo",
  "TargetDatabase": "ReportingDB",
  "TargetSchema": "dbo",
  "TableNames": "orders,customers,products"
}
```


### Performance Optimization Examples

#### Large Dataset with Partitioned Reads
```json
{
  "JobName": "large-dataset-migration",
  "SourceEngineType": "oracle",
  "TargetEngineType": "postgresql",
  "SourceDatabase": "PROD",
  "SourceSchema": "SALES",
  "TargetDatabase": "analytics",
  "TargetSchema": "public",
  "TableNames": "transactions,customer_history,product_catalog",
  "SourceConnectionString": "jdbc:oracle:thin:@//oracle-host:1521/PROD",
  "TargetConnectionString": "jdbc:postgresql://postgres-host:5432/analytics",
  "SourceDbUser": "etl_user",
  "SourceDbPassword": "oracle_password",
  "TargetDbUser": "postgres_user",
  "TargetDbPassword": "postgres_password",
  "SourceJdbcDriverS3Path": "s3://my-bucket/drivers/ojdbc11.jar",
  "TargetJdbcDriverS3Path": "s3://my-bucket/drivers/postgresql.jar",
  "WorkerType": "G.2X",
  "NumberOfWorkers": "10",
  "EnablePartitionedReads": "auto",
  "DefaultNumPartitions": "0",
  "DefaultFetchSize": "10000",
  "CountingStrategy": "auto",
  "ProgressUpdateInterval": "60",
  "EnableDetailedMetrics": "true"
}
```

#### Custom Partitioned Read Configuration
```json
{
  "JobName": "custom-partitioned-reads",
  "EnablePartitionedReads": "auto",
  "PartitionedReadConfig": "{\"orders\": {\"partition_column\": \"order_id\", \"num_partitions\": 30}, \"order_items\": {\"partition_column\": \"order_item_id\", \"num_partitions\": 50, \"fetch_size\": 20000}}"
}
```

### Cross-VPC Network Configuration

#### Cross-VPC with VPC Endpoints
```json
{
  "JobName": "cross-vpc-replication",
  "SourceEngineType": "oracle",
  "TargetEngineType": "postgresql",
  "SourceVpcId": "vpc-source123",
  "SourceSubnetIds": "subnet-src1,subnet-src2",
  "SourceSecurityGroupIds": "sg-oracle-access",
  "SourceAvailabilityZone": "us-east-1a",
  "CreateSourceS3VpcEndpoint": "YES",
  "CreateSourceGlueVpcEndpoint": "YES",
  "TargetVpcId": "vpc-target456",
  "TargetSubnetIds": "subnet-tgt1,subnet-tgt2",
  "TargetSecurityGroupIds": "sg-postgres-access",
  "TargetAvailabilityZone": "us-east-1b",
  "CreateTargetS3VpcEndpoint": "YES",
  "CreateTargetGlueVpcEndpoint": "YES"
}
```

### Manual Bookmark Configuration

#### With Manual Bookmark Columns
```json
{
  "JobName": "manual-bookmark-job",
  "SourceEngineType": "sqlserver",
  "TargetEngineType": "sqlserver",
  "TableNames": "employees,orders,products,audit_log",
  "ManualBookmarkConfig": "{\"employees\": \"last_modified_date\", \"orders\": \"order_timestamp\", \"audit_log\": \"event_time\"}"
}
```

---

## Troubleshooting

### Common Issues

| Issue | Symptoms | Solution |
|-------|----------|----------|
| Progress shows 0% | Using deferred counting or SQL COUNT failed | Check logs for SQL COUNT failure; use `auto` strategy |
| High CloudWatch costs | Too frequent progress updates | Increase `ProgressUpdateInterval` to 120+ seconds |
| Migration slower than expected | Network latency or worker configuration | Check phase durations; increase workers or worker type |
| Out of memory errors | Insufficient worker resources | Increase `WorkerType` or `NumberOfWorkers` |
| Partitioned reads not working | No suitable partition column found | Check logs; specify `partition_column` in config |
| Uneven partition performance | Data skew in partition column | Use different partition column or specify manual bounds |
| Too many database connections | Too many partitions configured | Reduce `num_partitions` or `DefaultNumPartitions` |
| Glue Connection creation fails | Missing Secrets Manager permissions | Add required IAM permissions to Glue job role |
| Connection not found | Nonexistent connection specified | Create the connection or use correct name |

### Parameter Validation Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| Mutually exclusive parameters | Both Create and Use connection specified | Remove one of the conflicting parameters |
| Missing JDBC parameters | Connection creation without required params | Provide all required JDBC parameters |
| Invalid connection name | Special characters in connection name | Use alphanumeric, hyphens, underscores only |
| Iceberg parameter ignored | Glue Connection params with Iceberg engine | Remove Glue params or change engine type |

---

## CloudFormation Deployment

### Required Capabilities

The template creates named IAM resources, requiring `CAPABILITY_NAMED_IAM`:

```bash
aws cloudformation create-stack \
  --stack-name my-data-replication \
  --template-body file://infrastructure/cloudformation/glue-data-replication.yaml \
  --parameters file://my-parameters.json \
  --capabilities CAPABILITY_NAMED_IAM
```

### Created IAM Resources

- `${JobName}-glue-job-role` - Main Glue job execution role

---

## Best Practices

### Security
- Use AWS Secrets Manager for credentials (`CreateSourceConnection=true`)
- Enable VPC endpoints for private connectivity
- Restrict security groups to minimum required access
- Rotate credentials regularly

### Performance
- Use `auto` counting strategy (SQL COUNT is fast for all sizes)
- Enable partitioned reads for tables > 10M rows
- Choose appropriate worker type based on data volume
- Monitor CloudWatch metrics for optimization opportunities

### Cost Optimization
- Use `G.025X` workers for development/small datasets
- Reduce progress update frequency for long jobs
- Disable detailed metrics in development environments
- Set appropriate timeout values to avoid runaway jobs

### Reliability
- Set appropriate retry counts based on data criticality
- Use multiple subnets across availability zones
- Configure proper security group rules
- Test network connectivity before production deployment
