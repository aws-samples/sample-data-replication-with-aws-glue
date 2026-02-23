# AWS Glue Data Replication - Terraform Deployment Guide

This guide covers deploying the AWS Glue Data Replication solution using Terraform. For CloudFormation deployment, see [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md).

## Prerequisites

- **Terraform** >= 1.0 ([install guide](https://developer.hashicorp.com/terraform/install))
- **AWS CLI** configured with credentials that have permissions to create IAM roles, Glue jobs, CloudWatch resources, Secrets Manager secrets, and VPC endpoints
- **S3 bucket** for hosting Glue job assets (main script, modules zip, config files, JDBC drivers)
- **Database connection details** for source and target (JDBC connection strings, credentials, driver JARs)
- **VPC configuration** (VPC ID, subnet IDs, security group IDs) if deploying with Glue Connections or VPC endpoints

## Module Structure

The Terraform module lives under `infrastructure/terraform/` with a standard 3-file layout:

```
infrastructure/terraform/
├── variables.tf    # All ~50 input variables (mirrors CFN parameters)
├── main.tf         # Locals (conditions), all resources, data sources
└── outputs.tf      # Output values (job name, role ARN, resource IDs)
```

| File | Purpose |
|------|---------|
| `variables.tf` | Input variables with types, defaults, and validation rules matching the CloudFormation parameters. Also contains provider and Terraform version constraints. |
| `main.tf` | Locals block with ~30 condition booleans, plus all AWS resources: IAM role, Glue job, Glue connections, Secrets Manager secrets, VPC endpoints, CloudWatch log groups/dashboard/alarms, SNS topic, and Log Insights queries. |
| `outputs.tf` | Outputs for all created resource identifiers (Glue job name, IAM role ARN, connection names, secret ARNs, etc.). Conditional resources output `null` when not created. |

Terraform treats all `.tf` files in a directory as a single module, so this structure is purely organizational. The module requires the AWS provider (>= 4.0) and Terraform >= 1.0.

## Deployment Workflow

### Step 1: Configure a Parameter File

Create a `.tfvars.json` file with your deployment parameters. This is a flat JSON object where keys match the Terraform variable names:

```json
{
  "JobName": "my-replication-job",
  "SourceEngineType": "sqlserver",
  "TargetEngineType": "sqlserver",
  "SourceDatabase": "source_db",
  "TargetDatabase": "target_db",
  "SourceSchema": "dbo",
  "TargetSchema": "dbo",
  "TableNames": "customers,orders,products",
  "SourceDbUser": "admin",
  "SourceDbPassword": "your-password",
  "TargetDbUser": "admin",
  "TargetDbPassword": "your-password",
  "SourceConnectionString": "jdbc:sqlserver://source-host:1433;databaseName=source_db",
  "TargetConnectionString": "jdbc:sqlserver://target-host:1433;databaseName=target_db",
  "SourceJdbcDriverS3Path": "s3://my-bucket/jdbc-drivers/mssql-jdbc.jar",
  "TargetJdbcDriverS3Path": "s3://my-bucket/jdbc-drivers/mssql-jdbc.jar",
  "GlueJobScriptS3Path": "s3://my-bucket/glue-scripts/main.py",
  "WorkerType": "G.1X",
  "NumberOfWorkers": "3"
}
```

See the `examples/` directory for ready-to-use parameter files covering common scenarios. All example files use the `.tfvars.json` extension and work with both Terraform and the `deploy.sh` script.

### Step 2: Upload Glue Job Assets to S3

Before running Terraform, upload the Glue job code and dependencies to your S3 bucket. You can use `deploy.sh` for this (it handles packaging and upload automatically), or upload manually:

```bash
# Option A: Use deploy.sh with --skip-upload=false (uploads assets only)
./deploy.sh -b my-bucket -p my-params.tfvars.json --type terraform --dry-run

# Option B: Manual upload
./infrastructure/scripts/package-glue-modules.sh --clean
aws s3 cp dist/main.py s3://my-bucket/glue-scripts/main.py
aws s3 cp dist/glue-job-modules.zip s3://my-bucket/glue-modules/glue-job-modules.zip
```

### Step 3: Initialize Terraform

```bash
cd infrastructure/terraform
terraform init
```

This downloads the AWS provider and initializes the working directory.

### Step 4: Review the Plan

```bash
terraform plan -var-file=../../examples/sqlserver-to-sqlserver-parameters.tfvars.json
```

Review the plan output to confirm the resources that will be created. The module uses `count` to conditionally create resources based on your parameter values — only the resources relevant to your configuration will appear.

### Step 5: Apply

```bash
terraform apply -var-file=../../examples/sqlserver-to-sqlserver-parameters.tfvars.json
```

Type `yes` when prompted to confirm. Terraform will create all resources and display the outputs.

### Using deploy.sh (Recommended)

The `deploy.sh` script provides an automated workflow that handles asset upload, format conversion, and Terraform execution in one command:

```bash
# Full deployment
./deploy.sh -b my-bucket \
  -p examples/sqlserver-to-sqlserver-parameters.tfvars.json \
  --type terraform

# Validate only (runs terraform validate + plan, no apply)
./deploy.sh -b my-bucket \
  -p examples/sqlserver-to-sqlserver-parameters.tfvars.json \
  --type terraform --validate-only

# Dry run (shows terraform plan output, no apply)
./deploy.sh -b my-bucket \
  -p examples/sqlserver-to-sqlserver-parameters.tfvars.json \
  --type terraform --dry-run

# Destroy all Terraform-managed resources
./deploy.sh -b my-bucket \
  -p examples/sqlserver-to-sqlserver-parameters.tfvars.json \
  --type terraform --destroy

# Use -s to name the plan file (useful when managing multiple deployments)
./deploy.sh -s my-deployment -b my-bucket \
  -p examples/sqlserver-to-sqlserver-parameters.tfvars.json \
  --type terraform
```

The script automatically:
1. Creates the S3 bucket if it doesn't exist
2. Packages and uploads Glue job assets (main.py, modules zip, config files)
3. Updates `GlueJobScriptS3Path` and `GlueJobModulesS3Path` in the parameter file
4. Runs `terraform init`, `terraform plan`, and `terraform apply`

When `-s` is provided for Terraform deployments, it names the plan file (e.g., `my-deployment.tfplan` instead of the default `tfplan`). This is useful when managing multiple deployments from the same Terraform directory.

If your parameter file is in CloudFormation array format, the script auto-detects and converts it to flat JSON before passing it to Terraform.

## Variable Reference

The table below lists the most commonly configured variables. See `infrastructure/terraform/variables.tf` for the complete list with all defaults, types, and validation rules.

| Variable | Description | Required | Example |
|----------|-------------|----------|---------|
| `JobName` | Unique name for the Glue job | Yes | `"my-replication-job"` |
| `SourceEngineType` | Source database engine | Yes | `"sqlserver"`, `"oracle"`, `"postgresql"`, `"db2"`, `"iceberg"` |
| `TargetEngineType` | Target database engine | Yes | `"sqlserver"`, `"oracle"`, `"postgresql"`, `"db2"`, `"iceberg"` |
| `SourceDatabase` | Source database name | Yes | `"source_db"` |
| `TargetDatabase` | Target database name | Yes | `"target_db"` |
| `TableNames` | Comma-separated table list | Yes | `"customers,orders"` |
| `GlueJobScriptS3Path` | S3 path to main.py | Yes | `"s3://bucket/glue-scripts/main.py"` |
| `SourceConnectionString` | Source JDBC URL | No* | `"jdbc:sqlserver://host:1433;databaseName=db"` |
| `TargetConnectionString` | Target JDBC URL | No* | `"jdbc:postgresql://host:5432/db"` |
| `SourceJdbcDriverS3Path` | S3 path to source JDBC driver | No* | `"s3://bucket/drivers/mssql-jdbc.jar"` |
| `TargetJdbcDriverS3Path` | S3 path to target JDBC driver | No* | `"s3://bucket/drivers/ojdbc11.jar"` |
| `WorkerType` | Glue worker type | No | `"G.1X"` (default), `"G.2X"`, `"Standard"` |
| `NumberOfWorkers` | Number of Glue workers | No | `"3"` (default) |
| `CreateSourceConnection` | Create a Glue JDBC connection for source | No | `"true"` / `"false"` (default) |
| `CreateTargetConnection` | Create a Glue JDBC connection for target | No | `"true"` / `"false"` (default) |
| `EnableCloudWatchDashboard` | Create CloudWatch dashboard | No | `"YES"` / `"NO"` (default) |
| `EnableCloudWatchAlarms` | Create CloudWatch alarms | No | `"YES"` / `"NO"` (default) |
| `AlarmNotificationEmail` | Email for alarm notifications (creates SNS topic) | No | `"ops@example.com"` |

*Required for JDBC engines, not needed for Iceberg.


## Common Deployment Scenarios

### SQL Server to SQL Server (JDBC)

Basic JDBC replication with Glue Connections and VPC configuration:

```bash
terraform apply -var-file=../../examples/sqlserver-to-sqlserver-parameters.tfvars.json
```

See [`examples/sqlserver-to-sqlserver-parameters.tfvars.json`](../examples/sqlserver-to-sqlserver-parameters.tfvars.json) for the full parameter file. Key settings:

```json
{
  "SourceEngineType": "sqlserver",
  "TargetEngineType": "sqlserver",
  "CreateSourceConnection": "true",
  "CreateTargetConnection": "true",
  "SourceVpcId": "vpc-0123456789abcdef0",
  "SourceSubnetIds": "subnet-0123456789abcdef0",
  "SourceSecurityGroupIds": "sg-0123456789abcdef0"
}
```

### Oracle to PostgreSQL

Cross-engine migration with alarm notifications:

```bash
terraform apply -var-file=../../examples/oracle-to-postgresql-parameters.tfvars.json
```

See [`examples/oracle-to-postgresql-parameters.tfvars.json`](../examples/oracle-to-postgresql-parameters.tfvars.json). Key settings:

```json
{
  "SourceEngineType": "oracle",
  "TargetEngineType": "postgresql",
  "SourceJdbcDriverS3Path": "s3://my-bucket/jdbc-drivers/oracle/ojdbc11.jar",
  "TargetJdbcDriverS3Path": "s3://my-bucket/jdbc-drivers/postgresql/postgresql-42.6.0.jar",
  "EnableCloudWatchAlarms": "YES",
  "AlarmNotificationEmail": "admin@example.com"
}
```

### Iceberg Source or Target

When using Iceberg as a source or target, JDBC connection parameters are not needed for the Iceberg side. The module automatically skips Glue Connection and Secrets Manager resources for Iceberg engines.

```bash
terraform apply -var-file=../../examples/iceberg-source-basic-parameters.tfvars.json
```

See [`examples/iceberg-source-basic-parameters.tfvars.json`](../examples/iceberg-source-basic-parameters.tfvars.json) and [`examples/iceberg-to-iceberg-parameters.tfvars.json`](../examples/iceberg-to-iceberg-parameters.tfvars.json). Key settings for an Iceberg source:

```json
{
  "SourceEngineType": "iceberg",
  "TargetEngineType": "postgresql",
  "SourceWarehouseLocation": "s3://my-datalake-bucket/warehouse/",
  "SourceConnectionString": "",
  "SourceJdbcDriverS3Path": ""
}
```

### Cross-VPC with Glue Connections

For deployments where source and target databases are in different VPCs, configure VPC parameters for both sides and enable VPC endpoints as needed:

```bash
terraform apply -var-file=../../examples/sqlserver-to-sqlserver-create-glue-connections-parameters.tfvars.json
```

See [`examples/sqlserver-to-sqlserver-create-glue-connections-parameters.tfvars.json`](../examples/sqlserver-to-sqlserver-create-glue-connections-parameters.tfvars.json). Key settings:

```json
{
  "CreateSourceConnection": "true",
  "CreateTargetConnection": "true",
  "SourceVpcId": "vpc-source123",
  "SourceSubnetIds": "subnet-source123",
  "SourceSecurityGroupIds": "sg-source123",
  "SourceAvailabilityZone": "us-east-1a",
  "TargetVpcId": "vpc-target456",
  "TargetSubnetIds": "subnet-target456",
  "TargetSecurityGroupIds": "sg-target456",
  "TargetAvailabilityZone": "us-east-1b",
  "CreateSourceGlueVpcEndpoint": "YES",
  "CreateTargetGlueVpcEndpoint": "YES",
  "CreateSourceS3VpcEndpoint": "YES",
  "CreateTargetS3VpcEndpoint": "YES"
}
```

### Kerberos Authentication

For SQL Server or DB2 with Kerberos authentication, provide the Kerberos SPN, domain, KDC, and optionally a keytab file in S3:

```bash
terraform apply -var-file=../../examples/sqlserver-to-sqlserver-kerberos-parameters.tfvars.json
```

See [`examples/sqlserver-to-sqlserver-kerberos-parameters.tfvars.json`](../examples/sqlserver-to-sqlserver-kerberos-parameters.tfvars.json) and [`examples/db2-to-db2-kerberos-parameters.tfvars.json`](../examples/db2-to-db2-kerberos-parameters.tfvars.json). Key settings:

```json
{
  "SourceKerberosSPN": "MSSQLSvc/db-host.domain.com:1433",
  "SourceKerberosDomain": "DOMAIN.COM",
  "SourceKerberosKDC": "DOMAIN.COM",
  "SourceKerberosKeytabS3Path": "s3://my-bucket/kerberos/user.keytab",
  "CreateSourceConnection": "true"
}
```

When Kerberos parameters are provided, the module creates Kerberos-specific JDBC connections instead of standard JDBC connections.

## State Management

### Local State (Default)

By default, Terraform stores state in a local `terraform.tfstate` file in the `infrastructure/terraform/` directory. This works for individual use but is not recommended for team environments.

### Remote State with S3 Backend (Recommended)

For team environments, configure an S3 backend to store state remotely with locking via DynamoDB. Create a `backend.tf` file in `infrastructure/terraform/`:

```hcl
terraform {
  backend "s3" {
    bucket         = "my-terraform-state-bucket"
    key            = "glue-data-replication/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-state-lock"
    encrypt        = true
  }
}
```

Then reinitialize:

```bash
terraform init -migrate-state
```

The DynamoDB table provides state locking to prevent concurrent modifications. Create it with a `LockID` string partition key:

```bash
aws dynamodb create-table \
  --table-name terraform-state-lock \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST
```

### Workspaces

If you deploy multiple instances of the replication job (e.g., dev, staging, prod), use Terraform workspaces to isolate state:

```bash
terraform workspace new dev
terraform apply -var-file=dev.tfvars.json

terraform workspace new prod
terraform apply -var-file=prod.tfvars.json
```

## Troubleshooting

### Missing Required Variables

```
Error: No value for required variable
  on variables.tf line XX:
  XX: variable "JobName" {
```

Ensure your `.tfvars.json` file includes all required variables (`JobName`, `SourceEngineType`, `TargetEngineType`, `SourceDatabase`, `TargetDatabase`, `TableNames`, `GlueJobScriptS3Path`). Check `variables.tf` for which variables have no default.

### Provider Authentication Errors

```
Error: error configuring Terraform AWS Provider: no valid credential sources found
```

Terraform uses the standard AWS credential chain. Verify your credentials:

```bash
aws sts get-caller-identity
```

If using a named profile, set the `AWS_PROFILE` environment variable or configure the provider block in `main.tf`.

### State Lock Errors

```
Error: Error acquiring the state lock
```

This occurs when another Terraform process holds the lock (common in team environments with S3 backend). If the lock is stale (e.g., a previous run crashed), force-unlock it:

```bash
terraform force-unlock <LOCK_ID>
```

Use this with caution — only when you're certain no other process is running.

### Variable Validation Errors

```
Error: Invalid value for variable
  "SourceEngineType" must be one of: oracle, sqlserver, postgresql, db2, iceberg
```

The module includes validation rules that mirror the CloudFormation parameter constraints. Check the error message for allowed values and update your `.tfvars.json` file accordingly.

### Resource Already Exists

```
Error: creating IAM Role (my-job-glue-role): EntityAlreadyExists
```

This happens when a resource with the same name already exists outside of Terraform's state (e.g., from a previous CloudFormation deployment). Either:
- Import the existing resource: `terraform import aws_iam_role.glue_job_role my-job-glue-role`
- Delete the existing resource manually before applying
- Use a different `JobName` to avoid naming conflicts

### Plan Shows Unexpected Changes

If `terraform plan` shows changes you don't expect, it may be due to:
- State drift (resources modified outside Terraform) — run `terraform refresh`
- Provider version differences — pin the provider version in `variables.tf`
- Parameter file differences — compare your `.tfvars.json` with the previous deployment

## Additional Resources

- [Deployment Guide](DEPLOYMENT_GUIDE.md) — CloudFormation deployment and general setup
- [Parameter Reference](PARAMETER_REFERENCE.md) — Complete parameter documentation
- [Database Configuration Guide](DATABASE_CONFIGURATION_GUIDE.md) — JDBC driver setup and database-specific configuration
- [Quick Start Guide](QUICK_START_GUIDE.md) — Getting started with both deployment methods
