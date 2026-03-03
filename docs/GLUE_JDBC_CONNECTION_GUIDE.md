# Standalone Glue JDBC Connection - Terraform Script

A convenience Terraform script to create an AWS Glue JDBC Connection with Secrets Manager credential storage. This creates the same resources that the main deployment (`main.tf`) provisions when `CreateSourceConnection` or `CreateTargetConnection` is set to `"true"`.

Use this when you need to create a Glue JDBC Connection independently, without deploying the full replication stack.

## What It Creates

1. **AWS Secrets Manager Secret** at `/aws-glue/{connection_name}` containing `{"username": "...", "password": "..."}`
2. **AWS Glue Connection** of type `JDBC` configured with:
   - `JDBC_CONNECTION_URL` — your JDBC connection string
   - `SECRET_ID` — reference to the Secrets Manager secret
   - `JDBC_DRIVER_JAR_URI` — S3 path to the JDBC driver JAR
   - `JDBC_DRIVER_CLASS_NAME` — auto-resolved from the engine type
3. **Physical connection requirements** (VPC subnet, security groups, AZ) — optional, only when network parameters are provided

## Script Location

```
infrastructure/terraform/glue-jdbc-connection/glue-jdbc-connection.tf
```

## Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `connection_name` | Yes | Name for the Glue connection (e.g., `my-job-source-jdbc-connection`) |
| `engine_type` | Yes | Database engine: `oracle`, `sqlserver`, `postgresql`, or `db2` |
| `jdbc_connection_url` | Yes | JDBC connection string (e.g., `jdbc:sqlserver://host:1433;databaseName=mydb`) |
| `jdbc_driver_s3_path` | Yes | S3 path to the JDBC driver JAR (e.g., `s3://bucket/drivers/driver.jar`) |
| `db_username` | Yes | Database username |
| `db_password` | Yes | Database password (stored in Secrets Manager, marked sensitive) |
| `secrets_manager_kms_key_arn` | No | KMS key ARN for encrypting the secret (defaults to `aws/secretsmanager`) |
| `subnet_id` | No | Subnet ID for VPC connectivity |
| `security_group_ids` | No | List of security group IDs for VPC connectivity |
| `availability_zone` | No | Availability zone for VPC connectivity |

> **Note**: All three network parameters (`subnet_id`, `security_group_ids`, `availability_zone`) must be provided together for VPC connectivity. If any is omitted, the connection is created without physical network requirements.

## JDBC Driver Class Mapping

The script automatically resolves the JDBC driver class name based on `engine_type`:

| Engine Type | JDBC Driver Class |
|-------------|-------------------|
| `oracle` | `oracle.jdbc.OracleDriver` |
| `sqlserver` | `com.microsoft.sqlserver.jdbc.SQLServerDriver` |
| `postgresql` | `org.postgresql.Driver` |
| `db2` | `com.ibm.db2.jcc.DB2Driver` |

## Quick Start

### 1. Update the parameter file

An example parameter file named `terraform.tfvars.json` is placed in the same directory as the `.tf` file so Terraform auto-loads it:

```bash
ls infrastructure/terraform/glue-jdbc-connection/terraform.tfvars.json
```

Then edit `terraform.tfvars.json` with your values:

```json
{
  "connection_name": "my-job-source-jdbc-connection",
  "engine_type": "sqlserver",
  "jdbc_connection_url": "jdbc:sqlserver://my-database.crvpwxncbjrg.us-east-1.rds.amazonaws.com:1433;databaseName=mydb",
  "jdbc_driver_s3_path": "s3://my-glue-bucket/jdbc-drivers/sqlserver/12.2.0.jre11/mssql-jdbc-12.2.0.jre11.jar",
  "db_username": "admin",
  "db_password": "your-password-here",
  "subnet_id": "subnet-0abc1234def56789a",
  "security_group_ids": ["sg-0abc1234def56789a"],
  "availability_zone": "us-east-1a"
}
```

> **Why `terraform.tfvars.json`?** Terraform auto-loads files named `terraform.tfvars` or `terraform.tfvars.json` from the working directory. This avoids path resolution issues with the `-var-file` flag.

### 2. Initialize Terraform

```bash
cd infrastructure/terraform/glue-jdbc-connection
terraform init
```

### 3. Preview the changes

```bash
terraform plan
```

### 4. Apply

```bash
terraform apply
```

### 5. Verify

```bash
# Check the Glue connection
aws glue get-connection --name test-jdbc-connection

# Check the Secrets Manager secret
aws secretsmanager describe-secret --secret-id /aws-glue/test-jdbc-connection
```

## Example: Creating Connections for Multiple Databases

### Oracle source connection

```json
{
  "connection_name": "replication-oracle-source",
  "engine_type": "oracle",
  "jdbc_connection_url": "jdbc:oracle:thin:@//oracle-host.example.com:1521/ORCL",
  "jdbc_driver_s3_path": "s3://my-bucket/jdbc-drivers/oracle/ojdbc11.jar",
  "db_username": "repl_user",
  "db_password": "secure-password",
  "subnet_id": "subnet-0abc1234def56789a",
  "security_group_ids": ["sg-0abc1234def56789a"],
  "availability_zone": "us-east-1a"
}
```

### PostgreSQL target connection (no VPC)

```json
{
  "connection_name": "replication-postgres-target",
  "engine_type": "postgresql",
  "jdbc_connection_url": "jdbc:postgresql://postgres-host.example.com:5432/targetdb",
  "jdbc_driver_s3_path": "s3://my-bucket/jdbc-drivers/postgresql/postgresql-42.7.1.jar",
  "db_username": "repl_user",
  "db_password": "secure-password"
}
```

## Cleanup

To destroy the connection and its associated secret:

```bash
cd infrastructure/terraform/glue-jdbc-connection
terraform destroy
```

## Outputs

| Output | Description |
|--------|-------------|
| `connection_name` | Name of the created Glue JDBC connection |
| `secret_arn` | ARN of the Secrets Manager secret |
| `secret_name` | Name of the Secrets Manager secret |

## Related Documentation

- [Deployment Guide](DEPLOYMENT_GUIDE.md) — Full stack deployment with `CreateSourceConnection`/`CreateTargetConnection`
- [Parameter Reference](PARAMETER_REFERENCE.md) — All parameters for the main deployment
- [Database Configuration Guide](DATABASE_CONFIGURATION_GUIDE.md) — JDBC driver download and setup
- [Network Configuration Guide](NETWORK_CONFIGURATION_GUIDE.md) — VPC, subnet, and security group setup
