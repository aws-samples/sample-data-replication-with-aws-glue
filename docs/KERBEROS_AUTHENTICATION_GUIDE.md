# Kerberos Authentication Guide

## Overview

This guide provides comprehensive instructions for configuring Kerberos authentication in the AWS Glue Data Replication solution. Kerberos authentication enables secure, enterprise-grade authentication to source and target databases without storing credentials in parameter files.

## Prerequisites

### AWS Glue Environment Requirements

1. **AWS Glue Version**: Glue 3.0 or later with Python 3.7+
2. **Network Connectivity**: 
   - Glue job must have network access to Kerberos Key Distribution Center (KDC)
   - Database servers must be accessible from Glue job's VPC/subnet
   - Port 88 (Kerberos) must be open between Glue job and KDC
3. **IAM Permissions**: 
   - Standard Glue job execution permissions
   - Access to AWS Secrets Manager (if storing Kerberos keytabs)
   - VPC and subnet permissions for network access

### Kerberos Infrastructure Requirements

1. **Active Directory or Kerberos KDC**: Properly configured and accessible
2. **Service Principal Names (SPNs)**: Registered for each database service
3. **Kerberos Realm**: Properly configured domain/realm
4. **Network Time Protocol (NTP)**: Synchronized time across all systems
5. **DNS Resolution**: Proper forward and reverse DNS for all systems

### Database-Specific Requirements

#### SQL Server
- SQL Server must be configured for Kerberos authentication
- Service Principal Name (SPN) registered in Active Directory
- SQL Server service account must have appropriate permissions
- Example SPN format: `MSSQLSvc/hostname.domain.com:1433`

#### Oracle
- Oracle database configured for Kerberos authentication
- Oracle Net Services configured with Kerberos parameters
- Service Principal Name registered for Oracle service
- Example SPN format: `oracle/hostname.domain.com@REALM.COM`

#### PostgreSQL
- PostgreSQL configured with GSSAPI authentication
- Kerberos configuration in postgresql.conf and pg_hba.conf
- Service Principal Name registered for PostgreSQL service
- Example SPN format: `postgres/hostname.domain.com@REALM.COM`

#### IBM DB2
- DB2 configured for Kerberos authentication
- DB2 security plugin configured for Kerberos
- Service Principal Name registered for DB2 service
- Example SPN format: `db2/hostname.domain.com@REALM.COM`

## Configuration Parameters

### Kerberos Parameters

The solution supports six new CloudFormation parameters for Kerberos authentication:

#### Source Database Kerberos Parameters
- **SourceKerberosSPN**: Service Principal Name for source database
- **SourceKerberosDomain**: Kerberos domain/realm for source database
- **SourceKerberosKDC**: Key Distribution Center for source database

#### Target Database Kerberos Parameters
- **TargetKerberosSPN**: Service Principal Name for target database
- **TargetKerberosDomain**: Kerberos domain/realm for target database
- **TargetKerberosKDC**: Key Distribution Center for target database

### Keytab File Parameters

The solution uses keytab files stored in S3 for Kerberos authentication. At runtime, the Glue job downloads the keytab from S3 to the local filesystem and uses it for authentication — no password is needed in the parameter file.

#### Source and Target Keytab Parameters
- **SourceKerberosKeytabS3Path**: S3 path to the keytab file for source database authentication
- **TargetKerberosKeytabS3Path**: S3 path to the keytab file for target database authentication

#### Uploading the Keytab to S3

The keytab file must be uploaded manually to your S3 bucket before deploying the stack. The deployment scripts (`deploy.sh`, `upload-assets.sh`) do not handle keytab uploads.

Use the following S3 path convention:

```
s3://<bucket>/kerberos/<job-name>/<username>.keytab
```

Upload with the AWS CLI:

```bash
aws s3 cp /path/to/local/username.keytab s3://my-bucket/kerberos/my-job-name/username.keytab
```

Verify the upload:

```bash
aws s3 ls s3://my-bucket/kerberos/my-job-name/
```

Then reference the S3 path in your parameter file:

```json
{
  "SourceKerberosKeytabS3Path": "s3://my-bucket/kerberos/my-job-name/username.keytab",
  "TargetKerberosKeytabS3Path": "s3://my-bucket/kerberos/my-job-name/username.keytab"
}
```

#### IAM Permissions

The Glue job's IAM execution role must have `s3:GetObject` permission on the keytab S3 path. If your keytab is in the same bucket used for deployment assets, the default IAM policy already covers this. If you use a different bucket, add a policy like:

```json
{
  "Effect": "Allow",
  "Action": "s3:GetObject",
  "Resource": "arn:aws:s3:::my-bucket/kerberos/*"
}
```

#### How It Works at Runtime

1. The Glue job reads the `SourceKerberosKeytabS3Path` or `TargetKerberosKeytabS3Path` parameter
2. Downloads the keytab from S3 to `/tmp/krb5.keytab` on the Glue worker
3. Sets file permissions to `0600` (owner-read only)
4. Generates a JAAS configuration that references the local keytab
5. Configures JVM system properties for Kerberos authentication

Because the keytab is downloaded fresh on each job run, you can rotate keytabs by uploading a new file to the same S3 path — no redeployment needed.

#### Security Best Practices for Keytab Files

- Restrict S3 bucket access to only the Glue job's IAM role
- Enable S3 server-side encryption (SSE-S3 or SSE-KMS) on the keytab objects
- Enable S3 bucket versioning to maintain an audit trail of keytab changes
- Do not store keytab files in public or broadly accessible buckets
- Use a dedicated S3 prefix (e.g., `kerberos/`) to scope IAM permissions narrowly

### Parameter Format Requirements

#### Service Principal Name (SPN)
- Format: `service/hostname.domain.com` or `service/hostname.domain.com@REALM.COM`
- Must match exactly what is registered in Active Directory/KDC
- Case-sensitive in most environments

#### Kerberos Domain/Realm
- Format: `DOMAIN.COM` (typically uppercase)
- Must match the Kerberos realm configuration
- Should correspond to Active Directory domain

#### Key Distribution Center (KDC)
- Format: `hostname.domain.com` or `hostname.domain.com:88`
- Can be hostname or IP address
- Port 88 is default for Kerberos, specify if different

## Configuration Examples

### SQL Server to SQL Server with Kerberos

```json
{
  "SourceEngine": "sqlserver",
  "SourceHost": "source-sql.company.com",
  "SourcePort": "1433",
  "SourceDatabase": "SourceDB",
  "SourceKerberosSPN": "MSSQLSvc/source-sql.company.com:1433",
  "SourceKerberosDomain": "COMPANY.COM",
  "SourceKerberosKDC": "dc1.company.com",
  "SourceKerberosKeytabS3Path": "s3://my-bucket/kerberos/sqlserver-replication/svc_user.keytab",
  
  "TargetEngine": "sqlserver",
  "TargetHost": "target-sql.company.com",
  "TargetPort": "1433",
  "TargetDatabase": "TargetDB",
  "TargetKerberosSPN": "MSSQLSvc/target-sql.company.com:1433",
  "TargetKerberosDomain": "COMPANY.COM",
  "TargetKerberosKDC": "dc1.company.com",
  "TargetKerberosKeytabS3Path": "s3://my-bucket/kerberos/sqlserver-replication/svc_user.keytab",
  
  "TableList": "dbo.Users,dbo.Orders"
}
```

### Oracle to PostgreSQL Mixed Authentication

```json
{
  "SourceEngine": "oracle",
  "SourceHost": "oracle-prod.company.com",
  "SourcePort": "1521",
  "SourceDatabase": "PRODDB",
  "SourceKerberosSPN": "oracle/oracle-prod.company.com@COMPANY.COM",
  "SourceKerberosDomain": "COMPANY.COM",
  "SourceKerberosKDC": "dc1.company.com",
  "SourceKerberosKeytabS3Path": "s3://my-bucket/kerberos/oracle-to-pg/oracle_svc.keytab",
  
  "TargetEngine": "postgresql",
  "TargetHost": "postgres-dev.company.com",
  "TargetPort": "5432",
  "TargetDatabase": "devdb",
  "TargetUsername": "replication_user",
  "TargetPassword": "secure_password",
  
  "TableList": "hr.employees,sales.orders"
}
```

### DB2 to DB2 with Kerberos

```json
{
  "SourceEngine": "db2",
  "SourceHost": "db2-main.company.com",
  "SourcePort": "50000",
  "SourceDatabase": "MAINDB",
  "SourceKerberosSPN": "db2/db2-main.company.com@COMPANY.COM",
  "SourceKerberosDomain": "COMPANY.COM",
  "SourceKerberosKDC": "dc1.company.com:88",
  "SourceKerberosKeytabS3Path": "s3://my-bucket/kerberos/db2-replication/db2_svc.keytab",
  
  "TargetEngine": "db2",
  "TargetHost": "db2-backup.company.com",
  "TargetPort": "50000",
  "TargetDatabase": "BACKUPDB",
  "TargetKerberosSPN": "db2/db2-backup.company.com@COMPANY.COM",
  "TargetKerberosDomain": "COMPANY.COM",
  "TargetKerberosKDC": "dc2.company.com:88",
  "TargetKerberosKeytabS3Path": "s3://my-bucket/kerberos/db2-replication/db2_svc.keytab",
  
  "TableList": "SCHEMA1.TABLE1,SCHEMA2.TABLE2"
}
```

## Step-by-Step Configuration

### Step 1: Verify Kerberos Infrastructure

1. **Test KDC Connectivity**:
   ```bash
   # Test from Glue job's network
   telnet dc1.company.com 88
   ```

2. **Verify DNS Resolution**:
   ```bash
   nslookup dc1.company.com
   nslookup database-server.company.com
   ```

3. **Check Time Synchronization**:
   ```bash
   # Ensure time difference is less than 5 minutes
   date
   ```

### Step 2: Configure Database for Kerberos

#### SQL Server Configuration
1. Register SPN in Active Directory:
   ```cmd
   setspn -A MSSQLSvc/hostname.domain.com:1433 DOMAIN\sql-service-account
   ```

2. Configure SQL Server for Kerberos authentication
3. Restart SQL Server service

#### Oracle Configuration
1. Configure Oracle Net Services (sqlnet.ora):
   ```
   SQLNET.AUTHENTICATION_SERVICES = (KERBEROS5)
   SQLNET.KERBEROS5_CONF = /etc/krb5.conf
   ```

2. Register SPN and configure service

**Note**: The AWS Glue Data Replication solution automatically generates the required `krb5.conf` file and sets Java system properties based on your Kerberos parameters. You don't need to manually create these configuration files.

#### PostgreSQL Configuration
1. Configure postgresql.conf:
   ```
   krb_server_keyfile = '/path/to/postgres.keytab'
   ```

2. Configure pg_hba.conf:
   ```
   host all all 0.0.0.0/0 gss
   ```

#### DB2 Configuration
1. Configure DB2 for Kerberos authentication
2. Set up Kerberos security plugin
3. Register appropriate SPNs

### Step 3: Upload Keytab File to S3

Before creating your parameter file, upload the keytab file to S3. The Glue job downloads it at runtime for authentication.

1. **Choose an S3 path** following the convention `s3://<bucket>/kerberos/<job-name>/<username>.keytab`:

   ```bash
   aws s3 cp /path/to/username.keytab s3://my-bucket/kerberos/my-job-name/username.keytab
   ```

2. **Verify the upload**:

   ```bash
   aws s3 ls s3://my-bucket/kerberos/my-job-name/
   ```

3. **Enable encryption** (recommended):

   ```bash
   aws s3 cp /path/to/username.keytab s3://my-bucket/kerberos/my-job-name/username.keytab --sse aws:kms
   ```

4. **Verify IAM permissions**: The Glue job's execution role must have `s3:GetObject` on the keytab path. If the keytab is in the same bucket as your deployment assets, the default IAM policy covers this.

### Step 4: Create Parameter File

1. **Complete Kerberos Configuration**: Ensure all three parameters (SPN, Domain, KDC) are provided for each connection requiring Kerberos
2. **Mixed Authentication**: You can use Kerberos for source and username/password for target, or vice versa
3. **Validation**: The solution will validate parameter formats and engine compatibility

### Step 5: Deploy and Test

1. **Deploy CloudFormation Stack**:
   ```bash
   ./deploy.sh -s my-stack -b my-bucket -p kerberos-parameters.json
   ```

2. **Monitor Glue Job Logs**: Check for Kerberos authentication success/failure messages
3. **Verify Data Replication**: Confirm data is being replicated successfully

## Authentication Flow

### Runtime Kerberos File Generation

At runtime, the Glue job generates `krb5.conf` and `jaas.conf` configuration files based on your Kerberos parameters and uploads them to S3 at `s3://<bucket>/kerberos/<job-name>/` for distribution to Spark executors. The bucket is derived from the JDBC driver S3 path. The IAM policy grants `s3:GetObject` and `s3:PutObject` on the `kerberos/*` prefix to support this.

### Kerberos Authentication Process

1. **Parameter Parsing**: Solution detects complete Kerberos configuration (SPN, Domain, KDC)
2. **Validation**: Validates parameter formats and engine compatibility
3. **Keytab Download**: Downloads the keytab file from S3 (`SourceKerberosKeytabS3Path` or `TargetKerberosKeytabS3Path`) to `/tmp/krb5.keytab` on the Glue worker
4. **Environment Setup**: Automatically generates `krb5.conf` and `jaas.conf` files, and sets Java system properties:
   - `java.security.krb5.conf`: Path to generated krb5.conf
   - `java.security.krb5.realm`: Kerberos realm/domain
   - `java.security.krb5.kdc`: Key Distribution Center
   - `java.security.auth.login.config`: Path to generated jaas.conf
5. **JDBC Connection**: Establishes JDBC connection using Kerberos authentication via the keytab
6. **Data Replication**: Performs data replication with authenticated connection

### Fallback Behavior

- **Incomplete Configuration**: If any Kerberos parameter is missing, falls back to username/password authentication
- **Engine Compatibility**: Validates that only supported engines use Kerberos parameters
- **Error Handling**: Provides detailed error messages for configuration issues

### Connection Strategy with Kerberos

When Kerberos authentication is enabled, the `CreateSourceConnection` and `CreateTargetConnection` deployment parameters are effectively ignored. No Glue JDBC Connection or Secrets Manager secret is created for the Kerberos-authenticated side. Instead, the Glue job establishes a direct JDBC connection using Kerberos credentials (keytab + SPN) configured through the job's default arguments.

Network connections (created when `SourceVpcId`/`TargetVpcId` are set) are still used to provide VPC routing for the Glue job's ENIs.

## Supported Database Engines

| Engine | Kerberos Support | Notes |
|--------|------------------|-------|
| SQL Server | ✅ Yes | Full support with proper SPN configuration |
| Oracle | ✅ Yes | Requires Oracle Net Services configuration |
| PostgreSQL | ✅ Yes | Requires GSSAPI configuration |
| IBM DB2 | ✅ Yes | Requires Kerberos security plugin |
| Apache Iceberg | ❌ No | Uses AWS IAM authentication instead |

## Security Considerations

### Best Practices

1. **Network Security**: Ensure Kerberos traffic (port 88) is encrypted and secured
2. **Time Synchronization**: Maintain accurate time synchronization across all systems
3. **SPN Management**: Regularly audit and manage Service Principal Names
4. **Credential Rotation**: Implement regular rotation of Kerberos credentials
5. **Monitoring**: Monitor authentication attempts and failures

### Limitations

1. **Credential Storage**: Kerberos credentials are not stored in parameter files
2. **Network Dependencies**: Requires network connectivity to KDC during authentication
3. **Time Sensitivity**: Kerberos tickets have limited lifetime and require time synchronization
4. **Engine Support**: Only traditional JDBC databases support Kerberos (not Iceberg)

## Troubleshooting

### Common Issues

#### Authentication Failures
- **Symptom**: "Kerberos authentication failed" errors
- **Solutions**:
  - Verify SPN is correctly registered and matches parameter
  - Check time synchronization between systems
  - Ensure network connectivity to KDC
  - Validate Kerberos realm configuration

#### Cannot Locate Default Realm
- **Symptom**: "GSSException: Invalid name provided (Mechanism level: KrbException: Cannot locate default realm)"
- **Root Cause**: Missing or incorrect Kerberos environment configuration
- **Solutions**:
  - Ensure all three Kerberos parameters (SPN, Domain, KDC) are provided
  - Verify KDC parameter format (hostname or hostname:port)
  - Check that Domain parameter matches your Kerberos realm
  - Confirm network connectivity to the specified KDC
  - **Note**: The solution automatically generates krb5.conf and sets Java properties, so this error typically indicates parameter issues

#### Connection Timeouts
- **Symptom**: Connection timeouts during authentication
- **Solutions**:
  - Check network connectivity to KDC (port 88)
  - Verify DNS resolution for all hostnames
  - Check firewall rules for Kerberos traffic

#### Invalid SPN Format
- **Symptom**: "Invalid Service Principal Name" errors
- **Solutions**:
  - Verify SPN format matches database requirements
  - Check case sensitivity of SPN components
  - Ensure SPN is registered in Active Directory

#### Partial Configuration
- **Symptom**: Warning messages about incomplete Kerberos configuration
- **Solutions**:
  - Provide all three parameters (SPN, Domain, KDC) for complete configuration
  - Remove partial parameters to use standard authentication
  - Check parameter names match exactly (case-sensitive)

### Diagnostic Commands

```bash
# Test Kerberos connectivity
kinit username@REALM.COM
klist

# Test database connectivity with Kerberos
sqlplus /@database_service_name

# Check Glue job logs
aws logs describe-log-groups --log-group-name-prefix "/aws-glue"
```

## Advanced Configuration

### Multiple KDCs
For environments with multiple KDCs, specify the primary KDC in the parameter. The Kerberos client will automatically failover to backup KDCs if configured.

### Cross-Realm Authentication
For cross-realm scenarios, ensure proper trust relationships are established between Kerberos realms.

### Custom Ports
If using non-standard Kerberos ports, specify the port in the KDC parameter:
```
"SourceKerberosKDC": "dc1.company.com:8888"
```

## Support and Resources

### AWS Documentation
- [AWS Glue Connection Properties](https://docs.aws.amazon.com/glue/latest/dg/connection-properties.html)
- [AWS Glue Security](https://docs.aws.amazon.com/glue/latest/dg/security.html)

### Database-Specific Resources
- [SQL Server Kerberos Configuration](https://docs.microsoft.com/en-us/sql/database-engine/configure-windows/register-a-service-principal-name-for-kerberos-connections)
- [Oracle Kerberos Authentication](https://docs.oracle.com/en/database/oracle/oracle-database/19/dbseg/configuring-kerberos-authentication.html)
- [PostgreSQL GSSAPI Authentication](https://www.postgresql.org/docs/current/gssapi-auth.html)
- [DB2 Kerberos Configuration](https://www.ibm.com/docs/en/db2/11.5?topic=authentication-kerberos)