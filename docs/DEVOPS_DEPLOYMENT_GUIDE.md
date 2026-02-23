# DevOps Deployment Guide

This guide provides detailed instructions for DevOps engineers to deploy and manage the AWS Glue Data Replication system using Infrastructure as Code principles.

## Prerequisites

### AWS Account Requirements

- AWS account with appropriate service limits for Glue jobs
- AWS CLI configured with deployment credentials
- CloudFormation service enabled in target regions
- S3 bucket for storing JDBC drivers and artifacts

### Required Tools

- AWS CLI v2.0 or later
- Git for version control
- Text editor for parameter file customization
- (Optional) Terraform >= 1.0 for Terraform-based deployments
- (Optional) AWS CloudFormation CLI for advanced template validation

## IAM Permissions

### DevOps Engineer Permissions

DevOps engineers need specific IAM permissions to deploy the CloudFormation stack. Apply the policy from `infrastructure/iam/devops-policy.json` to your deployment user or role.

#### Policy Overview

The DevOps policy includes permissions for:

1. **CloudFormation Operations**
   - Stack creation, updates, and deletion
   - Template validation and resource description
   - Stack event monitoring

2. **IAM Management**
   - Role and policy creation for Glue jobs
   - Role assumption permissions
   - Policy attachment and detachment

3. **Glue Service Management**
   - Job creation, updates, and deletion
   - Job configuration and monitoring

4. **S3 Access**
   - JDBC driver storage and retrieval
   - Glue asset management

#### Applying the DevOps Policy

**Option 1: Attach to existing IAM user**
```bash
aws iam put-user-policy \
  --user-name your-devops-user \
  --policy-name GlueDataReplicationDeployment \
  --policy-document file://infrastructure/iam/devops-policy.json
```

**Option 2: Create dedicated deployment role**
```bash
# Create the role
aws iam create-role \
  --role-name GlueDataReplicationDeploymentRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "AWS": "arn:aws:iam::ACCOUNT-ID:user/your-user"
        },
        "Action": "sts:AssumeRole"
      }
    ]
  }'

# Attach the policy
aws iam put-role-policy \
  --role-name GlueDataReplicationDeploymentRole \
  --policy-name GlueDataReplicationDeployment \
  --policy-document file://infrastructure/iam/devops-policy.json
```

### Service-Linked Roles

The CloudFormation template automatically creates the necessary service roles for Glue job execution. No additional service-linked role configuration is required.

## Pre-Deployment Setup

### 1. JDBC Driver Preparation

Before deploying the stack, ensure JDBC drivers are available in S3:

```bash
# Create S3 bucket for drivers (if not exists)
aws s3 mb s3://your-glue-assets-bucket

# Upload modular Glue job structure
aws s3 sync src/glue_job/ s3://your-glue-assets-bucket/src/glue_job/ --delete

# Upload JDBC drivers using recommended structure
aws s3 cp ojdbc11.jar s3://your-glue-assets-bucket/jdbc-drivers/oracle/21.7.0.0/ojdbc11.jar
aws s3 cp mssql-jdbc-12.2.0.jre11.jar s3://your-glue-assets-bucket/jdbc-drivers/sqlserver/12.2.0.jre11/mssql-jdbc-12.2.0.jre11.jar
aws s3 cp postgresql-42.6.0.jar s3://your-glue-assets-bucket/jdbc-drivers/postgresql/42.6.0/postgresql-42.6.0.jar
aws s3 cp db2jcc4.jar s3://your-glue-assets-bucket/jdbc-drivers/db2/11.5.8.0/db2jcc4.jar
```

### 2. Network Configuration

The system supports multiple network connectivity scenarios. Choose the appropriate configuration based on your database locations:

#### Same VPC (Default)
- No additional network configuration required
- Both databases accessible from Glue execution environment
- Simplest deployment option

#### Cross-VPC Configuration
- Required when databases are in different VPCs
- Requires additional CloudFormation parameters
- See [Network Configuration Guide](NETWORK_CONFIGURATION_GUIDE.md) for detailed setup

**For On-Premises Databases:**
- Configure VPN or Direct Connect to AWS
- Set up cross-VPC connectivity if database VPC differs from Glue VPC
- Configure appropriate security groups for database access
- Verify DNS resolution across network boundaries

**For RDS/Aurora Databases:**
- Identify VPC location of each database
- Configure cross-VPC parameters if databases are in different VPCs
- Set up security groups to allow Glue ENI access
- Configure S3 Interface VPC endpoints if databases are in private subnets (endpoints use subnets and security groups, no route tables needed)
- Verify subnet routing and availability zones

**Network Parameter Planning:**

Before deployment, gather the following information for each database in a different VPC:

| Information Required | Example | Purpose |
|---------------------|---------|----------|
| VPC ID | `vpc-12345678` | Identifies target VPC |
| Subnet IDs | `subnet-123,subnet-456` | ENI placement locations |
| Security Group IDs | `sg-database123` | Network access control |
| Subnet Type | Private/Public | Determines S3 VPC endpoint need |
| Availability Zones | `us-east-1a,us-east-1b` | High availability planning |

### 3. Database Preparation

**Source Database:**
- Verify user has read permissions on required tables
- Test JDBC connectivity from a similar environment
- Ensure database is accessible during planned migration windows

**Target Database:**
- Create target schema and tables (schema migration assumed complete)
- Verify user has write permissions
- Test JDBC connectivity
- Ensure sufficient storage space

## Deployment Process

### Step 1: Template Validation

Always validate the CloudFormation template before deployment:

```bash
aws cloudformation validate-template \
  --template-body file://infrastructure/cloudformation/glue-data-replication.yaml
```

### Step 2: Parameter File Creation

Create a parameter file based on your environment. Parameter files use the flat JSON format (`.tfvars.json`). For the full list of available parameters, see [Parameter Reference](PARAMETER_REFERENCE.md). For ready-to-use templates, copy one of the files from the `examples/` directory and customize it:

| Example File | Scenario |
|-------------|----------|
| [`oracle-to-oracle-parameters.tfvars.json`](../examples/oracle-to-oracle-parameters.tfvars.json) | Oracle to Oracle with cross-VPC |
| [`sqlserver-to-sqlserver-parameters.tfvars.json`](../examples/sqlserver-to-sqlserver-parameters.tfvars.json) | SQL Server to SQL Server with Glue Connections |
| [`sqlserver-to-iceberg-parameters.tfvars.json`](../examples/sqlserver-to-iceberg-parameters.tfvars.json) | SQL Server to Iceberg data lake ingestion |
| [`sqlserver-to-sqlserver-kerberos-parameters.tfvars.json`](../examples/sqlserver-to-sqlserver-kerberos-parameters.tfvars.json) | SQL Server with Kerberos authentication |
| [`iceberg-to-iceberg-parameters.tfvars.json`](../examples/iceberg-to-iceberg-parameters.tfvars.json) | Iceberg to Iceberg replication |
| [`iceberg-cross-account-source-parameters.tfvars.json`](../examples/iceberg-cross-account-source-parameters.tfvars.json) | Iceberg cross-account (source side) |
| [`iceberg-cross-account-target-parameters.tfvars.json`](../examples/iceberg-cross-account-target-parameters.tfvars.json) | Iceberg cross-account (target side) |
| [`iceberg-multi-region-parameters.tfvars.json`](../examples/iceberg-multi-region-parameters.tfvars.json) | Iceberg multi-region |

#### Same VPC Configuration

When both databases are in the same VPC (the most common production setup), you still need to provide the source VPC, subnet, and security group parameters so that Glue can place ENIs in the VPC and reach the databases. Databases in private subnets are the expected configuration — publicly accessible databases are an anti-pattern and should be avoided.

```json
{
  "JobName": "prod-oracle-to-postgres",
  "SourceEngineType": "oracle",
  "TargetEngineType": "postgresql",
  "SourceDatabase": "PRODDB",
  "TargetDatabase": "analytics_db",
  "SourceSchema": "SALES",
  "TargetSchema": "public",
  "TableNames": "customers,orders,order_items,products",
  "SourceDbUser": "replication_user",
  "SourceDbPassword": "your-secure-source-password",
  "TargetDbUser": "postgres",
  "TargetDbPassword": "your-secure-target-password",
  "SourceConnectionString": "jdbc:oracle:thin:@prod-oracle.company.com:1521:PRODDB",
  "TargetConnectionString": "jdbc:postgresql://analytics-postgres.company.com:5432/analytics_db",
  "SourceJdbcDriverS3Path": "s3://company-glue-assets/jdbc-drivers/oracle/21.7.0.0/ojdbc11.jar",
  "TargetJdbcDriverS3Path": "s3://company-glue-assets/jdbc-drivers/postgresql/42.6.0/postgresql-42.6.0.jar",
  "SourceVpcId": "vpc-shared-databases",
  "SourceSubnetIds": "subnet-private-1a,subnet-private-1b",
  "SourceSecurityGroupIds": "sg-glue-database-access",
  "SourceAvailabilityZone": "us-east-1a",
  "CreateSourceS3VpcEndpoint": "YES"
}
```

Since both databases share the same VPC, only the source-side network parameters are needed — the Glue Connection created in that VPC can reach both databases. Set `CreateSourceS3VpcEndpoint` to `YES` if the subnets are private (no internet gateway route), so the Glue job can access S3 for drivers and bookmarks.

#### Cross-VPC Configuration

When databases are in different VPCs, add both source and target network parameters. See [`sqlserver-to-iceberg-parameters.tfvars.json`](../examples/sqlserver-to-iceberg-parameters.tfvars.json) for a working example with cross-VPC source configuration, or [`oracle-to-oracle-parameters.tfvars.json`](../examples/oracle-to-oracle-parameters.tfvars.json) for a dual-VPC setup.

The full set of network parameters is documented in the [Network Configuration](PARAMETER_REFERENCE.md#network-configuration) section of the Parameter Reference.

#### CloudFormation Array Format (Legacy)

If your pipeline requires the legacy CloudFormation array format (`[{"ParameterKey": "...", "ParameterValue": "..."}]`), convert from flat JSON using the included script:

```bash
python3 infrastructure/scripts/convert_params.py --to-cfn my-params.tfvars.json my-cfn-params.json
```

#### Network Parameter Guidelines

**When to include network parameters:**
- Source database is in a different VPC than Glue execution environment
- Target database is in a different VPC than Glue execution environment
- Databases are in private subnets requiring S3 VPC endpoints

**Parameter selection criteria:**

| Parameter | Required When | Example Value |
|-----------|---------------|---------------|
| `SourceVpcId` | Source DB in different VPC | `vpc-12345678` |
| `SourceSubnetIds` | Source DB in different VPC | `subnet-123,subnet-456` |
| `SourceSecurityGroupIds` | Source DB in different VPC | `sg-database-access` |
| `CreateSourceS3VpcEndpoint` | Source DB in private subnet | `YES` or `NO` |
| `TargetVpcId` | Target DB in different VPC | `vpc-87654321` |
| `TargetSubnetIds` | Target DB in different VPC | `subnet-789,subnet-012` |
| `TargetSecurityGroupIds` | Target DB in different VPC | `sg-database-access` |
| `CreateTargetS3VpcEndpoint` | Target DB in private subnet | `YES` or `NO` |

### Step 3: Stack Deployment

Deploy the CloudFormation stack:

```bash
# Create new stack
aws cloudformation create-stack \
  --stack-name glue-data-replication-prod \
  --template-body file://infrastructure/cloudformation/glue-data-replication.yaml \
  --parameters file://parameters/prod-parameters.json \
  --capabilities CAPABILITY_NAMED_IAM \
  --tags Key=Environment,Value=Production Key=Project,Value=DataReplication

# Monitor deployment progress
aws cloudformation describe-stack-events \
  --stack-name glue-data-replication-prod \
  --query 'StackEvents[?ResourceStatus!=`CREATE_COMPLETE`]'
```

### Step 4: Deployment Verification

Verify the deployment was successful:

```bash
# Check stack status
aws cloudformation describe-stacks \
  --stack-name glue-data-replication-prod \
  --query 'Stacks[0].StackStatus'

# List created resources
aws cloudformation list-stack-resources \
  --stack-name glue-data-replication-prod

# Verify Glue job creation
aws glue get-job --job-name prod-oracle-to-postgres
```

## Post-Deployment Configuration

### 1. Initial Job Execution

Run the job for the first time to perform full-load migration:

```bash
# Start job run
JOB_RUN_ID=$(aws glue start-job-run \
  --job-name prod-oracle-to-postgres \
  --query 'JobRunId' \
  --output text)

echo "Job run started with ID: $JOB_RUN_ID"

# Monitor job progress
aws glue get-job-run \
  --job-name prod-oracle-to-postgres \
  --run-id $JOB_RUN_ID \
  --query 'JobRun.JobRunState'
```

### 2. Schedule Configuration

Set up job scheduling using AWS Glue triggers:

```bash
# Create daily trigger
aws glue create-trigger \
  --name prod-oracle-to-postgres-daily \
  --type SCHEDULED \
  --schedule "cron(0 2 * * ? *)" \
  --actions JobName=prod-oracle-to-postgres \
  --start-on-creation
```

### 3. Monitoring Setup

Configure CloudWatch alarms for job monitoring:

```bash
# Create alarm for job failures
aws cloudwatch put-metric-alarm \
  --alarm-name "GlueJob-prod-oracle-to-postgres-Failures" \
  --alarm-description "Alert on Glue job failures" \
  --metric-name "glue.driver.aggregate.numFailedTasks" \
  --namespace "AWS/Glue" \
  --statistic Sum \
  --period 300 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --dimensions Name=JobName,Value=prod-oracle-to-postgres \
  --evaluation-periods 1 \
  --alarm-actions arn:aws:sns:region:account:alert-topic
```

## Environment Management

### Development Environment

For development and testing:

```bash
# Use smaller instance types and reduced monitoring
aws cloudformation create-stack \
  --stack-name glue-data-replication-dev \
  --template-body file://infrastructure/cloudformation/glue-data-replication.yaml \
  --parameters file://parameters/dev-parameters.json \
  --capabilities CAPABILITY_NAMED_IAM \
  --tags Key=Environment,Value=Development
```

### Production Environment

For production deployments:

- Use dedicated VPC and subnets
- Enable detailed CloudWatch monitoring
- Configure backup and disaster recovery
- Implement proper change management processes

### Multi-Region Deployment

For multi-region setups:

```bash
# Deploy to multiple regions
for region in us-east-1 us-west-2 eu-west-1; do
  aws cloudformation create-stack \
    --region $region \
    --stack-name glue-data-replication-prod-$region \
    --template-body file://infrastructure/cloudformation/glue-data-replication.yaml \
    --parameters file://parameters/prod-$region-parameters.json \
    --capabilities CAPABILITY_NAMED_IAM
done
```

## Maintenance and Updates

### Stack Updates

Update existing stacks when configuration changes:

```bash
# Update stack with new parameters
aws cloudformation update-stack \
  --stack-name glue-data-replication-prod \
  --template-body file://infrastructure/cloudformation/glue-data-replication.yaml \
  --parameters file://parameters/prod-parameters-updated.json \
  --capabilities CAPABILITY_NAMED_IAM
```

### JDBC Driver Updates

Update JDBC drivers by uploading new versions to S3 and updating stack parameters:

```bash
# Upload new driver version
aws s3 cp ojdbc11-new-version.jar s3://company-glue-assets/jdbc-drivers/oracle/21.8.0.0/ojdbc11.jar

# Update stack with new driver path
# (Update parameters file with new S3 path and run stack update)
```

### Job Code Updates

Update the PySpark job code by repackaging and uploading the modules:

```bash
# Repackage modules with latest code changes
./infrastructure/scripts/package-glue-modules.sh --clean

# Upload updated main script and modules zip
aws s3 cp dist/main.py s3://company-glue-assets/glue-scripts/main.py
aws s3 cp dist/glue-job-modules.zip s3://company-glue-assets/glue-modules/glue-job-modules.zip
```

Or use the upload script which handles packaging and upload in one step:

```bash
./infrastructure/scripts/upload-assets.sh company-glue-assets
```

## Troubleshooting

### Common Deployment Issues

1. **IAM Permission Errors**
   ```
   Error: User is not authorized to perform: iam:CreateRole
   ```
   - Verify DevOps policy is correctly applied
   - Check policy resource ARNs match your account

2. **CloudFormation Template Errors**
   ```
   Error: Template format error
   ```
   - Validate template syntax
   - Check parameter constraints

3. **Resource Naming Conflicts**
   ```
   Error: Role already exists
   ```
   - Use unique job names
   - Clean up previous failed deployments

### Network Connectivity Troubleshooting

#### Pre-Deployment Network Validation

Before deploying cross-VPC configurations, validate network connectivity:

```bash
# Test database connectivity from EC2 instance in target subnet
# 1. Launch test EC2 instance
aws ec2 run-instances \
  --image-id ami-12345678 \
  --instance-type t3.micro \
  --subnet-id subnet-target-123 \
  --security-group-ids sg-test-connectivity

# 2. Test database connection
ssh ec2-user@test-instance
telnet oracle-host 1521
telnet postgres-host 5432

# 3. Test S3 connectivity (if using VPC endpoint)
aws s3 ls s3://your-jdbc-bucket/
```

#### Common Network Issues

**1. Glue Connection Creation Failures**

```bash
# Verify VPC and subnet configuration
aws ec2 describe-vpcs --vpc-ids vpc-12345678
aws ec2 describe-subnets --subnet-ids subnet-12345678

# Check security group rules
aws ec2 describe-security-groups --group-ids sg-12345678

# Validate availability zones
aws ec2 describe-availability-zones --zone-names us-east-1a
```

**2. ENI Creation Issues**

```bash
# Check available IP addresses in subnet
aws ec2 describe-subnets --subnet-ids subnet-12345678 \
  --query 'Subnets[0].AvailableIpAddressCount'

# Check service limits
aws service-quotas get-service-quota \
  --service-code ec2 \
  --quota-code L-DF5E4CA3

# List existing ENIs in subnet
aws ec2 describe-network-interfaces \
  --filters "Name=subnet-id,Values=subnet-12345678"
```

**3. S3 VPC Endpoint Issues**

S3 VPC endpoints use Interface (PrivateLink) type, which places ENIs in the specified subnets and associates them with the specified security groups. Troubleshoot connectivity by verifying the endpoint ENIs and security group rules:

```bash
# Check existing VPC endpoints
aws ec2 describe-vpc-endpoints \
  --filters "Name=vpc-id,Values=vpc-12345678"

# Verify the S3 Interface endpoint has ENIs in the expected subnets
aws ec2 describe-vpc-endpoints \
  --filters "Name=vpc-id,Values=vpc-12345678" "Name=service-name,Values=com.amazonaws.us-east-1.s3" \
  --query 'VpcEndpoints[0].{Type:VpcEndpointType,Subnets:SubnetIds,SecurityGroups:Groups,State:State}'

# Verify security groups on the S3 endpoint allow HTTPS (port 443) traffic
aws ec2 describe-security-groups --group-ids sg-s3-endpoint \
  --query 'SecurityGroups[0].IpPermissions'

# Test S3 access from private subnet
# (Run from EC2 instance in private subnet)
curl -I https://s3.amazonaws.com/your-bucket/test-file
```

**4. Security Group Configuration**

```bash
# Create security group for database access
aws ec2 create-security-group \
  --group-name glue-database-access \
  --description "Allow Glue access to database" \
  --vpc-id vpc-12345678

# Add inbound rule for database port
aws ec2 authorize-security-group-ingress \
  --group-id sg-database \
  --protocol tcp \
  --port 1521 \
  --source-group sg-glue-eni

# Verify security group rules
aws ec2 describe-security-groups \
  --group-ids sg-database \
  --query 'SecurityGroups[0].IpPermissions'
```

#### Network Monitoring and Diagnostics

```bash
# Enable VPC Flow Logs for network troubleshooting
aws ec2 create-flow-logs \
  --resource-type VPC \
  --resource-ids vpc-12345678 \
  --traffic-type ALL \
  --log-destination-type cloud-watch-logs \
  --log-group-name VPCFlowLogs

# Monitor Glue connection usage
aws glue get-connection --name source-connection
aws glue get-connection --name target-connection

# Check CloudWatch logs for network errors
aws logs filter-log-events \
  --log-group-name /aws-glue/jobs/output \
  --filter-pattern "Connection" \
  --start-time $(date -d '1 hour ago' +%s)000
```

### Monitoring and Alerting

Set up comprehensive monitoring:

```bash
# Create dashboard for job monitoring
aws cloudwatch put-dashboard \
  --dashboard-name GlueDataReplication \
  --dashboard-body file://monitoring/dashboard.json

# Set up log group retention
aws logs put-retention-policy \
  --log-group-name /aws-glue/jobs/output \
  --retention-in-days 30
```

### Backup and Recovery

Implement backup strategies:

- **Configuration Backup**: Store parameter files in version control
- **State Backup**: Regular snapshots of job bookmark data (see [Bookmark Details](BOOKMARK_DETAILS.md) for S3 storage details)
- **Code Backup**: Version control for all scripts and templates

## Security Best Practices

### Credential Management

- Use AWS Secrets Manager for database passwords
- Rotate credentials regularly
- Implement least-privilege access

### Network Security

- Use Interface VPC endpoints for private S3 access (placed in subnets with security group controls)
- Implement security groups with minimal required access
- Enable VPC Flow Logs for network monitoring

### Audit and Compliance

- Enable CloudTrail for API call logging
- Use AWS Config for resource compliance monitoring
- Implement regular security assessments

## Cost Optimization

### Resource Sizing

- Monitor Glue job resource utilization
- Adjust worker types and counts based on workload
- Use spot instances where appropriate

### Scheduling Optimization

- Schedule jobs during off-peak hours
- Implement job dependencies to avoid resource conflicts
- Use incremental loading to reduce processing time

### Storage Optimization

- Implement S3 lifecycle policies for JDBC drivers
- Use appropriate S3 storage classes
- Clean up old job logs and artifacts

## Support and Escalation

### Internal Support Process

1. Check CloudWatch logs for detailed error information
2. Review this deployment guide for common issues
3. Consult AWS Glue documentation for service-specific problems
4. Escalate to AWS Support for infrastructure issues

### Documentation Updates

Keep this guide updated with:
- New deployment patterns
- Lessons learned from production deployments
- Updated IAM policies and permissions
- New troubleshooting scenarios

For questions or improvements to this guide, contact the DevOps team or submit a pull request to the repository.

## Manual Deployment Without deploy.sh

Enterprise CI/CD pipelines often need to invoke CloudFormation or Terraform directly rather than relying on wrapper scripts. This section documents the manual steps for both deployment methods.

Both paths share the same prerequisite: packaging and uploading Glue job assets to S3.

### Common Step: Package and Upload Assets

The Glue job code must be packaged and uploaded to S3 before deploying infrastructure. The S3 paths must then be referenced in your parameter file.

```bash
# 1. Package the Glue job modules
./infrastructure/scripts/package-glue-modules.sh --clean

# This produces:
#   dist/main.py                  — Glue job entry point
#   dist/glue-job-modules.zip     — supporting Python modules

# 2. Upload to S3
BUCKET_NAME="your-glue-assets-bucket"

aws s3 cp dist/main.py "s3://${BUCKET_NAME}/glue-scripts/main.py"
aws s3 cp dist/glue-job-modules.zip "s3://${BUCKET_NAME}/glue-modules/glue-job-modules.zip"

# 3. For CloudFormation: upload the template (required — template exceeds 51,200 char inline limit)
aws s3 cp infrastructure/cloudformation/glue-data-replication.yaml \
  "s3://${BUCKET_NAME}/cloudformation/glue-data-replication.yaml"
```

Alternatively, `upload-assets.sh` handles steps 1 and 2 in one command and can optionally upload JDBC drivers:

```bash
./infrastructure/scripts/upload-assets.sh ${BUCKET_NAME}
./infrastructure/scripts/upload-assets.sh ${BUCKET_NAME} --include-drivers
```

### Parameter File Format

Both deployment methods use the same parameter files from the `examples/` directory. The canonical format is flat JSON (`.tfvars.json`):

```json
{
  "JobName": "my-replication-job",
  "SourceEngineType": "sqlserver",
  "TargetEngineType": "sqlserver",
  "GlueJobScriptS3Path": "s3://your-bucket/glue-scripts/main.py",
  "GlueJobModulesS3Path": "s3://your-bucket/glue-modules/glue-job-modules.zip",
  ...
}
```

Terraform consumes this format directly. CloudFormation requires the array format (`[{"ParameterKey": "...", "ParameterValue": "..."}]`). Use the included converter:

```bash
python3 infrastructure/scripts/convert_params.py --to-cfn my-params.tfvars.json my-cfn-params.json
```

Make sure `GlueJobScriptS3Path` and `GlueJobModulesS3Path` in your parameter file point to the S3 locations where you uploaded the assets.

### Manual CloudFormation Deployment

The CloudFormation template is a static file at `infrastructure/cloudformation/glue-data-replication.yaml`. No code generation is needed — all configuration is driven by parameters.

```bash
BUCKET_NAME="your-glue-assets-bucket"
STACK_NAME="my-replication-stack"
REGION="us-east-1"

# Convert parameter file to CFN format (if starting from flat JSON)
python3 infrastructure/scripts/convert_params.py \
  --to-cfn my-params.tfvars.json my-cfn-params.json

# Create stack using S3-hosted template
aws cloudformation create-stack \
  --stack-name ${STACK_NAME} \
  --template-url "https://s3.amazonaws.com/${BUCKET_NAME}/cloudformation/glue-data-replication.yaml" \
  --parameters file://my-cfn-params.json \
  --capabilities CAPABILITY_NAMED_IAM \
  --region ${REGION} \
  --tags Key=Environment,Value=Production

# Wait for completion
aws cloudformation wait stack-create-complete \
  --stack-name ${STACK_NAME} --region ${REGION}

# Update an existing stack
aws cloudformation update-stack \
  --stack-name ${STACK_NAME} \
  --template-url "https://s3.amazonaws.com/${BUCKET_NAME}/cloudformation/glue-data-replication.yaml" \
  --parameters file://my-cfn-params.json \
  --capabilities CAPABILITY_NAMED_IAM \
  --region ${REGION}

# Destroy
aws cloudformation delete-stack \
  --stack-name ${STACK_NAME} --region ${REGION}
```

For non-us-east-1 regions, use the regional S3 URL format: `https://s3-${REGION}.amazonaws.com/${BUCKET_NAME}/...`

### Manual Terraform Deployment

The Terraform module lives in `infrastructure/terraform/` with three files: `variables.tf`, `main.tf`, and `outputs.tf`. Variable names match the CloudFormation parameter names exactly, so the same flat JSON parameter files work for both.

```bash
BUCKET_NAME="your-glue-assets-bucket"
PARAMS_FILE="../../my-params.tfvars.json"  # relative to infrastructure/terraform/

cd infrastructure/terraform

# Initialize (downloads AWS provider)
terraform init -input=false

# Optional: use workspaces to isolate multiple deployments
terraform workspace new my-replication-job
# or: terraform workspace select my-replication-job

# Plan
terraform plan \
  -var-file=${PARAMS_FILE} \
  -input=false \
  -out=my-replication-job.tfplan

# Apply
terraform apply -input=false my-replication-job.tfplan

# View outputs
terraform output

# Destroy
terraform destroy \
  -var-file=${PARAMS_FILE} \
  -auto-approve
```

For CI/CD pipelines that manage multiple deployments, use Terraform workspaces to isolate state per deployment. The `deploy.sh` script uses `JobName` from the parameter file as the workspace name by convention, but you can use any naming scheme.

### What deploy.sh Does (For Reference)

The `deploy.sh` script automates the manual steps above into a single command. Understanding what it does helps when integrating into existing pipelines:

1. Validates AWS credentials and bucket name format
2. Creates the S3 bucket if it doesn't exist
3. Calls `package-glue-modules.sh` to build `dist/main.py` and `dist/glue-job-modules.zip`
4. Uploads assets to S3 (`glue-scripts/main.py`, `glue-modules/glue-job-modules.zip`, and the CFN template)
5. Auto-detects parameter file format and converts if needed (flat JSON ↔ CFN array)
6. Injects `GlueJobScriptS3Path` and `GlueJobModulesS3Path` into the parameter file based on the `--bucket` argument
7. Deploys via `aws cloudformation create-stack` or `terraform apply` depending on `--type`

Enterprise pipelines can replicate any subset of these steps. The infrastructure code (CFN template and Terraform module) is static and requires no generation — only the parameter file needs to be prepared with the correct S3 paths.

## Terraform Deployment for CI/CD

The solution also supports Terraform-based deployments, which may be preferred in environments that standardize on Terraform for infrastructure management.

### Terraform CI/CD Pipeline Steps

```bash
# 1. Package and upload Glue assets to S3
./infrastructure/scripts/package-glue-modules.sh --clean
aws s3 cp dist/main.py s3://${BUCKET_NAME}/glue-scripts/main.py
aws s3 cp dist/glue-job-modules.zip s3://${BUCKET_NAME}/glue-modules/glue-job-modules.zip

# 2. Initialize Terraform
terraform -chdir=infrastructure/terraform init -input=false

# 3. Plan (review in CI before apply)
terraform -chdir=infrastructure/terraform plan \
  -var-file=${PARAMS_FILE} \
  -input=false \
  -out=tfplan

# 4. Apply (automated, no prompt)
terraform -chdir=infrastructure/terraform apply -input=false tfplan
```

Or use the deploy script for a single-command workflow:

```bash
./deploy.sh -b ${BUCKET_NAME} -p ${PARAMS_FILE} --type terraform
```

### Remote State Backend

For team CI/CD environments, configure an S3 backend for Terraform state. Create `infrastructure/terraform/backend.tf`:

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

Then initialize with state migration:

```bash
terraform -chdir=infrastructure/terraform init -migrate-state
```

### GitLab CI Example (Terraform)

```yaml
terraform-deploy:
  stage: deploy
  image: hashicorp/terraform:latest
  before_script:
    - pip install awscli
  script:
    - ./deploy.sh -b ${BUCKET_NAME} -p ${PARAMS_FILE} --type terraform
  only:
    - main
```

For the full Terraform deployment guide, see [Terraform Deployment Guide](TERRAFORM_DEPLOYMENT_GUIDE.md).