#!/bin/bash

# Script to get RDS network information for Glue job configuration
# Usage: ./get-rds-network-info.sh <rds-instance-identifier>

set -e

if [ $# -eq 0 ]; then
    echo "Usage: $0 <rds-instance-identifier>"
    echo "Example: $0 database-1"
    exit 1
fi

RDS_INSTANCE="$1"

echo "Getting network information for RDS instance: $RDS_INSTANCE"
echo "=================================================="

# Get VPC ID
VPC_ID=$(aws rds describe-db-instances \
  --db-instance-identifier "$RDS_INSTANCE" \
  --query 'DBInstances[0].DBSubnetGroup.VpcId' \
  --output text)
echo "VPC ID: $VPC_ID"

# Get Subnet IDs
SUBNET_IDS=$(aws rds describe-db-instances \
  --db-instance-identifier "$RDS_INSTANCE" \
  --query 'DBInstances[0].DBSubnetGroup.Subnets[*].SubnetIdentifier' \
  --output text | tr '\t' ',')
echo "Subnet IDs: $SUBNET_IDS"

# Get first subnet's availability zone
FIRST_SUBNET=$(echo "$SUBNET_IDS" | cut -d',' -f1)
AZ=$(aws ec2 describe-subnets \
  --subnet-ids "$FIRST_SUBNET" \
  --query 'Subnets[0].AvailabilityZone' \
  --output text)
echo "Availability Zone: $AZ"

# Get Security Group IDs
SECURITY_GROUP_IDS=$(aws rds describe-db-instances \
  --db-instance-identifier "$RDS_INSTANCE" \
  --query 'DBInstances[0].VpcSecurityGroups[*].VpcSecurityGroupId' \
  --output text | tr '\t' ',')
echo "Security Group IDs: $SECURITY_GROUP_IDS"

echo ""
echo "=================================================="
echo "Parameters for your deployment file:"
echo "=================================================="
echo "\"SourceVpcId\": \"$VPC_ID\","
echo "\"SourceSubnetIds\": \"$SUBNET_IDS\","
echo "\"SourceSecurityGroupIds\": \"$SECURITY_GROUP_IDS\","
echo "\"SourceAvailabilityZone\": \"$AZ\""
echo ""
echo "If source and target share the same VPC, also use:"
echo "\"TargetVpcId\": \"$VPC_ID\","
echo "\"TargetSubnetIds\": \"$SUBNET_IDS\","
echo "\"TargetSecurityGroupIds\": \"$SECURITY_GROUP_IDS\","
echo "\"TargetAvailabilityZone\": \"$AZ\""
