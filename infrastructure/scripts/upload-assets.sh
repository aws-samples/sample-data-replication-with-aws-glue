#!/bin/bash

# AWS Glue Data Replication - Asset Upload Script
# This script packages and uploads the Glue job modules and optionally JDBC drivers to S3.
# It mirrors the same S3 structure that deploy.sh uses:
#   - glue-scripts/main.py                      (Glue job entry point)
#   - glue-modules/glue-job-modules.zip          (supporting modules)
#   - jdbc-drivers/<engine>/<version>/<jar>       (JDBC drivers, optional)

set -e

# Configuration
BUCKET_NAME=""
SRC_DIR="src"
MAIN_SCRIPT="src/glue_job/main.py"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

show_usage() {
    cat << EOF
AWS Glue Data Replication - Asset Upload Script

Usage: $0 <s3-bucket-name> [options]

Required:
    s3-bucket-name          S3 bucket name for asset storage

Options:
    --include-drivers       Upload JDBC drivers to S3 (searches current dir and jdbc-drivers/ folder)
    --driver-dir DIR        Directory containing JDBC driver JAR files (default: current dir + jdbc-drivers/)
    --dry-run               Show what would be uploaded without executing
    -h, --help              Show this help message

Examples:
    $0 my-glue-assets-bucket
    $0 my-glue-assets-bucket --include-drivers
    $0 my-glue-assets-bucket --include-drivers --driver-dir /path/to/drivers
    $0 my-glue-assets-bucket --dry-run

This script packages and uploads:
    - glue-scripts/main.py                       (Glue job entry point)
    - glue-modules/glue-job-modules.zip           (supporting Python modules)
    - jdbc-drivers/<engine>/<version>/<jar>        (JDBC drivers, with --include-drivers)

These S3 paths match what deploy.sh produces, so assets uploaded by this script
are fully compatible with the deployed Glue job.
EOF
}

# Parse command line arguments
INCLUDE_DRIVERS=false
DRY_RUN=false
DRIVER_DIR=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --include-drivers)
            INCLUDE_DRIVERS=true
            shift
            ;;
        --driver-dir)
            DRIVER_DIR="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        -*)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
        *)
            if [[ -z "$BUCKET_NAME" ]]; then
                BUCKET_NAME="$1"
            else
                print_error "Too many arguments"
                show_usage
                exit 1
            fi
            shift
            ;;
    esac
done

# Validate bucket name
if [[ -z "$BUCKET_NAME" ]]; then
    print_error "S3 bucket name is required"
    show_usage
    exit 1
fi

if [[ ! $BUCKET_NAME =~ ^[a-z0-9][a-z0-9.-]*[a-z0-9]$ ]]; then
    print_error "Invalid S3 bucket name format"
    exit 1
fi

# Check project structure
if [[ ! -d "$SRC_DIR/glue_job" ]]; then
    print_error "Source directory not found: $SRC_DIR/glue_job"
    echo "Please run this script from the project root directory."
    exit 1
fi

if [[ ! -f "$MAIN_SCRIPT" ]]; then
    print_error "Main Glue script not found: $MAIN_SCRIPT"
    exit 1
fi

print_info "Starting asset upload to S3 bucket: $BUCKET_NAME"

# ============================================================
# Dry run mode
# ============================================================
if [[ "$DRY_RUN" == true ]]; then
    print_info "DRY RUN MODE - Would upload the following:"
    echo ""
    echo "  Glue job script:   s3://$BUCKET_NAME/glue-scripts/main.py"
    echo "  Glue job modules:  s3://$BUCKET_NAME/glue-modules/glue-job-modules.zip"

    if [[ "$INCLUDE_DRIVERS" == true ]]; then
        echo ""
        print_info "JDBC drivers (if found):"
        _found=false
        for search_dir in "${DRIVER_DIR:-.}" "jdbc-drivers"; do
            if [[ -d "$search_dir" ]]; then
                while IFS= read -r jar; do
                    echo "  $jar -> s3://$BUCKET_NAME/jdbc-drivers/..."
                    _found=true
                done < <(find "$search_dir" -name "*.jar" -type f 2>/dev/null)
            fi
        done
        if [[ "$_found" == false ]]; then
            print_warning "No JAR files found. See --driver-dir option."
        fi
    fi

    echo ""
    print_success "Dry run completed"
    exit 0
fi

# ============================================================
# Validate AWS CLI
# ============================================================
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    print_error "AWS CLI is not configured or credentials are invalid"
    echo "Please run 'aws configure' to set up your credentials."
    exit 1
fi

# ============================================================
# Check / create bucket
# ============================================================
print_info "Checking if S3 bucket exists..."
if aws s3 ls "s3://$BUCKET_NAME" > /dev/null 2>&1; then
    print_success "S3 bucket exists: $BUCKET_NAME"
else
    print_info "Bucket doesn't exist. Creating: $BUCKET_NAME"
    AWS_REGION=$(aws configure get region)
    if [[ -z "$AWS_REGION" ]]; then
        AWS_REGION="us-east-1"
        print_info "No region configured, using default: $AWS_REGION"
    fi

    if [[ "$AWS_REGION" = "us-east-1" ]]; then
        aws s3 mb "s3://$BUCKET_NAME"
    else
        aws s3 mb "s3://$BUCKET_NAME" --region "$AWS_REGION"
    fi
    print_success "Created S3 bucket: $BUCKET_NAME"
fi

# ============================================================
# Package Glue job modules (same as deploy.sh)
# ============================================================
print_info "Packaging Glue job modules..."

if [[ -x "infrastructure/scripts/package-glue-modules.sh" ]]; then
    ./infrastructure/scripts/package-glue-modules.sh --clean
else
    print_warning "Packaging script not found, using fallback method"
    rm -rf dist
    mkdir -p dist
    CURRENT_DIR="$(pwd)"
    cd src && zip -r "$CURRENT_DIR/dist/glue-job-modules.zip" glue_job/ -x "glue_job/main.py" && cd ..
    cp src/glue_job/main.py dist/
fi

print_success "Packaged Glue job modules"

# ============================================================
# Upload main script
# ============================================================
print_info "Uploading Glue job main script..."
aws s3 cp "dist/main.py" "s3://$BUCKET_NAME/glue-scripts/main.py"
print_success "Uploaded: s3://$BUCKET_NAME/glue-scripts/main.py"

# ============================================================
# Upload modules zip
# ============================================================
print_info "Uploading Glue job modules zip..."
aws s3 cp "dist/glue-job-modules.zip" "s3://$BUCKET_NAME/glue-modules/glue-job-modules.zip"
print_success "Uploaded: s3://$BUCKET_NAME/glue-modules/glue-job-modules.zip"

# ============================================================
# Upload JDBC drivers (optional)
# Uses infrastructure/config/jdbc_drivers.json for driver metadata (filenames,
# versions, download URLs, and S3 path conventions).
# ============================================================
if [[ "$INCLUDE_DRIVERS" == true ]]; then
    DRIVERS_CONFIG="infrastructure/config/jdbc_drivers.json"

    if [[ ! -f "$DRIVERS_CONFIG" ]]; then
        print_error "Driver configuration not found: $DRIVERS_CONFIG"
        echo "This file defines supported JDBC drivers, versions, and download URLs."
        exit 1
    fi

    print_info "Reading driver definitions from $DRIVERS_CONFIG"

    drivers_found=false

    # Build list of directories to search for JAR files
    SEARCH_DIRS=("." "jdbc-drivers")
    if [[ -n "$DRIVER_DIR" ]]; then
        SEARCH_DIRS=("$DRIVER_DIR")
    fi

    # Iterate over each driver defined in jdbc_drivers.json
    for engine in $(python3 -c "import json; d=json.load(open('$DRIVERS_CONFIG')); print(' '.join(d['jdbc_drivers'].keys()))"); do
        jar_filename=$(python3 -c "import json; print(json.load(open('$DRIVERS_CONFIG'))['jdbc_drivers']['$engine']['jar_filename'])")
        version=$(python3 -c "import json; print(json.load(open('$DRIVERS_CONFIG'))['jdbc_drivers']['$engine']['recommended_version'])")
        driver_name=$(python3 -c "import json; print(json.load(open('$DRIVERS_CONFIG'))['jdbc_drivers']['$engine']['driver_name'])")
        download_url=$(python3 -c "import json; print(json.load(open('$DRIVERS_CONFIG'))['jdbc_drivers']['$engine']['download_url'])")

        s3_path="s3://$BUCKET_NAME/jdbc-drivers/$engine/$version/$jar_filename"

        # Search for the JAR file in the search directories
        jar_found=""
        for search_dir in "${SEARCH_DIRS[@]}"; do
            [[ ! -d "$search_dir" ]] && continue
            match=$(find "$search_dir" -maxdepth 2 -name "$jar_filename" -type f 2>/dev/null | head -1)
            if [[ -n "$match" ]]; then
                jar_found="$match"
                break
            fi
        done

        if [[ -n "$jar_found" ]]; then
            print_info "Uploading $driver_name: $jar_filename"
            aws s3 cp "$jar_found" "$s3_path"
            print_success "  -> $s3_path"
            drivers_found=true
        else
            print_warning "$driver_name ($jar_filename) not found in: ${SEARCH_DIRS[*]}"
            echo "  Download: $download_url"
            echo "  Then re-run, or upload manually:"
            echo "    aws s3 cp $jar_filename $s3_path"
            echo ""
        fi
    done

    if [[ "$drivers_found" == false ]]; then
        echo ""
        print_warning "No JDBC driver JAR files were uploaded."
        echo ""
        echo "To upload drivers, either:"
        echo "  1. Download the JARs listed above and place them in the current directory or jdbc-drivers/"
        echo "  2. Use --driver-dir to specify a directory containing JAR files"
        echo "  3. Upload manually using the aws s3 cp commands shown above"
        echo ""
        echo "Then reference the S3 paths in your parameter file:"
        echo "  \"SourceJdbcDriverS3Path\": \"s3://$BUCKET_NAME/jdbc-drivers/<engine>/<version>/<jar>\""
    fi
fi

# ============================================================
# Summary
# ============================================================
echo ""
print_success "Asset upload completed!"
echo ""
print_info "Uploaded assets:"
echo "  Glue job script:  s3://$BUCKET_NAME/glue-scripts/main.py"
echo "  Glue job modules: s3://$BUCKET_NAME/glue-modules/glue-job-modules.zip"
if [[ "$INCLUDE_DRIVERS" == true && "$drivers_found" == true ]]; then
    echo "  JDBC drivers:     s3://$BUCKET_NAME/jdbc-drivers/"
fi
echo ""
echo "These paths are compatible with deploy.sh. To deploy without re-uploading:"
echo "  ./deploy.sh -s <stack-name> -b $BUCKET_NAME -p <parameters-file> --skip-upload"
