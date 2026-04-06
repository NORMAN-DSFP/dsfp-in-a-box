# DSFP Data Loader

The DSFP Data Loader is a containerized service that downloads JSON files from AWS S3 and loads them into Elasticsearch for the Digital Structure Fingerprinting Platform.

## Features

- **Flexible Input Modes**: 
  - ZIP File Mode: Process a specific ZIP file containing JSON files
  - Folder Mode: Scan S3 folder for JSON and ZIP files
- Downloads files from AWS S3 with robust error handling
- Handles ZIP archives containing multiple JSON files
- Bulk indexing to Elasticsearch with configurable batch sizes
- Automatic file size filtering to prevent memory issues
- Comprehensive logging and error handling
- Runs as a separate container for clean separation of concerns

## Setup

### 1. Configure AWS Credentials

Copy the example environment file and configure your AWS credentials:

```bash
cp .env.example .env
```

Edit `.env` with your AWS configuration:

**For ZIP File Mode (single ZIP file):**
```bash
# AWS S3 Credentials
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_REGION=us-east-1
AWS_S3_BUCKET=your-s3-bucket-name

# Input Mode Configuration
INPUT_MODE=zip_file
ZIP_FILE_PATH=path/to/your/data.zip
```

**For Folder Mode (scan multiple files):**
```bash
# AWS S3 Credentials
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_REGION=us-east-1
AWS_S3_BUCKET=your-s3-bucket-name

# Input Mode Configuration
INPUT_MODE=folder
AWS_S3_PREFIX=dsfp-data/
```

### 2. Ensure DSFP Stack is Running

The data loader requires Elasticsearch to be running and properly initialized:

```bash
# Start the DSFP stack
docker compose up -d

# Wait for all services to be healthy
docker compose ps
```

### 3. Run the Data Loader

You now have two modes of operation:

#### Mode A: Process a Specific ZIP File (Recommended for single ZIP files)

**On Linux/macOS:**
```bash
chmod +x load-zip.sh
./load-zip.sh datasets/my-data.zip
# Or with custom bucket:
./load-zip.sh datasets/my-data.zip my-bucket-name
```

**On Windows (PowerShell):**
```powershell
.\load-zip.ps1 -ZipFilePath datasets/my-data.zip
# Or with custom bucket:
.\load-zip.ps1 -ZipFilePath datasets/my-data.zip -CustomBucket my-bucket-name
```

#### Mode B: Scan Folder for Multiple Files

**On Linux/macOS:**
```bash
chmod +x load-data.sh
./load-data.sh
```

**On Windows (PowerShell):**
```powershell
.\load-data.ps1
```

#### Mode C: Manual Execution

```bash
# For ZIP file mode, set environment variables:
export INPUT_MODE=zip_file
export ZIP_FILE_PATH=datasets/my-data.zip

# Build and run
docker compose build data-loader
docker compose --profile data-loading run --rm data-loader
```

## How It Works

### 1. S3 Discovery
- Connects to AWS S3 using provided credentials
- Lists all files in the specified bucket and prefix
- Filters for `.json` and `.zip` files
- Checks file sizes against the configured limit

### 2. File Processing
- Downloads files to temporary container storage
- For ZIP files: extracts and processes contained JSON files
- For JSON files: processes directly
- Handles both single JSON objects and arrays of objects

### 3. Elasticsearch Indexing
- Connects to the Elasticsearch container
- Verifies the target index exists (created by init-elasticsearch)
- Bulk indexes documents in configurable batch sizes
- Uses document `id` or `_id` fields if available
- Provides detailed logging of the indexing process

### 4. Cleanup
- Automatically removes temporary files
- Exits when complete (container stops)

## Configuration Options

All configuration is done through environment variables in the `.env` file:

| Variable | Description | Default |
|----------|-------------|---------|
| `AWS_ACCESS_KEY_ID` | AWS access key | Required |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | Required |
| `AWS_REGION` | AWS region | `us-east-1` |
| `AWS_S3_BUCKET` | S3 bucket name | Required |
| `INPUT_MODE` | Processing mode: `zip_file` or `folder` | `folder` |
| `ZIP_FILE_PATH` | S3 key for specific ZIP file (when mode=zip_file) | None |
| `AWS_S3_PREFIX` | S3 folder path (when mode=folder) | `dsfp-data/` |
| `ELASTICSEARCH_URL` | Elasticsearch endpoint | `http://elasticsearch:9200` |
| `ELASTICSEARCH_INDEX` | Target index name | `dsfp-screening-index` |
| `BATCH_SIZE` | Documents per bulk request | `100` |
| `MAX_FILE_SIZE_MB` | Maximum file size to process | `100` |

## Troubleshooting

### Common Issues

**"AWS credentials not found"**
- Verify your `.env` file contains valid AWS credentials
- Ensure the AWS user has S3 read permissions for the specified bucket

**"Elasticsearch index does not exist"**
- Ensure the `init-elasticsearch` container has run successfully
- Check that the Elasticsearch container is healthy: `docker compose ps`

**"No files found to process"**
- Verify the S3 bucket name and prefix are correct
- Check that the S3 path contains `.json` or `.zip` files
- Ensure the AWS user has permission to list objects in the bucket

### Monitoring Progress

The data loader provides detailed logging:

```bash
# Follow the logs in real-time
docker compose --profile data-loading logs -f data-loader
```

### Re-running the Data Loader

The data loader can be run multiple times safely:
- Documents with the same ID will be updated (not duplicated)
- Documents without IDs will be duplicated
- The process is idempotent for documents with unique IDs

## Integration with DSFP

The data loader integrates seamlessly with the DSFP ecosystem:

1. **Dependencies**: Requires `elasticsearch` and `init-elasticsearch` services
2. **Network**: Uses the same Docker network for internal communication
3. **Index**: Loads data into the same index used by the DSFP server
4. **Monitoring**: Status visible in the DSFP dashboard at http://localhost:9000

## Architecture Decision

The data loader is implemented as a **separate container** rather than part of the Elasticsearch container because:

- **Single Responsibility**: Each container has one clear purpose
- **Reusability**: Can be run on-demand or scheduled independently
- **Maintainability**: Easy to update data loading logic without affecting Elasticsearch
- **Debugging**: Isolated logs make troubleshooting easier
- **Scalability**: Can be scaled or modified independently
- **Security**: Keeps AWS credentials isolated from the database container
