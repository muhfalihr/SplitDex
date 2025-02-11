# Engine for Split Index

## Overview
Split Index Engine is a tool for managing and splitting Elasticsearch indices based on configured parameters and date ranges.

## Configuration Guide

### Location
```bash
cd source
nano config.ini
```

### Configuration Parameters

The configuration file (`config.ini`) contains several sections with specific parameters:

#### Elasticsearch Configuration [elastic]
- `es_url`: Elasticsearch server URL (e.g., http://localhost:9200)
  - Required: Yes
  - Format: Valid URL with protocol
  - Example: http://localhost:9200

- `es_username`: Elasticsearch username
  - Required: Yes
  - Format: String
  - Example: elastic

- `es_password`: Elasticsearch password
  - Required: Yes
  - Format: String
  - Should contain: At least one uppercase, lowercase, number, and special character

- `es_timeout`: Connection timeout in seconds
  - Required: Yes
  - Format: Integer
  - Valid range: 1-86400

- `es_index_name`: Base name for new indices
  - Required: Yes
  - Format: String
  - Example: index-name

- `es_field`: Field used for splitting/sorting
  - Required: Yes
  - Format: String (dot notation supported for nested fields)
  - Example: created_at

#### Engine Configuration [engine]
- `batch_size`: Number of documents to process per batch
  - Required: Yes
  - Format: Integer
  - Valid range: 1-1000
  - Default: 10

- `max_retry_connection`: Maximum number of retry attempts for connection or indexing failures
  - Required: Yes
  - Format: Integer
  - Valid range: 1-10
  - Default: 3
  - Purpose: Defines how many times the system will attempt to reconnect or retry an operation before giving up
  - Helps prevent permanent failures during temporary network or service interruptions

- `format_date`: Date format for index naming
  - Required: Yes
  - Valid formats: 
    - YYYYmm (e.g., 202401)
    - YYYYmmdd (e.g., 20240131)
    - ddmmYYYY (e.g., 31-01-2024)
    - YYYY-mm-dd (e.g., 2024-01-31)
  - Default: YYYYmmdd

#### Query Configuration [query]
- `used_query`: Enable/disable query filtering
  - Required: Yes
  - Valid values: yes, no
  - Default: no

- `gte`: Greater than or equal to date
  - Required: Only if used_query = yes
  - Format: YYYY-MM-DD
  - Example: 2024-12-31
  - If not used, just delete it. because this is optional

- `lte`: Less than or equal to date
  - Required: Only if used_query = yes
  - Format: YYYY-MM-DD
  - Example: 2024-01-01
  - If not used, just delete it. because this is optional

- `iso_format`: Date format for query
  - Required: Only if used_query = yes
  - Required: Yes
  - Valid values: epoch_second, epoch_millis, strict_date_optional_time
  - Default: strict_date_optional_time

- `sort_order`: Sort order for processing
  - Required: Only if used_query = yes
  - Required: Yes
  - Valid values: asc, desc
  - Default: asc

### Example Configuration
```ini
[elastic]
es_url = http://localhost:9200
es_username = elastic
es_password = password
es_timeout = 60
es_index_name = index-name
es_field = created_at

[engine]
batch_size = 10
max_retry_connection = 3
format_date = YYYYmmdd

[query]
used_query = yes
gte = 2024-12-31
lte = 2024-01-01
iso_format = epoch_second
sort_order = asc
```

## Usage

### Method 1: Direct Command
```bash
cd source
python main.py
```

### Method 2: Docker Container
```bash
# Build the Docker image
docker build -t splitdex .

# Run the container
docker run -d -v ./source/config.ini:/app/config.ini --rm --name=splitdex splitdex
```

## Validation Rules
- All required fields must be present in the configuration
- URLs must be valid and include protocol (http:// or https://)
- Date formats must match the specified format exactly
- Numeric values must be within their specified ranges
- Elasticsearch credentials must meet security requirements
- Field names must use proper dot notation for nested fields
- Sort order must be either 'asc' or 'desc'
- Date range queries (gte/lte) must be in valid YYYY-MM-DD format

## Error Handling
The engine will validate all configuration parameters before starting and will:
1. Display specific error messages for invalid configurations
2. Suggest corrections for common misconfigurations
3. Abort execution if critical parameters are invalid

## Troubleshooting
If you encounter configuration errors:
1. Verify all required fields are present
2. Check field value formats match the specifications
3. Ensure Elasticsearch connection details are correct
4. Validate date formats match the specified format
5. Confirm numeric values are within allowed ranges
6. Contact developer https://t.me/muhfalihr