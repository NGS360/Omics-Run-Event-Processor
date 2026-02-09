# NGS360 Omics Run Event Processor

An AWS Lambda function that processes AWS HealthOmics run state change events, archives them to S3, and forwards them to the NGS360 API server for workflow execution tracking.

## Overview

This serverless application monitors AWS HealthOmics workflow runs and processes state change events in real-time. When an Omics workflow changes state (e.g., starts, completes, fails), this Lambda function:

1. **Captures** the event from EventBridge
2. **Flattens** the nested JSON structure for easier analysis
3. **Enriches** the event with additional information (for completed/failed/cancelled events)
4. **Archives** the event to an S3 data lake with server-side encryption
5. **Notifies** the NGS360 API server via callback endpoint

## Architecture

```
AWS HealthOmics → EventBridge → Lambda Function → S3 Data Lake
                                       ↓
                               NGS360 API Server
```

The Lambda function is deployed in a VPC for secure communication with the NGS360 API server and includes:
- Dead Letter Queue (SNS) for failed executions
- CloudWatch Logs for monitoring
- IAM roles with least-privilege permissions

## Features

- ✅ Real-time event processing from AWS HealthOmics
- ✅ Event archival to S3 with AES-256 encryption
- ✅ Callback integration with NGS360 API
- ✅ JSON flattening for simplified event structure
- ✅ Event enrichment with run tags, output file mapping, and log URLs
- ✅ WES run ID extraction from run tags for API integration
- ✅ Configurable verbose logging
- ✅ VPC support for secure networking
- ✅ Dead letter queue for error handling
- ✅ Infrastructure as Code via CloudFormation

## Prerequisites

- AWS Account with appropriate permissions
- AWS CLI configured with credentials
- Python 3.12
- make (for build automation)
- An S3 bucket for the data lake
- NGS360 API Server URL
- VPC with security group and subnet configured

## Configuration

The stack requires the following parameters (typically defined in `parameters.json`):

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| `DeadLetterEmail` | Email address for failed execution notifications | Yes | - |
| `ApiServer` | NGS360 API Server URL | Yes | - |
| `DataLakeBucket` | S3 bucket name for event storage | Yes | - |
| `BucketPrefix` | S3 prefix/folder for events | No | `omics-run-events` |
| `VerboseLogging` | Enable debug-level logging | No | `false` |
| `SecurityGroupId` | Security group ID for Lambda VPC | Yes | - |
| `SubnetId` | Subnet ID for Lambda VPC | Yes | - |

### Example parameters.json

```json
[
  {
    "ParameterKey": "DeadLetterEmail",
    "ParameterValue": "alerts@example.com"
  },
  {
    "ParameterKey": "ApiServer",
    "ParameterValue": "https://api.ngs360.example.com"
  },
  {
    "ParameterKey": "DataLakeBucket",
    "ParameterValue": "my-ngs360-data-lake"
  },
  {
    "ParameterKey": "SecurityGroupId",
    "ParameterValue": "sg-0123456789abcdef0"
  },
  {
    "ParameterKey": "SubnetId",
    "ParameterValue": "subnet-0123456789abcdef0"
  }
]
```

## Deployment

### Create New Stack

```bash
# Set required environment variables
export DATA_LAKE_BUCKET=your-bucket-name
export BUCKET_PREFIX=omics-run-events

# Build and deploy
make cf-create
```

This command will:
1. Create the Lambda deployment package with dependencies
2. Upload the package to S3
3. Create the CloudFormation stack with all resources

### Update Existing Stack

```bash
make cf-update
```

### Manual Deployment

If you prefer to deploy without make:

```bash
# 1. Create deployment package
rm -rf lambda-package
mkdir lambda-package
cd lambda-package
cp ../lambda.py .
pip3 install -r ../requirements.txt -t .
zip -r ../lambda-package.zip .
cd ..

# 2. Upload to S3
aws s3 cp lambda-package.zip s3://your-bucket/omics-run-events/lambda-package.zip --sse

# 3. Deploy CloudFormation stack
aws cloudformation create-stack \
  --stack-name ngs360-omics-run-event-processor \
  --template-body file://ngs360-omics-run-event-processor.yaml \
  --capabilities CAPABILITY_IAM \
  --parameters file://parameters.json
```

## Event Processing

The Lambda function processes EventBridge events from AWS HealthOmics. To set up event routing:

1. Create an EventBridge rule to trigger this Lambda function
2. Configure the rule to filter for HealthOmics state change events

Example EventBridge rule pattern:

```json
{
  "source": ["aws.omics"],
  "detail-type": ["Run Status Change"]
}
```

## Event Structure

### Input Event
Events from HealthOmics contain nested structures with run information, status, and metadata.

### Flattened Event
The [`flatten()`](lambda.py:15) function processes the nested JSON into a single-level dictionary for easier querying and analysis in downstream systems.

### Enhanced Event Data
For all events, the Lambda function:
- **Retrieves Tags**: Gets the run tags from AWS HealthOmics, including the `WESRunId` tag which is used to link the run to the corresponding WES run ID

For events with status COMPLETED, FAILED, or CANCELLED, the Lambda function additionally adds:
- **Log URLs**: Links to CloudWatch logs for the run, tasks, and manifest
- **Output File Mapping**: For COMPLETED events, a mapping of output names to S3 URIs

### Storage Format
Events are stored in S3 as:
```
s3://{DATA_LAKE_BUCKET}/{S3_PREFIX}/event_{YYYYMMDD_HHMMSS}_{UUID}.json
```

## API Integration

The function calls the NGS360 API endpoint:
```
POST {API_SERVER}/internal/callbacks/omics-state-change
Content-Type: application/json
```

The enhanced event JSON is sent as the request body with a 10-second timeout.

## Development

### Local Testing

```python
import json
from lambda import lambda_handler

# Load a sample event
with open('sample-event.json', 'r') as f:
    event = json.load(f)

# Set required environment variables
import os
os.environ['API_SERVER'] = 'https://api.example.com'
os.environ['DATA_LAKE_BUCKET'] = 'test-bucket'

# Invoke handler
response = lambda_handler(event, None)
print(response)
```

### Dependencies

The Lambda function uses:
- **boto3** (AWS SDK) - Pre-installed in Lambda runtime
- **requests** - HTTP client for API calls (see [`requirements.txt`](requirements.txt:1))

### Logging

Set `VerboseLogging` to `true` in stack parameters to enable DEBUG-level logging. By default, the function logs at INFO level.

## Monitoring

### CloudWatch Logs
Logs are written to: `/aws/lambda/ngs360/omics-run-event-processor`

### Dead Letter Queue
Failed executions are sent to the SNS topic and email notifications are sent to the configured address.

### Metrics to Monitor
- Lambda invocation count
- Lambda error rate
- Lambda duration
- DLQ message count
- S3 PutObject success/failure

## IAM Permissions

The Lambda execution role has permissions for:
- VPC access (AWSLambdaVPCAccessExecutionRole)
- SNS publish (dead letter queue)
- S3 PutObject and GetObject (data lake storage)
- AWS HealthOmics API access (GetRun, ListRuns)

## Security

- 🔒 Lambda runs in VPC for network isolation
- 🔒 S3 objects encrypted with AES-256
- 🔒 IAM roles follow least-privilege principle
- 🔒 No sensitive data in logs (non-verbose mode)

## Troubleshooting

### Lambda Timeout
Default timeout is 900 seconds (15 minutes). Adjust in CloudFormation template if needed.

### VPC Connectivity Issues
Ensure the security group allows outbound HTTPS traffic to:
- S3 (via VPC endpoint or NAT Gateway)
- AWS HealthOmics API
- NGS360 API Server

### API Callback Failures
Check CloudWatch Logs for HTTP error responses. Verify API_SERVER URL and network connectivity.

### Missing Output Mapping or Log URLs
For COMPLETED events, check if the output mapping file exists at the expected S3 path:
```
s3://{bucket}/{prefix}/{run_id}/logs/outputs.json
```

## Project Structure

```
.
├── lambda.py                                  # Lambda function code
├── requirements.txt                           # Python dependencies
├── ngs360-omics-run-event-processor.yaml     # CloudFormation template
├── Makefile                                  # Build and deployment automation
└── README.md                                 # This file
```

## License

Copyright © 2026 NGS360. All rights reserved.

## Support

For issues or questions, please contact the NGS360 development team or open an issue in this repository.
