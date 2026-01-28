import json
import os
from lambda import lambda_handler

# Set required environment variables
os.environ['API_SERVER'] = 'https://your-api-server.com'
os.environ['DATA_LAKE_BUCKET'] = 'your-data-lake-bucket'
os.environ['S3_PREFIX'] = 'omics-run-events'
os.environ['VERBOSE_LOGGING'] = 'true'

# Load a sample event
with open('tests/example_jsons/event_completed_example.json', 'r') as f:
    event = json.load(f)

# Make sure you have AWS credentials configured
# This will use your default AWS profile

# Invoke handler
response = lambda_handler(event, None)
print(response)

