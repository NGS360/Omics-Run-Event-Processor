import json
import os
import sys

sys.path.append('.')

# Use importlib to import a module with a Python keyword name
import importlib

# Set required environment variables
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
os.environ['API_SERVER'] = 'http://localhost:8000'
os.environ['DATA_LAKE_BUCKET'] = 'bmsrd-ngs-omics'
os.environ['S3_PREFIX'] = 'omics-run-events'
os.environ['VERBOSE_LOGGING'] = 'true'

lambda_module = importlib.import_module('lambda')

# Load a sample event
#with open('tests/allfiles/event_20260129_142708_2fdc76d2-1f2b-4fda-8fe3-786153c4601b.json', 'r') as f: # completed job
with open('tests/allfiles/event_20260129_183844_af0a2997-c2cc-4483-8d88-35d4df86ca30.json', 'r') as f: # completed job with tag
#with open('tests/allfiles/event_20260129_142335_908ba878-82b3-481a-9931-450fff324d44.json', 'r') as f: # running job
#with open('tests/allfiles/event_stopping.json', 'r') as f:
    event = json.load(f)

# Make sure you have AWS credentials configured
# This will use your default AWS profile

# Invoke handler
response = lambda_module.lambda_handler(event, None)
print(response)

