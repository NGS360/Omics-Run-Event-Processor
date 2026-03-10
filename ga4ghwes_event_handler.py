
import json
import os

import boto3
from logger import get_logger

logger = get_logger()
omics_client = boto3.client('omics')


def submit_omics_run(event):
    """
    Handle workflow submission requests from GA4GH WES API.
    Submits new workflows to AWS Omics.
    """
    logger.info(f"Received workflow submission request: "
                f"{json.dumps(event, default=str)[:500]}...")
    try:
        # Validate input parameters
        is_valid, error_msg = validate_submission_request(event)
        if not is_valid:
            return {
                'statusCode': 400,
                'error': 'ValidationError',
                'message': error_msg
            }

        # Extract parameters
        wes_run_id = event['wes_run_id']
        workflow_id = event['workflow_id']
        workflow_engine_params = event.get('workflow_engine_parameters', {})

        # Set output URI - use provided or default
        output_uri = workflow_engine_params.get('outputUri')

        # Set task name
        if "name" in workflow_engine_params:
            task_name = workflow_engine_params['name']
        elif "tags" in event and "TaskName" in event['tags']:
            task_name = event['tags']['TaskName']
        else:
            task_name = f"wes-run-{wes_run_id}"

        # Build basic Omics parameters
        kwargs = {
            'workflowId': workflow_id,
            'roleArn': os.environ['OMICS_ROLE_ARN'],
            'parameters': event.get('parameters', {}),
            'outputUri': output_uri,
            'name': task_name,
            'tags': {'WESRunId': wes_run_id, **event.get('tags', {})},
            'retentionMode': 'REMOVE',
            'storageType': 'DYNAMIC'
        }

        # Override name if provided in tags
        if "Name" in kwargs['tags']:
            kwargs['name'] = kwargs['tags']['Name']

        # Add optional parameters if provided
        if 'workflowVersionName' in workflow_engine_params:
            kwargs['workflowVersionName'] = workflow_engine_params['workflowVersionName']
        if 'cacheId' in workflow_engine_params:
            kwargs['cacheId'] = workflow_engine_params['cacheId']

        # Submit to Omics
        response = omics_client.start_run(**kwargs)
        omics_run_id = response['id']

        logger.info(f"Started Omics run {omics_run_id} "
                    f"for WES run {wes_run_id}")

        return {
            'statusCode': 200,
            'omics_run_id': omics_run_id,
            'output_uri': output_uri,
            'message': 'Workflow submitted successfully',
            'wes_run_id': wes_run_id
        }

    except Exception as e:
        logger.error(f"Error submitting workflow: {str(e)}")
        return {
            'statusCode': 500,
            'error': 'OmicsSubmissionError',
            'message': str(e)
        }


def validate_submission_request(event):
    """
    Validate workflow submission request.

    Args:
        event: Lambda event containing submission request

    Returns:
        tuple: (is_valid, error_message)
    """
    required_fields = [
        'action', 'wes_run_id', 'workflow_id', 'workflow_engine_parameters'
    ]

    # Make sure the event contains all required fields
    for field in required_fields:
        if field not in event:
            return False, f"Missing required field: {field}"
    if 'outputUri' not in event.get('workflow_engine_parameters'):
        return False, "Missing required field: outputUri"

    # The only supported action is 'submit_workflow' for now
    if event['action'] != 'submit_workflow':
        return False, f"Invalid action: {event['action']}"

    # Validate workflow_id - it should be a non-empty string
    workflow_id = event.get('workflow_id', '')
    if not workflow_id or len(workflow_id) < 1:
        return False, f"Invalid workflow_id: {workflow_id}. " \
                      "Workflow ID is required"

    return True, None


def ga4ghwes_event_handler(event):
    if event.get('action') == 'submit_workflow':
        logger.info("Routing to workflow submission handler")
        return submit_omics_run(event)
