
import json
import os
import requests
from datetime import datetime, timezone

import boto3
from logger import get_logger
from utils import get_auth_token

logger = get_logger()
omics_client = boto3.client('omics')

API_SERVER = os.environ['API_SERVER']


def submit_omics_run(event) -> dict:
    """
    Handle workflow submission requests from GA4GH WES API.
    Submits new workflows to AWS Omics.
    """
    logger.info(f"Received workflow submission request: "
                f"{json.dumps(event, default=str)[:500]}...")
    try:
        # Validate input parameters
        is_valid, error_msg = _validate_submission_request(event)
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
        }

        # Add optional parameters if provided
        if 'workflowVersionName' in workflow_engine_params:
            kwargs['workflowVersionName'] = workflow_engine_params['workflowVersionName']
        if 'cacheId' in workflow_engine_params:
            kwargs['cacheId'] = workflow_engine_params['cacheId']
        if 'storageType' in workflow_engine_params:
            kwargs['storageType'] = workflow_engine_params['storageType']
        if 'storageCapacity' in workflow_engine_params:
            kwargs['storageCapacity'] = workflow_engine_params['storageCapacity']

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


def _validate_submission_request(event) -> tuple[bool, str]:
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


def _pingback_to_ga4ghwes(lambda_response, event):
    # Call GA4GH WES API Server
    if "tags" in event and "callback_url" in event["tags"] and "WESRunId" in event["tags"]:
        api_url = event["tags"]["callback_url"]
    else:
        logger.info(
            "No callback url or GA4GH WES run_id provided. Skip pingback to GA4GH."
        )
        return

    headers = {'Content-Type': 'application/json'}
    headers['X-Internal-API-Key'] = get_auth_token()

    data = {
        "wes_run_id": event["tags"]["WESRunId"],
        "event_time": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    }

    if lambda_response["statusCode"] == 200:
        data["status"] = "PENDING"
        data["omics_run_id"] = lambda_response["omics_run_id"]
    else:
        data["status"] = "FAILED"
        data["failure_reason"] = lambda_response["message"]

    try:
        response = requests.post(
            api_url, headers=headers, data=json.dumps(data), timeout=10
        )
        response.raise_for_status()
        logger.info(
            "Successfully sent event to API server: %s", response.status_code
        )
    except requests.exceptions.RequestException as e:
        logger.error("Error sending event to API server: %s", str(e))


def ga4ghwes_event_handler(event):
    if event.get('action') == 'submit_workflow':
        logger.info("Routing to workflow submission handler")
        response = submit_omics_run(event)
        _pingback_to_ga4ghwes(response, event)
        return response

    return {
        'statusCode': 400,
        'error': 'UnknownAction',
        'message': f'Unknown Action in GA4GHWES Event Handler: '
                   f'{event.get("action")}'
    }
