"""NGS360 Event Handler for Workflow Registration."""

import json
import os

import boto3
from logger import get_logger

logger = get_logger()
omics_client = boto3.client('omics')


def register_workflow(event):
    """
    Handle workflow registration requests from NGS360.
    Registers a CWL workflow from S3 path to AWS Omics.

    Args:
        event: Lambda event containing registration request

    Returns:
        dict: Response with workflow_id or error
    """
    logger.info(f"Received workflow registration request: "
                f"{json.dumps(event, default=str)[:500]}...")

    required_fields = ['cwl_s3_path', 'name', 'id']
    for field in required_fields:
        if field not in event:
            logger.error(f"{field} is required but not provided.")
            return {
                'statusCode': 400,
                'error': 'ValidationError',
                'message': f"{field} is required but not provided."
            }

    # Build Omics create_workflow parameters
    kwargs = {
        'name': event['name'],
        'engine': 'CWL',
        'definitionUri': event['cwl_s3_path'],
        'tags': {
            "NGS360_workflow_id": event['id']
        }
    }

    # Create the workflow in Omics
    try:
        logger.info(f"Creating Omics workflow with parameters: {kwargs}")
        response = omics_client.create_workflow(**kwargs)
        workflow_id = response['id']
        logger.info(f"Successfully registered workflow {workflow_id} "
                    f"from CWL path {event['cwl_s3_path']}")
        return {
            'statusCode': 200,
            'workflow_id': workflow_id,
            'message': 'Workflow registered successfully'
        }

    except Exception as e:
        logger.error(f"Error registering workflow: {str(e)}")
        return {
            'statusCode': 500,
            'error': 'OmicsCreateWorkflowError',
            'message': str(e)
        }


def ngs360_event_handler(event):
    """
    Main handler for NGS360 events.
    
    Args:
        event: Lambda event from NGS360 source
        
    Returns:
        dict: Response from appropriate handler function
    """
    action = event.get('action')
    
    if action == 'register_workflow':
        logger.info("Routing to workflow registration handler")
        return register_workflow(event)
    else:
        logger.error(f"Unknown NGS360 action: {action}")
        return {
            'statusCode': 400,
            'error': 'UnknownAction',
            'message': f"Unknown NGS360 action: {action}. Supported actions: register_workflow"
        }
