''' Omics Run Event Processor Lambda Function '''
import json

from logger import get_logger

from batch_event_handler import batch_event_handler
import ga4ghwes_event_handler
from omics_event_handler import omics_event_handler

logger = get_logger()


def lambda_handler(event, context):
    """
    Main entry point for Lambda function.

    The goal of this entry point is to handle workflow submissions from GA4GH
    WES API and handle workflow status updates from aws.omics via EventBridge.

    The event structure will be for GA4GH workflow submissions:
    {
        'action': 'submit_workflow',
    }
    or EventBridge events
    {
        'source': 'aws.omics',
        'detail-type': 'Run Status Change',
        'detail': {
            'runId': 'string',
            'status': 'string',
            ...
        },
        ...
    }
    """
    # Log the incoming event (truncated for readability)
    logger.info(f"Received event: {json.dumps(event, default=str)[:500]}...")

    try:
        # Check for events from GA4GH WES API
        if event.get('source') == 'ga4ghwes':
            logger.info("Received GA4GH WES event")
            return ga4ghwes_event_handler(event)

        # Check for AWS Events from EventBridge
        elif event.get('source') == 'aws.omics':
            logger.info("Recieved Omics event")
            return omics_event_handler(event)

        # Check for AWS Batch events
        elif event.get('source') == 'aws.batch':
            logger.info("Received AWS Batch event")
            return batch_event_handler(event)

        # Unknown event type
        else:
            error_msg = f"Unknown event type. Event structure: " \
                        f"{json.dumps(event, default=str)[:500]}..."
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'error': 'UnknownEventType',
                'message': 'Unable to determine event type - neither '
                           'EventBridge nor workflow submission'
            }

    except Exception as e:
        logger.error(f"Error in main handler: {str(e)}")
        return {
            'statusCode': 500,
            'error': 'InternalError',
            'message': f'Lambda handler error: {str(e)}'
        }
