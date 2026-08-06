import json

import requests
import os
from logger import get_logger
from utils import get_ngs360_token

logger = get_logger()


def post_job(job_id, job_status, log_stream_name):
    """ Post Batch job status to NGS360 REST API """
    message_body = {
        'status': job_status,
        'log_stream_name': log_stream_name
    }

    url = "%s/api/v1/jobs/%s" % (os.environ['NGS360_API_SERVER'], job_id)

    headers = {}

    # NGS360 authenticates on the Authorization header, taking either a JWT or
    # an ngs360_-prefixed API key. This is not the GA4GH WES scheme, which uses
    # its own X-Internal-API-Key header, so the two need separate credentials.
    #
    # Sent only when one is configured: PUT /api/v1/jobs/{job_id} is currently
    # open, so this deploys safely before the credential exists and starts
    # authenticating as soon as it does.
    token = get_ngs360_token()
    if token:
        headers['Authorization'] = f"Bearer {token}"
    else:
        logger.warning(
            "No NGS360 API token configured; job status update will be sent "
            "unauthenticated. Set NGS360_API_TOKEN in the ENV_SECRETS secret."
        )

    try:
        # Logs the body and URL but never the headers, which carry a credential.
        logger.info("PUT %s to %s", message_body, url)
        res = requests.put(
            url,
            json=message_body,
            headers=headers,
            timeout=10
        )
        if res.status_code != 200:
            logger.error("%s returned %s", url, res.status_code)
            logger.error("Response: %s", res.text)
    except requests.exceptions.RequestException as e:
        logger.error(str(e))
        return False
    return True


def batch_event_handler(event):
    """
    AWS Batch Event Handler
    """
    # Extract relevant information from the Batch event
    job_id = event.get('detail', {}).get('jobId')
    job_name = event.get('detail', {}).get('jobName')
    job_status = event.get('detail', {}).get('status')
    log_stream_name = ''

    logger.info(
        f"Processing Batch job - ID: {job_id}, "
        f"Name: {job_name}, Status: {job_status}"
    )

    # Implement your logic to handle different job statuses
    if job_status in ('STARTING', 'RUNNING', 'SUCCEEDED', 'FAILED'):
        if 'logStreamName' in event['detail']['container']:
            log_stream_name = event['detail']['container']['logStreamName']
            logger.info(f"Log Stream Name: {log_stream_name}")
        else:
            logger.error("Unable to determine logStreamName")

    post_job(job_id, job_status, log_stream_name)

    return {
        'statusCode': 200,
        'message': 'AWS Batch Event processed.',
    }
