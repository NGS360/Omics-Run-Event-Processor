from logger import get_logger

logger = get_logger()


def batch_event_handler(event):
    """
    AWS Batch Event Handler
    """
    logger.info(f"Received AWS Batch event: {event}")

    # Extract relevant information from the Batch event
    job_id = event.get('detail', {}).get('jobId')
    job_name = event.get('detail', {}).get('jobName')
    job_status = event.get('detail', {}).get('status')

    logger.info(
        f"Processing Batch job - ID: {job_id}, "
        f"Name: {job_name}, Status: {job_status}"
    )

    # Implement your logic to handle different job statuses
    if job_status == 'SUCCEEDED':
        logger.info(f"Batch job {job_id} succeeded.")
        # Add your success handling code here
    elif job_status == 'FAILED':
        logger.error(f"Batch job {job_id} failed.")
        # Add your failure handling code here
    else:
        logger.warning(f"Batch job {job_id} has an unhandled status: "
                       f"{job_status}")
        # Handle other statuses if necessary
