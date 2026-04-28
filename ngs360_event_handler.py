"""NGS360 Event Handler for Workflow Registration."""

import json
import os
import re
import tempfile
import zipfile
import subprocess
from datetime import datetime
import boto3
from logger import get_logger

logger = get_logger()
omics_client = boto3.client('omics')
s3_client = boto3.client('s3')
ecr_client = boto3.client('ecr')


def parse_s3_path(s3_path):
    """Parse s3://bucket/key into bucket and key."""
    if not s3_path.startswith('s3://'):
        raise ValueError(f"Invalid S3 path format: {s3_path}")

    parts = s3_path.replace('s3://', '').split('/', 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid S3 path format: {s3_path}")

    return parts[0], parts[1]


def download_cwl_from_s3(s3_path):
    """Download file from S3 and return content as string."""
    try:
        bucket, key = parse_s3_path(s3_path)
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')

        logger.info(f"Successfully downloaded {len(content)} bytes from S3")
        return content

    except Exception as e:
        error_message = f"Error downloading file from S3 {s3_path}: {str(e)}"
        logger.error(error_message)
        # Re-raise with more descriptive message that includes context
        raise RuntimeError(error_message) from e


def ensure_ecr_repository(repo_name, ecr_account):
    """
    Ensure ECR repository exists and has proper policies for HealthOmics.

    Args:
        repo_name: Repository name
        ecr_account: ECR account ID
    """
    try:
        # Try to create the repository
        ecr_client.create_repository(repositoryName=repo_name)
        logger.info(f"Created ECR repository: {repo_name}")

        # Set repository policy for HealthOmics access
        policy_text = '''
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "omics workflow",
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "omics.amazonaws.com"
                    },
                    "Action": [
                        "ecr:GetDownloadUrlForLayer",
                        "ecr:BatchGetImage",
                        "ecr:BatchCheckLayerAvailability"
                    ]
                }
            ]
        }
        '''

        ecr_client.set_repository_policy(
            repositoryName=repo_name,
            policyText=policy_text.strip()
        )
        logger.info(f"Set HealthOmics policy for repository: {repo_name}")

    except ecr_client.exceptions.RepositoryAlreadyExistsException:
        logger.info(f"ECR repository already exists: {repo_name}")
    except Exception as e:
        logger.error(f"Error managing ECR repository {repo_name}: {str(e)}")
        # Don't fail the entire process for ECR issues
        pass


def migrate_docker_images_with_crane(
        cwl_content, ecr_account, ecr_region, docker_prefix
    ):
    """
    Update Docker repository URLs in CWL content and migrate images using crane.

    Args:
        cwl_content: String content of the packed CWL file
        ecr_account: AWS ECR account ID
        ecr_region: AWS ECR region
        docker_prefix: Docker repository prefix

    Returns:
        Updated CWL content string
    """
    logger.info(f"Migrating Docker images to AWS ECR using crane")
    # Authenticate to both source (SBG) and destination (ECR) registries
    authenticate_crane_to_sbg()
    authenticate_crane_to_ecr(ecr_account, ecr_region)
    ecr_base_uri = f'{ecr_account}.dkr.ecr.{ecr_region}.amazonaws.com/{docker_prefix}/'

    processed_images = []
    lines = cwl_content.split('\n')
    updated_lines = []
    pattern = re.compile(r'"dockerPull":\s*"(.*?)"')

    for line in lines:
        match = pattern.search(line)
        if match:
            source_image = match.group(1)
            reponame_version = source_image.split('/')[-1]
            target_image = ecr_base_uri + reponame_version
            logger.info(f"Processing: {source_image} → {target_image}")

            if reponame_version not in processed_images:
                processed_images.append(reponame_version)

                # Create ECR repository & migrate image
                repo_name = f"{docker_prefix}/{reponame_version.split(':')[0]}"
                ensure_ecr_repository(repo_name, ecr_account)
                migrate_image_with_crane(source_image, target_image)

            # Replace the Docker image URL in the line
            line = line.replace(source_image, target_image)

        updated_lines.append(line)

    updated_content = '\n'.join(updated_lines)
    logger.info(f"Successfully migrated {len(processed_images)} unique Docker images to AWS ECR")

    return updated_content


def authenticate_crane_to_ecr(ecr_account, ecr_region):
    """Authenticate crane to ECR using AWS credentials."""
    try:
        ecr_registry = f"{ecr_account}.dkr.ecr.{ecr_region}.amazonaws.com"

        # Get ECR login password using boto3 (uses Lambda's IAM role)
        token_response = ecr_client.get_authorization_token()

        # Extract password from token
        import base64
        auth_data = token_response['authorizationData'][0]
        token = auth_data['authorizationToken']
        username, password = base64.b64decode(token).decode().split(':', 1)

        # Login crane to ECR (HOME environment variable set via CloudFormation)
        login_cmd = ["./crane", "auth", "login", ecr_registry, "-u", username, "--password-stdin"]
        subprocess.run(login_cmd, input=password, text=True, check=True)
        logger.info(f"Successfully authenticated crane to ECR: {ecr_registry}")

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to authenticate crane to ECR: {e}")
        raise
    except Exception as e:
        logger.error(f"Error during ECR authentication: {e}")
        raise


def authenticate_crane_to_sbg():
    """Authenticate crane to Seven Bridges Genomics registry using SBG credentials."""
    try:
        # SBG registry endpoint
        sbg_registry = "images.sbgenomics.com"

        # Get username and token from environment variables
        sbg_username = os.environ.get('SBG_USERNAME')
        sbg_token = os.environ.get('SBG_AUTH_TOKEN')

        if not sbg_username or not sbg_token:
            raise ValueError("SBG_USERNAME and SBG_AUTH_TOKEN environment variables must be set for Docker registry authentication")

        # For SBG, the username is your SBG username and password is your API token
        # HOME environment variable set via CloudFormation
        login_cmd = ["./crane", "auth", "login", sbg_registry, "-u", sbg_username, "--password-stdin"]
        result = subprocess.run(login_cmd, input=sbg_token, text=True, capture_output=True, check=True)
        logger.info(f"Successfully authenticated crane to SBG registry: {sbg_registry}")

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to authenticate crane to SBG: {e.stderr or e.stdout}")
        raise
    except Exception as e:
        logger.error(f"Error during SBG authentication: {e}")
        raise

def migrate_image_with_crane(source_image, target_image):
    """Migrate single image from source registry to ECR using crane copy."""
    try:
        # Use crane copy for direct registry-to-registry migration
        # HOME environment variable set via CloudFormation
        copy_cmd = ["./crane", "copy", source_image, target_image]

        logger.info(f"Migrating image: {source_image} → {target_image}")
        result = subprocess.run(copy_cmd, capture_output=True, text=True, check=True)

        logger.info(f"Successfully migrated: {source_image} → {target_image}")

    except subprocess.CalledProcessError as e:
        error_message = f"Failed to migrate Docker image {source_image} to {target_image}. Command: {' '.join(copy_cmd)}. Error: {e.stderr or e.stdout or 'No error output'}"
        logger.error(error_message)
        # Fail the entire process since workflow won't work without the image
        raise RuntimeError(error_message) from e
    except Exception as e:
        error_message = f"Unexpected error migrating Docker image {source_image} to {target_image}: {str(e)}"
        logger.error(error_message)
        raise RuntimeError(error_message) from e


def create_workflow_zip(updated_cwl_content, workflow_id):
    """
    Create a ZIP file containing the updated CWL locally and upload to S3 for records.

    Args:
        updated_cwl_content: Updated CWL content string
        workflow_id: Workflow ID for naming

    Returns:
        tuple: (local_zip_path, s3_zip_path) for HealthOmics call and record keeping
    """
    try:
        bucket_name = os.environ['DATA_LAKE_BUCKET']
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Create ZIP file in Lambda's /tmp directory (persists for the execution)
        zip_file_path = f"/tmp/workflow_{workflow_id}_{timestamp}.zip"
        cwl_file_path = f"/tmp/workflow_{workflow_id}.packed.cwl"

        # Write updated CWL to temporary file
        with open(cwl_file_path, 'w') as f:
            f.write(updated_cwl_content)

        # Create ZIP file
        with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(cwl_file_path, 'workflow.packed.cwl')

        # Upload ZIP to S3 for record keeping
        zip_s3_key = f"Workflow_Definition/{workflow_id}/workflow_{timestamp}.zip"
        s3_client.upload_file(zip_file_path, bucket_name, zip_s3_key)
        zip_s3_path = f"s3://{bucket_name}/{zip_s3_key}"

        logger.info(f"Successfully created workflow ZIP locally: {zip_file_path}")
        logger.info(f"Successfully uploaded workflow ZIP to S3: {zip_s3_path}")
        return zip_s3_path

    except Exception as e:
        logger.error(f"Error creating workflow ZIP: {str(e)}")
        raise


def _process_workflow(event):
    """
    Common workflow processing logic shared by register_workflow and create_workflow_version.

    Args:
        event: Lambda event containing workflow request

    Returns:
        tuple: (s3_zip_path, ecr_account, ecr_region, docker_prefix)

    Raises:
        Exception: Any error during processing
    """
    # Get AWS configuration from environment variables
    ecr_account = os.environ['ECR_ACCOUNT']
    ecr_region = os.environ['ECR_REGION']
    docker_prefix = os.environ.get('DOCKER_PREFIX')

    logger.info(f"Using AWS configuration: ECR_ACCOUNT={ecr_account}, "
                f"ECR_REGION={ecr_region}, DOCKER_PREFIX={docker_prefix}")

    # Download packed CWL from S3
    cwl_content = download_cwl_from_s3(event['cwl_s3_path'])

    # Migrate Docker images and update CWL URLs
    updated_cwl_content = migrate_docker_images_with_crane(
        cwl_content,
        ecr_account,
        ecr_region,
        docker_prefix
    )

    # Create ZIP bundle with updated CWL
    s3_zip_path = create_workflow_zip(updated_cwl_content, event['id'])
    return s3_zip_path


def create_workflow(event):
    """
    Handle workflow creation requests from NGS360.
    Create a new workflow in HealthOmics.

    Expected event structure:
    {
        "action": "create_workflow",
        "name": "workflow-name",
        "cwl_s3_path": "s3://bucket/path/to/packed.cwl",
        "id": "ngs360-workflow-id"
    }
    """
    logger.info(f"Received workflow creation request: "
                f"{json.dumps(event, default=str)[:500]}...")

    # Validate required fields
    required_fields = ['cwl_s3_path', 'name', 'id']
    for field in required_fields:
        if field not in event:
            logger.error(f"{field} is required but not provided.")
            return {
                'statusCode': 400,
                'error': 'ValidationError',
                'message': f"{field} is required but not provided."
            }

    try:
        # Process workflow using common logic
        s3_zip_path = _process_workflow(event)

        # Create new workflow with HealthOmics
        kwargs = {
            'name': event['name'],
            'engine': 'CWL',
            'definitionUri': s3_zip_path,
            'main': 'workflow.packed.cwl',
            'tags': {
                "NGS360_workflow_id": event['id']
            }
        }

        logger.info(f"Creating Omics workflow with parameters: {kwargs}")
        response = omics_client.create_workflow(**kwargs)
        workflow_id = response['id']

        logger.info(f"Successfully registered workflow {workflow_id} using ZIP: {s3_zip_path}")

        return {
            'statusCode': 200,
            'workflow_id': workflow_id,
            'zip_s3_path': s3_zip_path,
            'message': f'Workflow registered successfully with Docker images processed'
        }

    except Exception as e:
        logger.error(f"Error registering workflow: {str(e)}")
        return {
            'statusCode': 500,
            'error': 'WorkflowRegistrationError',
            'message': str(e)
        }


def create_workflow_version(event):
    """
    Handle workflow version creation requests from NGS360.
    Create a new version of existing workflow in HealthOmics.

    Expected event structure:
    {
        "action": "create_workflow_version",
        "omics_workflow_id": "1234567",
        "version_name": "version-name",
        "cwl_s3_path": "s3://bucket/path/to/packed.cwl",
        "id": "ngs360-workflow-id"
    }
    """
    logger.info(f"Received workflow version creation request: "
                f"{json.dumps(event, default=str)[:500]}...")

    # Validate required fields
    required_fields = ['omics_workflow_id', 'cwl_s3_path', 'version_name', 'id']
    for field in required_fields:
        if field not in event:
            logger.error(f"{field} is required but not provided.")
            return {
                'statusCode': 400,
                'error': 'ValidationError',
                'message': f"{field} is required but not provided."
            }

    try:
        # Process workflow using common logic
        s3_zip_path = _process_workflow(event)

        # Create workflow version with HealthOmics
        kwargs = {
            'workflowId': event['omics_workflow_id'],  # Existing workflow ID
            'versionName': event['version_name'], # Version name
            'definitionUri': s3_zip_path,     # S3 ZIP path
            'main': 'workflow.packed.cwl',    # Main CWL file in ZIP
            'tags': {
                "NGS360_workflow_id": event['id'],
            }
        }

        logger.info(f"Creating Omics workflow version with parameters: {kwargs}")
        response = omics_client.create_workflow_version(**kwargs)
        version_name = response['versionName']

        logger.info(f"Successfully created workflow version {version_name} for workflow {event['omics_workflow_id']}")

        return {
            'statusCode': 200,
            'version_name': version_name,
            'omics_workflow_id': event['omics_workflow_id'],
            'zip_s3_path': s3_zip_path,
            'message': f'Workflow version created successfully with Docker images processed'
        }

    except Exception as e:
        logger.error(f"Error creating workflow version: {str(e)}")
        return {
            'statusCode': 500,
            'error': 'WorkflowVersionCreationError',
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

    if action == 'create_workflow':
        logger.info("Routing to workflow creation handler")
        return create_workflow(event)
    elif action == 'create_workflow_version':
        logger.info("Routing to workflow version creation handler")
        return create_workflow_version(event)
    else:
        logger.error(f"Unknown NGS360 action: {action}")
        return {
            'statusCode': 400,
            'error': 'UnknownAction',
            'message': f"Unknown NGS360 action: {action}. Supported actions: create_workflow, create_workflow_version"
        }
