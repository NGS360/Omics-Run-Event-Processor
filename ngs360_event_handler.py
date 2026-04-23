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
        logger.error(f"Error downloading file from S3: {str(e)}")
        raise


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

        # Login crane to ECR
        login_cmd = ["crane", "auth", "login", ecr_registry, "-u", username, "--password-stdin"]
        subprocess.run(login_cmd, input=password, text=True, check=True)

        logger.info(f"Successfully authenticated crane to ECR: {ecr_registry}")

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to authenticate crane to ECR: {e}")
        raise
    except Exception as e:
        logger.error(f"Error during ECR authentication: {e}")
        raise


def migrate_image_with_crane(source_image, target_image):
    """Migrate single image from source registry to ECR using crane copy."""
    try:
        # Use crane copy for direct registry-to-registry migration
        copy_cmd = ["crane", "copy", source_image, target_image]

        logger.info(f"Migrating image: {source_image} → {target_image}")
        result = subprocess.run(copy_cmd, capture_output=True, text=True, check=True)

        logger.info(f"Successfully migrated: {source_image} → {target_image}")

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to migrate image {source_image}: {e.stderr}")
        # Don't fail the entire process for one image
        logger.warning(f"Continuing workflow registration despite image migration failure")


def create_workflow_zip(updated_cwl_content, workflow_id):
    """
    Create a ZIP file containing the updated CWL and upload to S3.

    Args:
        updated_cwl_content: Updated CWL content string
        workflow_id: Workflow ID for naming
        bucket_name: S3 bucket name (if None, uses environment variable)

    Returns:
        S3 path to the created ZIP file
    """
    try:
        bucket_name = os.environ['DataLakeBucket']

        # Use a temporary directory for ZIP creation
        with tempfile.TemporaryDirectory() as temp_dir:
            # Write updated CWL to temporary file
            cwl_file_path = os.path.join(temp_dir, 'workflow.packed.cwl')
            with open(cwl_file_path, 'w') as f:
                f.write(updated_cwl_content)

            # Create ZIP file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            zip_file_path = os.path.join(temp_dir, f"workflow_{timestamp}.zip")
            with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(cwl_file_path, 'workflow.packed.cwl')

            # Upload ZIP to standardized S3 path
            zip_s3_key = f"Workflow_Definition/{workflow_id}/workflow_{timestamp}.zip"
            s3_client.upload_file(zip_file_path, bucket_name, zip_s3_key)

            zip_s3_path = f"s3://{bucket_name}/{zip_s3_key}"
            logger.info(f"Successfully created workflow ZIP at: {zip_s3_path}")
            return zip_s3_path

    except Exception as e:
        logger.error(f"Error creating workflow ZIP: {str(e)}")
        raise


def register_workflow(event):
    """
    Handle workflow registration requests from NGS360.
    Process packed CWL workflow with Docker repository updates.

    Expected event structure:
    {
        "action": "register_workflow",
        "name": "workflow-name",
        "cwl_s3_path": "s3://bucket/path/to/packed.cwl",
        "id": "ngs360-workflow-id"
    }

    Args:
        event: Lambda event containing registration request

    Returns:
        dict: Response with workflow_id or error
    """
    logger.info(f"Received workflow registration request: "
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

    # Get AWS configuration from environment variables (guaranteed to exist)
    ecr_account = os.environ['ECR_ACCOUNT']
    ecr_region = os.environ['ECR_REGION']
    docker_prefix = os.environ.get('DOCKER_PREFIX', '')  # Optional prefix

    logger.info(f"Using AWS configuration: ECR_ACCOUNT={ecr_account}, "
                f"ECR_REGION={ecr_region}, DOCKER_PREFIX={docker_prefix}")

    try:
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
        zip_s3_path = create_workflow_zip(updated_cwl_content, event['id'])

        # Register workflow with AWS HealthOmics
        kwargs = {
            'name': event['name'],
            'engine': 'CWL',
            'definitionZip': zip_s3_path,
            'main': 'workflow.packed.cwl',
            'tags': {
                "NGS360_workflow_id": event['id'],
                "ECR_account": ecr_account,
                "docker_prefix": docker_prefix
            }
        }

        logger.info(f"Creating Omics workflow with parameters: {kwargs}")
        response = omics_client.create_workflow(**kwargs)
        workflow_id = response['id']

        logger.info(f"Successfully registered workflow {workflow_id} "
                    f"from processed CWL at {zip_s3_path}")

        return {
            'statusCode': 200,
            'workflow_id': workflow_id,
            'message': f'Workflow registered successfully with Docker images processed'
        }

    except Exception as e:
        logger.error(f"Error registering workflow: {str(e)}")
        return {
            'statusCode': 500,
            'error': 'WorkflowRegistrationError',
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
