#!/usr/bin/env python3
"""
Pytest-based unit tests for the NGS360 Event Handler module.

This script tests the NGS360 workflow registration functionality using mocks
to avoid real AWS API calls.
"""

import json
import os
import sys
import pytest
from unittest.mock import MagicMock, patch

# Add the parent directory to the path so we can import the ngs360_event_handler module
sys.path.append('..')

# Set up required environment variables before importing modules
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
os.environ['VERBOSE_LOGGING'] = 'true'
os.environ['ECR_ACCOUNT'] = '123456789012'
os.environ['ECR_REGION'] = 'us-east-1'
os.environ['DOCKER_PREFIX'] = 'test-prefix'
os.environ['DATA_LAKE_BUCKET'] = 'test-bucket'

# Import the ngs360_event_handler module
import ngs360_event_handler


class TestNGS360WorkflowCreation:
    """Test NGS360 workflow creation functionality."""

    @patch('ngs360_event_handler._process_workflow')
    @patch('ngs360_event_handler.omics_client')
    def test_create_workflow_success(self, mock_omics_client, mock_process_workflow):
        """Test successful workflow creation."""
        # Set up mocks
        mock_process_workflow.return_value = 's3://test-bucket/workflow-zips/test.zip'
        mock_omics_client.create_workflow.return_value = {
            'id': 'wf-abc123456',
            'arn': 'arn:aws:omics:us-east-1:123456789012:workflow/wf-abc123456'
        }

        # Valid event
        event = {
            'action': 'create_workflow',
            'cwl_s3_path': 's3://test-bucket/workflows/hello-world.cwl',
            'name': 'hello-world-workflow',
            'id': 'ngs360-workflow-123'
        }

        response = ngs360_event_handler.create_workflow(event)

        # Verify response
        assert response['statusCode'] == 200
        assert response['workflow_id'] == 'wf-abc123456'
        assert 'Workflow registered successfully with Docker images processed' in response['message']

        # Verify create_workflow was called with correct parameters
        mock_omics_client.create_workflow.assert_called_once()
        call_kwargs = mock_omics_client.create_workflow.call_args[1]

        assert call_kwargs['name'] == 'hello-world-workflow'
        assert call_kwargs['engine'] == 'CWL'
        assert call_kwargs['definitionUri'] == 's3://test-bucket/workflow-zips/test.zip'
        assert call_kwargs['tags']['NGS360_workflow_id'] == 'ngs360-workflow-123'

    def test_create_workflow_missing_cwl_s3_path(self):
        """Test workflow creation with missing cwl_s3_path."""
        event = {
            'action': 'create_workflow',
            'name': 'hello-world-workflow',
            'id': 'ngs360-workflow-123'
            # Missing cwl_s3_path
        }

        response = ngs360_event_handler.create_workflow(event)

        assert response['statusCode'] == 400
        assert response['error'] == 'ValidationError'
        assert response['message'] == 'cwl_s3_path is required but not provided.'

    def test_create_workflow_missing_name(self):
        """Test workflow creation with missing name."""
        event = {
            'action': 'create_workflow',
            'cwl_s3_path': 's3://test-bucket/workflows/hello-world.cwl',
            'id': 'ngs360-workflow-123'
            # Missing name
        }

        response = ngs360_event_handler.create_workflow(event)

        assert response['statusCode'] == 400
        assert response['error'] == 'ValidationError'
        assert response['message'] == 'name is required but not provided.'

    def test_create_workflow_missing_id(self):
        """Test workflow creation with missing id."""
        event = {
            'action': 'create_workflow',
            'cwl_s3_path': 's3://test-bucket/workflows/hello-world.cwl',
            'name': 'hello-world-workflow'
            # Missing id
        }

        response = ngs360_event_handler.create_workflow(event)

        assert response['statusCode'] == 400
        assert response['error'] == 'ValidationError'
        assert response['message'] == 'id is required but not provided.'


class TestNGS360WorkflowVersionCreation:
    """Test NGS360 workflow version creation functionality."""

    @patch('ngs360_event_handler._process_workflow')
    @patch('ngs360_event_handler.omics_client')
    def test_create_workflow_version_success(self, mock_omics_client, mock_process_workflow):
        """Test successful workflow version creation."""
        # Set up mocks
        mock_process_workflow.return_value = 's3://test-bucket/workflow-zips/test-v2.zip'
        mock_omics_client.create_workflow_version.return_value = {
            'id': 'wf-version-456',
            'arn': 'arn:aws:omics:us-east-1:123456789012:workflow/wf-abc123456/version/wf-version-456',
            'versionName': 'v2.0'
        }

        # Valid event
        event = {
            'action': 'create_workflow_version',
            'omics_workflow_id': 'wf-abc123456',
            'version_name': 'v2.0',
            'cwl_s3_path': 's3://test-bucket/workflows/hello-world-v2.cwl',
            'id': 'ngs360-version-123'
        }

        response = ngs360_event_handler.create_workflow_version(event)

        # Verify response
        assert response['statusCode'] == 200
        assert response['version_name'] == 'v2.0'
        assert response['omics_workflow_id'] == 'wf-abc123456'
        assert 'Workflow version created successfully with Docker images processed' in response['message']

        # Verify create_workflow_version was called with correct parameters
        mock_omics_client.create_workflow_version.assert_called_once()
        call_kwargs = mock_omics_client.create_workflow_version.call_args[1]

        assert call_kwargs['workflowId'] == 'wf-abc123456'
        assert call_kwargs['versionName'] == 'v2.0'
        assert call_kwargs['definitionUri'] == 's3://test-bucket/workflow-zips/test-v2.zip'
        assert call_kwargs['tags']['NGS360_workflow_id'] == 'ngs360-version-123'

    def test_create_workflow_version_missing_omics_workflow_id(self):
        """Test workflow version creation with missing omics_workflow_id."""
        event = {
            'action': 'create_workflow_version',
            'version_name': 'v2.0',
            'cwl_s3_path': 's3://test-bucket/workflows/hello-world-v2.cwl',
            'id': 'ngs360-version-123'
            # Missing omics_workflow_id
        }

        response = ngs360_event_handler.create_workflow_version(event)

        assert response['statusCode'] == 400
        assert response['error'] == 'ValidationError'
        assert response['message'] == 'omics_workflow_id is required but not provided.'

    def test_create_workflow_version_missing_version_name(self):
        """Test workflow version creation with missing version_name."""
        event = {
            'action': 'create_workflow_version',
            'omics_workflow_id': 'wf-abc123456',
            'cwl_s3_path': 's3://test-bucket/workflows/hello-world-v2.cwl',
            'id': 'ngs360-version-123'
            # Missing version_name
        }

        response = ngs360_event_handler.create_workflow_version(event)

        assert response['statusCode'] == 400
        assert response['error'] == 'ValidationError'
        assert response['message'] == 'version_name is required but not provided.'

    @pytest.mark.parametrize("event,expected_field", [
        # Missing all required fields
        ({'action': 'create_workflow'}, 'cwl_s3_path'),
        # Missing name and id
        ({'action': 'create_workflow', 'cwl_s3_path': 's3://bucket/workflow.cwl'}, 'name'),
        # Missing cwl_s3_path and id
        ({'action': 'create_workflow', 'name': 'test-workflow'}, 'cwl_s3_path'),
    ])
    def test_register_workflow_missing_multiple_fields(self, event, expected_field):
        """Test workflow registration with multiple missing fields scenarios."""
        response = ngs360_event_handler.create_workflow(event)
        assert response['statusCode'] == 400
        assert response['error'] == 'ValidationError'
        assert 'is required but not provided' in response['message']

    @patch('ngs360_event_handler._process_workflow')
    @patch('ngs360_event_handler.omics_client')
    def test_register_workflow_omics_api_error(self, mock_omics_client, mock_process_workflow):
        """Test workflow registration with AWS Omics API error (expected error scenario)."""
        # Set up mocks
        mock_process_workflow.return_value = 's3://test-bucket/workflow-zips/invalid.zip'
        mock_omics_client.create_workflow.side_effect = Exception("InvalidWorkflowDefinitionException: Invalid CWL definition")

        event = {
            'action': 'create_workflow',
            'cwl_s3_path': 's3://test-bucket/workflows/invalid.cwl',
            'name': 'invalid-workflow',
            'id': 'ngs360-workflow-456'
        }

        response = ngs360_event_handler.create_workflow(event)

        assert response['statusCode'] == 500
        assert response['error'] == 'WorkflowRegistrationError'
        assert 'InvalidWorkflowDefinitionException' in response['message']

    @patch('ngs360_event_handler._process_workflow')
    @patch('ngs360_event_handler.omics_client')
    def test_register_workflow_with_minimal_event(self, mock_omics_client, mock_process_workflow):
        """Test workflow registration with minimal required fields only."""
        # Set up mocks
        mock_process_workflow.return_value = 's3://test-bucket/workflow-zips/minimal.zip'
        mock_omics_client.create_workflow.return_value = {
            'id': 'wf-minimal123',
            'arn': 'arn:aws:omics:us-east-1:123456789012:workflow/wf-minimal123'
        }

        # Minimal valid event
        event = {
            'cwl_s3_path': 's3://test-bucket/minimal.cwl',
            'name': 'minimal-workflow',
            'id': 'ngs360-minimal-1'
        }

        response = ngs360_event_handler.create_workflow(event)

        # Verify successful response
        assert response['statusCode'] == 200
        assert response['workflow_id'] == 'wf-minimal123'

        # Verify API was called with correct minimal parameters
        mock_omics_client.create_workflow.assert_called_once()
        call_kwargs = mock_omics_client.create_workflow.call_args[1]
        
        expected_params = {
            'name': 'minimal-workflow',
            'engine': 'CWL',
            'definitionUri': 's3://test-bucket/workflow-zips/minimal.zip',
            'tags': {'NGS360_workflow_id': 'ngs360-minimal-1'}
        }
        
        for key, value in expected_params.items():
            assert call_kwargs[key] == value


class TestNGS360EventHandler:
    """Test NGS360 event handler routing functionality."""

    @patch('ngs360_event_handler.create_workflow')
    def test_ngs360_event_handler_create_workflow(self, mock_create_workflow):
        """Test routing of create_workflow action."""
        mock_create_workflow.return_value = {
            'statusCode': 200,
            'workflow_id': 'wf-test123'
        }

        event = {
            'source': 'ngs360',
            'action': 'create_workflow',
            'cwl_s3_path': 's3://test-bucket/workflow.cwl',
            'name': 'test-workflow',
            'id': 'ngs360-test-1'
        }

        response = ngs360_event_handler.ngs360_event_handler(event)

        # Verify create_workflow was called
        mock_create_workflow.assert_called_once_with(event)
        assert response['statusCode'] == 200
        assert response['workflow_id'] == 'wf-test123'

    @patch('ngs360_event_handler.create_workflow_version')
    def test_ngs360_event_handler_create_workflow_version(self, mock_create_workflow_version):
        """Test routing of create_workflow_version action."""
        mock_create_workflow_version.return_value = {
            'statusCode': 200,
            'version_name': 'v2.0',
            'omics_workflow_id': 'wf-abc123456'
        }

        event = {
            'source': 'ngs360',
            'action': 'create_workflow_version',
            'omics_workflow_id': 'wf-abc123456',
            'version_name': 'v2.0',
            'cwl_s3_path': 's3://test-bucket/workflow-v2.cwl',
            'id': 'ngs360-version-1'
        }

        response = ngs360_event_handler.ngs360_event_handler(event)

        # Verify create_workflow_version was called
        mock_create_workflow_version.assert_called_once_with(event)
        assert response['statusCode'] == 200
        assert response['version_name'] == 'v2.0'
        assert response['omics_workflow_id'] == 'wf-abc123456'

    def test_ngs360_event_handler_unknown_action(self):
        """Test handling of unknown actions (expected error scenario)."""
        event = {
            'source': 'ngs360',
            'action': 'unknown_action',
            'cwl_s3_path': 's3://test-bucket/workflow.cwl'
        }

        response = ngs360_event_handler.ngs360_event_handler(event)

        assert response['statusCode'] == 400
        assert response['error'] == 'UnknownAction'
        assert 'unknown_action' in response['message']
        assert 'create_workflow' in response['message']
        assert 'create_workflow_version' in response['message']

    def test_ngs360_event_handler_missing_action(self):
        """Test handling of events with missing action (expected error scenario)."""
        event = {
            'source': 'ngs360',
            'cwl_s3_path': 's3://test-bucket/workflow.cwl',
            'name': 'test-workflow',
            'id': 'ngs360-test-2'
            # Missing action field
        }

        response = ngs360_event_handler.ngs360_event_handler(event)

        assert response['statusCode'] == 400
        assert response['error'] == 'UnknownAction'
        assert 'None' in response['message']  # action will be None

    def test_ngs360_event_handler_empty_action(self):
        """Test handling of events with empty action (expected error scenario)."""
        event = {
            'source': 'ngs360',
            'action': '',
            'cwl_s3_path': 's3://test-bucket/workflow.cwl',
            'name': 'test-workflow',
            'id': 'ngs360-test-3'
        }

        response = ngs360_event_handler.ngs360_event_handler(event)

        assert response['statusCode'] == 400
        assert response['error'] == 'UnknownAction'
        assert 'create_workflow, create_workflow_version' in response['message']


class TestNGS360Integration:
    """Test integration scenarios for NGS360 event handling."""

    @patch('ngs360_event_handler._process_workflow')
    @patch('ngs360_event_handler.omics_client')
    def test_full_workflow_creation_flow(self, mock_omics_client, mock_process_workflow):
        """Test complete workflow creation flow from event handler entry point."""
        # Set up mocks
        mock_process_workflow.return_value = 's3://test-bucket/workflow-zips/integration.zip'
        mock_omics_client.create_workflow.return_value = {
            'id': 'wf-integration123',
            'arn': 'arn:aws:omics:us-east-1:123456789012:workflow/wf-integration123'
        }

        # Complete event as would be received by lambda handler
        event = {
            'source': 'ngs360',
            'action': 'create_workflow',
            'cwl_s3_path': 's3://ngs360-workflows/production/alignment-pipeline.cwl',
            'name': 'alignment-pipeline-v2',
            'id': 'ngs360-alignment-pipeline-42'
        }

        # Call the main event handler
        response = ngs360_event_handler.ngs360_event_handler(event)

        # Verify successful response
        assert response['statusCode'] == 200
        assert response['workflow_id'] == 'wf-integration123'
        assert 'Workflow registered successfully with Docker images processed' in response['message']

        # Verify AWS API was called correctly
        mock_omics_client.create_workflow.assert_called_once()
        call_kwargs = mock_omics_client.create_workflow.call_args[1]
        
        assert call_kwargs['name'] == 'alignment-pipeline-v2'
        assert call_kwargs['engine'] == 'CWL'
        assert call_kwargs['definitionUri'] == 's3://test-bucket/workflow-zips/integration.zip'
        assert call_kwargs['tags']['NGS360_workflow_id'] == 'ngs360-alignment-pipeline-42'

    @patch('ngs360_event_handler._process_workflow')
    @patch('ngs360_event_handler.omics_client')
    def test_full_workflow_version_creation_flow(self, mock_omics_client, mock_process_workflow):
        """Test complete workflow version creation flow from event handler entry point."""
        # Set up mocks
        mock_process_workflow.return_value = 's3://test-bucket/workflow-zips/integration-v2.zip'
        mock_omics_client.create_workflow_version.return_value = {
            'id': 'wf-version-integration456',
            'arn': 'arn:aws:omics:us-east-1:123456789012:workflow/wf-abc123/version/wf-version-integration456',
            'versionName': 'v3.0-production'
        }

        # Complete event as would be received by lambda handler
        event = {
            'source': 'ngs360',
            'action': 'create_workflow_version',
            'omics_workflow_id': 'wf-abc123',
            'version_name': 'v3.0-production',
            'cwl_s3_path': 's3://ngs360-workflows/production/alignment-pipeline-v3.cwl',
            'id': 'ngs360-alignment-pipeline-v3-100'
        }

        # Call the main event handler
        response = ngs360_event_handler.ngs360_event_handler(event)

        # Verify successful response
        assert response['statusCode'] == 200
        assert response['version_name'] == 'v3.0-production'
        assert response['omics_workflow_id'] == 'wf-abc123'
        assert 'Workflow version created successfully with Docker images processed' in response['message']

        # Verify AWS API was called correctly
        mock_omics_client.create_workflow_version.assert_called_once()
        call_kwargs = mock_omics_client.create_workflow_version.call_args[1]
        
        assert call_kwargs['workflowId'] == 'wf-abc123'
        assert call_kwargs['versionName'] == 'v3.0-production'
        assert call_kwargs['definitionUri'] == 's3://test-bucket/workflow-zips/integration-v2.zip'
        assert call_kwargs['tags']['NGS360_workflow_id'] == 'ngs360-alignment-pipeline-v3-100'
