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

# Import the ngs360_event_handler module
import ngs360_event_handler


class TestNGS360WorkflowRegistration:
    """Test NGS360 workflow registration functionality."""

    @patch('ngs360_event_handler.omics_client')
    def test_register_workflow_success(self, mock_omics_client):
        """Test successful workflow registration."""
        # Set up mock
        mock_omics_client.create_workflow.return_value = {
            'id': 'wf-abc123456',
            'arn': 'arn:aws:omics:us-east-1:123456789012:workflow/wf-abc123456'
        }

        # Valid event
        event = {
            'action': 'register_workflow',
            'cwl_s3_path': 's3://test-bucket/workflows/hello-world.cwl',
            'name': 'hello-world-workflow',
            'id': 'ngs360-workflow-123'
        }

        response = ngs360_event_handler.register_workflow(event)

        # Verify response
        assert response['statusCode'] == 200
        assert response['workflow_id'] == 'wf-abc123456'
        assert response['message'] == 'Workflow registered successfully'

        # Verify create_workflow was called with correct parameters
        mock_omics_client.create_workflow.assert_called_once()
        call_kwargs = mock_omics_client.create_workflow.call_args[1]

        assert call_kwargs['name'] == 'hello-world-workflow'
        assert call_kwargs['engine'] == 'CWL'
        assert call_kwargs['definitionUri'] == 's3://test-bucket/workflows/hello-world.cwl'
        assert call_kwargs['tags']['NGS360_workflow_id'] == 'ngs360-workflow-123'

    def test_register_workflow_missing_cwl_s3_path(self):
        """Test workflow registration with missing cwl_s3_path."""
        event = {
            'action': 'register_workflow',
            'name': 'hello-world-workflow',
            'id': 'ngs360-workflow-123'
            # Missing cwl_s3_path
        }

        response = ngs360_event_handler.register_workflow(event)

        assert response['statusCode'] == 400
        assert response['error'] == 'ValidationError'
        assert response['message'] == 'cwl_s3_path is required but not provided.'

    def test_register_workflow_missing_name(self):
        """Test workflow registration with missing name."""
        event = {
            'action': 'register_workflow',
            'cwl_s3_path': 's3://test-bucket/workflows/hello-world.cwl',
            'id': 'ngs360-workflow-123'
            # Missing name
        }

        response = ngs360_event_handler.register_workflow(event)

        assert response['statusCode'] == 400
        assert response['error'] == 'ValidationError'
        assert response['message'] == 'name is required but not provided.'

    def test_register_workflow_missing_id(self):
        """Test workflow registration with missing id."""
        event = {
            'action': 'register_workflow',
            'cwl_s3_path': 's3://test-bucket/workflows/hello-world.cwl',
            'name': 'hello-world-workflow'
            # Missing id
        }

        response = ngs360_event_handler.register_workflow(event)

        assert response['statusCode'] == 400
        assert response['error'] == 'ValidationError'
        assert response['message'] == 'id is required but not provided.'

    @pytest.mark.parametrize("event,expected_field", [
        # Missing all required fields
        ({'action': 'register_workflow'}, 'cwl_s3_path'),
        # Missing name and id
        ({'action': 'register_workflow', 'cwl_s3_path': 's3://bucket/workflow.cwl'}, 'name'),
        # Missing cwl_s3_path and id
        ({'action': 'register_workflow', 'name': 'test-workflow'}, 'cwl_s3_path'),
    ])
    def test_register_workflow_missing_multiple_fields(self, event, expected_field):
        """Test workflow registration with multiple missing fields scenarios."""
        response = ngs360_event_handler.register_workflow(event)
        assert response['statusCode'] == 400
        assert response['error'] == 'ValidationError'
        assert 'is required but not provided' in response['message']

    @patch('ngs360_event_handler.omics_client')
    def test_register_workflow_omics_api_error(self, mock_omics_client):
        """Test workflow registration with AWS Omics API error (expected error scenario)."""
        # Set up mock to raise exception - this is testing error handling
        mock_omics_client.create_workflow.side_effect = Exception("InvalidWorkflowDefinitionException: Invalid CWL definition")

        event = {
            'action': 'register_workflow',
            'cwl_s3_path': 's3://test-bucket/workflows/invalid.cwl',
            'name': 'invalid-workflow',
            'id': 'ngs360-workflow-456'
        }

        response = ngs360_event_handler.register_workflow(event)

        assert response['statusCode'] == 500
        assert response['error'] == 'OmicsCreateWorkflowError'
        assert 'InvalidWorkflowDefinitionException' in response['message']

    @patch('ngs360_event_handler.omics_client')
    def test_register_workflow_with_minimal_event(self, mock_omics_client):
        """Test workflow registration with minimal required fields only."""
        # Set up mock
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

        response = ngs360_event_handler.register_workflow(event)

        # Verify successful response
        assert response['statusCode'] == 200
        assert response['workflow_id'] == 'wf-minimal123'

        # Verify API was called with correct minimal parameters
        mock_omics_client.create_workflow.assert_called_once()
        call_kwargs = mock_omics_client.create_workflow.call_args[1]
        
        expected_params = {
            'name': 'minimal-workflow',
            'engine': 'CWL', 
            'definitionUri': 's3://test-bucket/minimal.cwl',
            'tags': {'NGS360_workflow_id': 'ngs360-minimal-1'}
        }
        
        for key, value in expected_params.items():
            assert call_kwargs[key] == value


class TestNGS360EventHandler:
    """Test NGS360 event handler routing functionality."""

    @patch('ngs360_event_handler.register_workflow')
    def test_ngs360_event_handler_register_workflow(self, mock_register_workflow):
        """Test routing of register_workflow action."""
        mock_register_workflow.return_value = {
            'statusCode': 200,
            'workflow_id': 'wf-test123'
        }

        event = {
            'source': 'ngs360',
            'action': 'register_workflow',
            'cwl_s3_path': 's3://test-bucket/workflow.cwl',
            'name': 'test-workflow',
            'id': 'ngs360-test-1'
        }

        response = ngs360_event_handler.ngs360_event_handler(event)

        # Verify register_workflow was called
        mock_register_workflow.assert_called_once_with(event)
        assert response['statusCode'] == 200
        assert response['workflow_id'] == 'wf-test123'

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
        assert 'register_workflow' in response['message']

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
        assert 'Supported actions: register_workflow' in response['message']


class TestNGS360Integration:
    """Test integration scenarios for NGS360 event handling."""

    @patch('ngs360_event_handler.omics_client')
    def test_full_workflow_registration_flow(self, mock_omics_client):
        """Test complete workflow registration flow from event handler entry point."""
        # Set up mock
        mock_omics_client.create_workflow.return_value = {
            'id': 'wf-integration123',
            'arn': 'arn:aws:omics:us-east-1:123456789012:workflow/wf-integration123'
        }

        # Complete event as would be received by lambda handler
        event = {
            'source': 'ngs360',
            'action': 'register_workflow',
            'cwl_s3_path': 's3://ngs360-workflows/production/alignment-pipeline.cwl',
            'name': 'alignment-pipeline-v2',
            'id': 'ngs360-alignment-pipeline-42'
        }

        # Call the main event handler
        response = ngs360_event_handler.ngs360_event_handler(event)

        # Verify successful response
        assert response['statusCode'] == 200
        assert response['workflow_id'] == 'wf-integration123'
        assert response['message'] == 'Workflow registered successfully'

        # Verify AWS API was called correctly
        mock_omics_client.create_workflow.assert_called_once()
        call_kwargs = mock_omics_client.create_workflow.call_args[1]
        
        assert call_kwargs['name'] == 'alignment-pipeline-v2'
        assert call_kwargs['engine'] == 'CWL'
        assert call_kwargs['definitionUri'] == 's3://ngs360-workflows/production/alignment-pipeline.cwl'
        assert call_kwargs['tags']['NGS360_workflow_id'] == 'ngs360-alignment-pipeline-42'

    @patch('ngs360_event_handler.omics_client')
    def test_workflow_registration_with_special_characters(self, mock_omics_client):
        """Test workflow registration with special characters in names and paths."""
        # Set up mock
        mock_omics_client.create_workflow.return_value = {
            'id': 'wf-special123',
            'arn': 'arn:aws:omics:us-east-1:123456789012:workflow/wf-special123'
        }

        event = {
            'action': 'register_workflow',
            'cwl_s3_path': 's3://test-bucket/workflows/rna-seq_v1.2.cwl',
            'name': 'RNA-Seq Analysis Pipeline (v1.2)',
            'id': 'ngs360-rna-seq-v1.2_special'
        }

        response = ngs360_event_handler.register_workflow(event)

        assert response['statusCode'] == 200
        assert response['workflow_id'] == 'wf-special123'

        # Verify special characters are preserved in API call
        call_kwargs = mock_omics_client.create_workflow.call_args[1]
        assert call_kwargs['name'] == 'RNA-Seq Analysis Pipeline (v1.2)'
        assert call_kwargs['definitionUri'] == 's3://test-bucket/workflows/rna-seq_v1.2.cwl'
        assert call_kwargs['tags']['NGS360_workflow_id'] == 'ngs360-rna-seq-v1.2_special'

    @pytest.mark.parametrize("missing_field,expected_error", [
        ('cwl_s3_path', 'cwl_s3_path is required'),
        ('name', 'name is required'), 
        ('id', 'id is required'),
    ])
    def test_workflow_registration_field_validation_order(self, missing_field, expected_error):
        """Test that field validation happens in the expected order."""
        base_event = {
            'action': 'register_workflow',
            'cwl_s3_path': 's3://test-bucket/workflow.cwl',
            'name': 'test-workflow',
            'id': 'ngs360-test-id'
        }
        
        # Remove the field we want to test
        event = {k: v for k, v in base_event.items() if k != missing_field}
        
        response = ngs360_event_handler.register_workflow(event)
        assert response['statusCode'] == 400
        assert expected_error in response['message']
