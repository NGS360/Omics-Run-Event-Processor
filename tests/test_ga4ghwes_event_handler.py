#!/usr/bin/env python3
"""
Comprehensive unit tests for GA4GH WES event handler module.

This test suite covers all functions in ga4ghwes_event_handler.py including:
- submit_omics_run: Workflow submission to AWS Omics
- _validate_submission_request: Input validation
- _pingback_to_ga4ghwes: Callback functionality 
- ga4ghwes_event_handler: Main event routing
"""

import json
import os
import sys
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

# Add the parent directory to the path so we can import the module
sys.path.append('..')

# Set up required environment variables before importing
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
os.environ['VERBOSE_LOGGING'] = 'true'
os.environ['API_SERVER'] = 'https://test-api-server.com'
os.environ['OMICS_ROLE_ARN'] = 'arn:aws:iam::123456789012:role/TestOmicsRole'

# Mock boto3 clients before importing to avoid AWS credential issues
with patch('boto3.client') as mock_boto3_client:
    mock_boto3_client.return_value = MagicMock()
    import ga4ghwes_event_handler


class TestSubmitOmicsRun:
    """Test workflow submission to AWS Omics."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_omics_client = MagicMock()
        self.valid_event = {
            'action': 'submit_workflow',
            'wes_run_id': 'wes-run-123',
            'workflow_id': 'wf-abc123def456',
            'workflow_engine_parameters': {
                'outputUri': 's3://test-bucket/outputs/',
                'name': 'test-workflow-run'
            },
            'parameters': {
                'input_file': 's3://test-bucket/input.fastq',
                'reference_genome': 'GRCh38'
            },
            'tags': {
                'project': 'test-project',
                'environment': 'test'
            }
        }

    @patch('ga4ghwes_event_handler.omics_client')
    def test_submit_omics_run_success_minimal(self, mock_omics):
        """Test successful workflow submission with minimal parameters."""
        # Setup mock
        mock_omics.start_run.return_value = {'id': 'omics-run-789'}
        
        event = {
            'action': 'submit_workflow',
            'wes_run_id': 'wes-run-123',
            'workflow_id': 'wf-abc123',
            'workflow_engine_parameters': {
                'outputUri': 's3://test-bucket/outputs/'
            }
        }

        # Execute
        result = ga4ghwes_event_handler.submit_omics_run(event)

        # Assert
        assert result['statusCode'] == 200
        assert result['omics_run_id'] == 'omics-run-789'
        assert result['wes_run_id'] == 'wes-run-123'
        assert result['message'] == 'Workflow submitted successfully'
        assert result['output_uri'] == 's3://test-bucket/outputs/'

        # Verify omics client call
        mock_omics.start_run.assert_called_once()
        call_kwargs = mock_omics.start_run.call_args[1]
        assert call_kwargs['workflowId'] == 'wf-abc123'
        assert call_kwargs['roleArn'] == os.environ['OMICS_ROLE_ARN']
        assert call_kwargs['outputUri'] == 's3://test-bucket/outputs/'
        assert call_kwargs['name'] == 'wes-run-wes-run-123'  # default name
        assert call_kwargs['tags']['WESRunId'] == 'wes-run-123'
        assert call_kwargs['retentionMode'] == 'REMOVE'

    @patch('ga4ghwes_event_handler.omics_client')
    def test_submit_omics_run_success_full_parameters(self, mock_omics):
        """Test successful workflow submission with all parameters."""
        # Setup mock
        mock_omics.start_run.return_value = {'id': 'omics-run-789'}
        
        event = self.valid_event.copy()
        event['workflow_engine_parameters'].update({
            'workflowVersionName': 'v1.2.0',
            'cacheId': 'cache-123',
            'storageType': 'STATIC',
            'storageCapacity': 1000
        })

        # Execute
        result = ga4ghwes_event_handler.submit_omics_run(event)

        # Assert success response
        assert result['statusCode'] == 200
        assert result['omics_run_id'] == 'omics-run-789'

        # Verify all optional parameters passed
        call_kwargs = mock_omics.start_run.call_args[1]
        assert call_kwargs['workflowVersionName'] == 'v1.2.0'
        assert call_kwargs['cacheId'] == 'cache-123'
        assert call_kwargs['storageType'] == 'STATIC'
        assert call_kwargs['storageCapacity'] == 1000

    @patch('ga4ghwes_event_handler.omics_client')
    def test_submit_omics_run_task_name_from_workflow_params(self, mock_omics):
        """Test task name is taken from workflow engine parameters."""
        mock_omics.start_run.return_value = {'id': 'omics-run-789'}
        
        event = self.valid_event.copy()
        event['workflow_engine_parameters']['name'] = 'custom-task-name'

        ga4ghwes_event_handler.submit_omics_run(event)

        call_kwargs = mock_omics.start_run.call_args[1]
        assert call_kwargs['name'] == 'custom-task-name'

    @patch('ga4ghwes_event_handler.omics_client')
    def test_submit_omics_run_task_name_from_tags(self, mock_omics):
        """Test task name is taken from tags when not in workflow params."""
        mock_omics.start_run.return_value = {'id': 'omics-run-789'}
        
        event = self.valid_event.copy()
        del event['workflow_engine_parameters']['name']
        event['tags']['TaskName'] = 'tag-task-name'

        ga4ghwes_event_handler.submit_omics_run(event)

        call_kwargs = mock_omics.start_run.call_args[1]
        assert call_kwargs['name'] == 'tag-task-name'

    @patch('ga4ghwes_event_handler.omics_client')
    def test_submit_omics_run_validation_error(self, mock_omics):
        """Test workflow submission with validation error."""
        # Invalid event - missing outputUri
        event = {
            'action': 'submit_workflow',
            'wes_run_id': 'wes-run-123',
            'workflow_id': 'wf-abc123',
            'workflow_engine_parameters': {}  # Missing outputUri
        }

        result = ga4ghwes_event_handler.submit_omics_run(event)

        assert result['statusCode'] == 400
        assert result['error'] == 'ValidationError'
        assert 'outputUri' in result['message']
        mock_omics.start_run.assert_not_called()

    @patch('ga4ghwes_event_handler.omics_client')
    def test_submit_omics_run_omics_api_error(self, mock_omics):
        """Test handling of AWS Omics API errors."""
        # Setup mock to raise exception
        mock_omics.start_run.side_effect = Exception('Omics API error')

        result = ga4ghwes_event_handler.submit_omics_run(self.valid_event)

        assert result['statusCode'] == 500
        assert result['error'] == 'OmicsSubmissionError'
        assert result['message'] == 'Omics API error'


class TestValidateSubmissionRequest:
    """Test input validation for workflow submission requests."""

    def test_validate_submission_request_success(self):
        """Test successful validation with all required fields."""
        event = {
            'action': 'submit_workflow',
            'wes_run_id': 'wes-run-123',
            'workflow_id': 'wf-abc123',
            'workflow_engine_parameters': {
                'outputUri': 's3://test-bucket/outputs/'
            }
        }

        is_valid, error_msg = ga4ghwes_event_handler._validate_submission_request(event)
        
        assert is_valid is True
        assert error_msg is None

    @pytest.mark.parametrize("missing_field", [
        'action', 'wes_run_id', 'workflow_id', 'workflow_engine_parameters'
    ])
    def test_validate_submission_request_missing_required_fields(self, missing_field):
        """Test validation failure for missing required fields."""
        event = {
            'action': 'submit_workflow',
            'wes_run_id': 'wes-run-123',
            'workflow_id': 'wf-abc123',
            'workflow_engine_parameters': {
                'outputUri': 's3://test-bucket/outputs/'
            }
        }
        del event[missing_field]

        is_valid, error_msg = ga4ghwes_event_handler._validate_submission_request(event)
        
        assert is_valid is False
        assert f"Missing required field: {missing_field}" in error_msg

    def test_validate_submission_request_missing_output_uri(self):
        """Test validation failure for missing outputUri in workflow engine parameters."""
        event = {
            'action': 'submit_workflow',
            'wes_run_id': 'wes-run-123',
            'workflow_id': 'wf-abc123',
            'workflow_engine_parameters': {}  # Missing outputUri
        }

        is_valid, error_msg = ga4ghwes_event_handler._validate_submission_request(event)
        
        assert is_valid is False
        assert "Missing required field: outputUri" in error_msg

    def test_validate_submission_request_invalid_action(self):
        """Test validation failure for invalid action."""
        event = {
            'action': 'invalid_action',
            'wes_run_id': 'wes-run-123',
            'workflow_id': 'wf-abc123',
            'workflow_engine_parameters': {
                'outputUri': 's3://test-bucket/outputs/'
            }
        }

        is_valid, error_msg = ga4ghwes_event_handler._validate_submission_request(event)
        
        assert is_valid is False
        assert "Invalid action: invalid_action" in error_msg

    @pytest.mark.parametrize("workflow_id", [
        '', None, '   '  # Empty, None, or whitespace
    ])
    def test_validate_submission_request_invalid_workflow_id(self, workflow_id):
        """Test validation failure for invalid workflow_id."""
        event = {
            'action': 'submit_workflow',
            'wes_run_id': 'wes-run-123',
            'workflow_id': workflow_id,
            'workflow_engine_parameters': {
                'outputUri': 's3://test-bucket/outputs/'
            }
        }

        is_valid, error_msg = ga4ghwes_event_handler._validate_submission_request(event)
        
        assert is_valid is False
        assert "Invalid workflow_id" in error_msg


class TestPingbackToGA4GHWES:
    """Test callback functionality to GA4GH WES API."""

    def setup_method(self):
        """Set up test fixtures."""
        self.success_response = {
            'statusCode': 200,
            'omics_run_id': 'omics-run-789',
            'message': 'Workflow submitted successfully',
            'wes_run_id': 'wes-run-123'
        }
        
        self.error_response = {
            'statusCode': 500,
            'error': 'OmicsSubmissionError',
            'message': 'Omics API error'
        }

        self.event_with_callback = {
            'tags': {
                'callback_url': 'https://wes-api.example.com/callback',
                'WESRunId': 'wes-run-123'
            }
        }

    @patch('ga4ghwes_event_handler.requests.post')
    @patch('ga4ghwes_event_handler.get_auth_token')
    @patch('ga4ghwes_event_handler.datetime')
    def test_pingback_success_response(self, mock_datetime, mock_get_auth_token, mock_requests):
        """Test successful callback for successful workflow submission."""
        # Setup mocks
        mock_datetime.now.return_value.isoformat.return_value.replace.return_value = '2023-10-01T12:00:00Z'
        mock_get_auth_token.return_value = 'test-auth-token'
        mock_requests.return_value.raise_for_status.return_value = None

        ga4ghwes_event_handler._pingback_to_ga4ghwes(self.success_response, self.event_with_callback)

        # Verify request was made correctly
        mock_requests.assert_called_once()
        call_args = mock_requests.call_args
        
        assert call_args[1]['timeout'] == 10
        assert call_args[0][0] == 'https://wes-api.example.com/callback'
        
        # Check headers
        headers = call_args[1]['headers']
        assert headers['Content-Type'] == 'application/json'
        assert headers['X-Internal-API-Key'] == 'test-auth-token'
        
        # Check request data
        request_data = json.loads(call_args[1]['data'])
        assert request_data['wes_run_id'] == 'wes-run-123'
        assert request_data['status'] == 'PENDING'
        assert request_data['omics_run_id'] == 'omics-run-789'
        assert request_data['event_time'] == '2023-10-01T12:00:00Z'

    @patch('ga4ghwes_event_handler.requests.post')
    @patch('ga4ghwes_event_handler.get_auth_token')
    @patch('ga4ghwes_event_handler.datetime')
    def test_pingback_error_response(self, mock_datetime, mock_get_auth_token, mock_requests):
        """Test callback for failed workflow submission."""
        # Setup mocks
        mock_datetime.now.return_value.isoformat.return_value.replace.return_value = '2023-10-01T12:00:00Z'
        mock_get_auth_token.return_value = 'test-auth-token'
        mock_requests.return_value.raise_for_status.return_value = None

        ga4ghwes_event_handler._pingback_to_ga4ghwes(self.error_response, self.event_with_callback)

        # Check request data
        call_args = mock_requests.call_args
        request_data = json.loads(call_args[1]['data'])
        assert request_data['status'] == 'FAILED'
        assert request_data['failure_reason'] == 'Omics API error'
        assert 'omics_run_id' not in request_data

    def test_pingback_no_callback_url(self):
        """Test that no callback is made when callback_url is missing."""
        event_no_callback = {'tags': {'WESRunId': 'wes-run-123'}}  # Missing callback_url

        with patch('ga4ghwes_event_handler.requests.post') as mock_requests:
            ga4ghwes_event_handler._pingback_to_ga4ghwes(self.success_response, event_no_callback)
            mock_requests.assert_not_called()

    def test_pingback_no_wes_run_id(self):
        """Test that no callback is made when WESRunId is missing."""
        event_no_run_id = {'tags': {'callback_url': 'https://wes-api.example.com/callback'}}

        with patch('ga4ghwes_event_handler.requests.post') as mock_requests:
            ga4ghwes_event_handler._pingback_to_ga4ghwes(self.success_response, event_no_run_id)
            mock_requests.assert_not_called()

    def test_pingback_no_tags(self):
        """Test that no callback is made when tags are missing."""
        event_no_tags = {}

        with patch('ga4ghwes_event_handler.requests.post') as mock_requests:
            ga4ghwes_event_handler._pingback_to_ga4ghwes(self.success_response, event_no_tags)
            mock_requests.assert_not_called()

    @patch('ga4ghwes_event_handler.requests.post')
    @patch('ga4ghwes_event_handler.get_auth_token')
    def test_pingback_request_exception(self, mock_get_auth_token, mock_requests):
        """Test handling of request exceptions during callback."""
        mock_get_auth_token.return_value = 'test-auth-token'
        mock_requests.side_effect = Exception('Network error')

        # Should not raise exception
        ga4ghwes_event_handler._pingback_to_ga4ghwes(self.success_response, self.event_with_callback)


class TestGA4GHWESEventHandler:
    """Test main event handler routing."""

    @patch('ga4ghwes_event_handler.submit_omics_run')
    @patch('ga4ghwes_event_handler._pingback_to_ga4ghwes')
    def test_ga4ghwes_event_handler_submit_workflow(self, mock_pingback, mock_submit):
        """Test routing of submit_workflow action."""
        mock_submit.return_value = {
            'statusCode': 200,
            'omics_run_id': 'omics-run-789'
        }

        event = {
            'action': 'submit_workflow',
            'wes_run_id': 'wes-run-123',
            'workflow_id': 'wf-abc123'
        }

        result = ga4ghwes_event_handler.ga4ghwes_event_handler(event)

        # Verify correct functions called
        mock_submit.assert_called_once_with(event)
        mock_pingback.assert_called_once_with(mock_submit.return_value, event)
        
        # Verify response
        assert result['statusCode'] == 200
        assert result['omics_run_id'] == 'omics-run-789'

    def test_ga4ghwes_event_handler_unknown_action(self):
        """Test handling of unknown actions."""
        event = {
            'action': 'unknown_action',
            'wes_run_id': 'wes-run-123'
        }

        result = ga4ghwes_event_handler.ga4ghwes_event_handler(event)

        assert result['statusCode'] == 400
        assert result['error'] == 'UnknownAction'
        assert 'Unknown Action in GA4GHWES Event Handler: unknown_action' in result['message']

    def test_ga4ghwes_event_handler_missing_action(self):
        """Test handling of events with missing action field."""
        event = {
            'wes_run_id': 'wes-run-123',
            'workflow_id': 'wf-abc123'
        }

        result = ga4ghwes_event_handler.ga4ghwes_event_handler(event)

        assert result['statusCode'] == 400
        assert result['error'] == 'UnknownAction'
        assert 'Unknown Action in GA4GHWES Event Handler: None' in result['message']


class TestIntegration:
    """Integration tests combining multiple components."""

    @patch('ga4ghwes_event_handler.omics_client')
    @patch('ga4ghwes_event_handler.requests.post')
    @patch('ga4ghwes_event_handler.get_auth_token')
    def test_full_workflow_submission_flow(self, mock_get_auth_token, mock_requests, mock_omics):
        """Test complete workflow submission including callback."""
        # Setup mocks
        mock_omics.start_run.return_value = {'id': 'omics-run-789'}
        mock_get_auth_token.return_value = 'test-auth-token'
        mock_requests.return_value.raise_for_status.return_value = None

        event = {
            'action': 'submit_workflow',
            'wes_run_id': 'wes-run-123',
            'workflow_id': 'wf-abc123def456',
            'workflow_engine_parameters': {
                'outputUri': 's3://test-bucket/outputs/',
                'name': 'integration-test-run'
            },
            'parameters': {'input_file': 's3://test-bucket/input.fastq'},
            'tags': {
                'callback_url': 'https://wes-api.example.com/callback',
                'WESRunId': 'wes-run-123',
                'project': 'integration-test'
            }
        }

        result = ga4ghwes_event_handler.ga4ghwes_event_handler(event)

        # Verify workflow submission
        assert result['statusCode'] == 200
        assert result['omics_run_id'] == 'omics-run-789'
        
        # Verify Omics API was called correctly
        mock_omics.start_run.assert_called_once()
        omics_call_kwargs = mock_omics.start_run.call_args[1]
        assert omics_call_kwargs['workflowId'] == 'wf-abc123def456'
        assert omics_call_kwargs['name'] == 'integration-test-run'
        assert omics_call_kwargs['outputUri'] == 's3://test-bucket/outputs/'
        assert omics_call_kwargs['tags']['WESRunId'] == 'wes-run-123'
        assert omics_call_kwargs['tags']['project'] == 'integration-test'
        
        # Verify callback was made
        mock_requests.assert_called_once()
        callback_call_args = mock_requests.call_args
        assert callback_call_args[0][0] == 'https://wes-api.example.com/callback'
        
        callback_data = json.loads(callback_call_args[1]['data'])
        assert callback_data['wes_run_id'] == 'wes-run-123'
        assert callback_data['status'] == 'PENDING'
        assert callback_data['omics_run_id'] == 'omics-run-789'


if __name__ == '__main__':
    pytest.main([__file__])