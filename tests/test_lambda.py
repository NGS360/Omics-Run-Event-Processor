#!/usr/bin/env python3
"""
Pytest-based unit tests for the main Lambda handler routing logic.

This script tests only the lambda_handler function since all other functions
have been moved to their respective event handler modules.
"""

import json
import os
import sys
import pytest
from unittest.mock import MagicMock, patch

# Add the parent directory to the path so we can import the lambda module
sys.path.append('..')

# Set up required environment variables before importing lambda module
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
os.environ['VERBOSE_LOGGING'] = 'true'

# Mock boto3 clients before importing lambda module to avoid AWS credential issues
with patch('boto3.client') as mock_boto3_client:
    # Configure mock clients
    mock_boto3_client.return_value = MagicMock()

    # Import the lambda module with a different name to avoid keyword conflict
    import importlib
    lambda_func = importlib.import_module('lambda')


class TestLambdaHandlerRouting:
    """Test event routing in main lambda handler."""

    @patch('lambda.ga4ghwes_event_handler')
    def test_lambda_handler_ga4ghwes_routing(self, mock_ga4ghwes_handler):
        """Test routing of GA4GH WES events."""
        mock_ga4ghwes_handler.return_value = {
            'statusCode': 200,
            'omics_run_id': '123'
        }

        event = {
            'source': 'ga4ghwes',
            'action': 'submit_workflow',
            'wes_run_id': 'test-run',
            'workflow_id': '123'
        }

        response = lambda_func.lambda_handler(event, None)

        mock_ga4ghwes_handler.assert_called_once_with(event)
        assert response['statusCode'] == 200
        assert response['omics_run_id'] == '123'

    @patch('lambda.ngs360_event_handler')
    def test_lambda_handler_ngs360_routing(self, mock_ngs360_handler):
        """Test routing of NGS360 events."""
        mock_ngs360_handler.return_value = {
            'statusCode': 200,
            'workflow_id': 'wf-abc123'
        }

        event = {
            'source': 'ngs360',
            'action': 'register_workflow',
            'cwl_s3_path': 's3://bucket/workflow.cwl',
            'name': 'test-workflow',
            'id': 'ngs360-123'
        }

        response = lambda_func.lambda_handler(event, None)

        mock_ngs360_handler.assert_called_once_with(event)
        assert response['statusCode'] == 200
        assert response['workflow_id'] == 'wf-abc123'

    @patch('lambda.omics_event_handler')
    def test_lambda_handler_omics_routing(self, mock_omics_handler):
        """Test routing of AWS Omics EventBridge events."""
        mock_omics_handler.return_value = {'statusCode': 200}

        event = {
            'source': 'aws.omics',
            'detail-type': 'Run Status Change',
            'detail': {'status': 'RUNNING', 'runId': '123'}
        }

        response = lambda_func.lambda_handler(event, None)

        mock_omics_handler.assert_called_once_with(event)
        assert response['statusCode'] == 200

    @patch('lambda.batch_event_handler')
    def test_lambda_handler_batch_routing(self, mock_batch_handler):
        """Test routing of AWS Batch events."""
        mock_batch_handler.return_value = {'statusCode': 200}

        event = {
            'source': 'aws.batch',
            'version': '0',
            'detail': {
                'jobId': 'asdf-1234',
                'jobName': 'test-job',
                'status': 'SUCCEEDED',
                'container': {
                    'logStreamName': 'test-log-stream'
                }
            }
        }

        response = lambda_func.lambda_handler(event, None)

        mock_batch_handler.assert_called_once_with(event)
        assert response['statusCode'] == 200

    def test_lambda_handler_unknown_event(self):
        """Test handling of unknown event types (expected error scenario)."""
        event = {
            'unknown_field': 'unknown_value'
        }

        response = lambda_func.lambda_handler(event, None)

        assert response['statusCode'] == 400
        assert response['error'] == 'UnknownEventType'
        assert 'Unable to determine event type' in response['message']

    def test_lambda_handler_missing_source(self):
        """Test handling of events with missing source field (expected error scenario)."""
        event = {
            'action': 'some_action',
            'data': 'some_data'
        }

        response = lambda_func.lambda_handler(event, None)

        assert response['statusCode'] == 400
        assert response['error'] == 'UnknownEventType'

    @pytest.mark.parametrize("source,handler_name", [
        ('ga4ghwes', 'ga4ghwes_event_handler'),
        ('ngs360', 'ngs360_event_handler'), 
        ('aws.omics', 'omics_event_handler'),
        ('aws.batch', 'batch_event_handler'),
    ])
    def test_lambda_handler_routing_parameters(self, source, handler_name):
        """Test that each source routes to the correct handler."""
        event = {'source': source, 'test': 'data'}

        with patch(f'lambda.{handler_name}') as mock_handler:
            mock_handler.return_value = {'statusCode': 200, 'test': 'response'}

            response = lambda_func.lambda_handler(event, None)

            mock_handler.assert_called_once_with(event)
            assert response['statusCode'] == 200

    @patch('lambda.ga4ghwes_event_handler')
    def test_lambda_handler_exception_handling(self, mock_ga4ghwes_handler):
        """Test exception handling in main lambda handler."""
        # Make the handler raise an exception
        mock_ga4ghwes_handler.side_effect = Exception("Handler failed")

        event = {
            'source': 'ga4ghwes',
            'action': 'submit_workflow'
        }

        response = lambda_func.lambda_handler(event, None)

        assert response['statusCode'] == 500
        assert response['error'] == 'InternalError'
        assert 'Lambda handler error' in response['message']
        assert 'Handler failed' in response['message']

    def test_lambda_handler_event_logging(self):
        """Test that events are properly logged (basic functionality test)."""
        event = {'source': 'unknown', 'test_data': 'value'}

        # This should not raise an exception and should return error response
        response = lambda_func.lambda_handler(event, None)
        
        # Should handle unknown source gracefully
        assert response['statusCode'] == 400
        assert response['error'] == 'UnknownEventType'

    @pytest.mark.parametrize("event_data", [
        {'source': 'ga4ghwes', 'action': 'submit_workflow'},
        {'source': 'ngs360', 'action': 'register_workflow'}, 
        {'source': 'aws.omics', 'detail-type': 'Run Status Change'},
        {'source': 'aws.batch', 'detail': {'jobId': 'test'}},
    ])
    def test_lambda_handler_source_detection(self, event_data):
        """Test that the lambda handler correctly detects different event sources."""

        # Patch all handlers to avoid actual calls
        with patch('lambda.ga4ghwes_event_handler') as mock_ga4gh, \
             patch('lambda.ngs360_event_handler') as mock_ngs360, \
             patch('lambda.omics_event_handler') as mock_omics, \
             patch('lambda.batch_event_handler') as mock_batch:

            # Set all handlers to return success
            for mock_handler in [mock_ga4gh, mock_ngs360, mock_omics, mock_batch]:
                mock_handler.return_value = {'statusCode': 200}

            response = lambda_func.lambda_handler(event_data, None)
            
            # Should route correctly and return success
            assert response['statusCode'] == 200


class TestContextHandling:
    """Test lambda context handling."""

    @patch('lambda.ga4ghwes_event_handler')
    def test_lambda_handler_with_context(self, mock_ga4ghwes_handler):
        """Test lambda handler with mock context object."""
        mock_ga4ghwes_handler.return_value = {'statusCode': 200}

        # Mock context object  
        class MockContext:
            def __init__(self):
                self.aws_request_id = 'test-request-123'
                self.function_name = 'test-function'
                self.remaining_time_in_millis = lambda: 30000

        context = MockContext()
        event = {'source': 'ga4ghwes', 'action': 'test'}

        response = lambda_func.lambda_handler(event, context)

        mock_ga4ghwes_handler.assert_called_once_with(event)
        assert response['statusCode'] == 200

    @patch('lambda.ngs360_event_handler')  
    def test_lambda_handler_context_none(self, mock_ngs360_handler):
        """Test lambda handler with None context (should work fine)."""
        mock_ngs360_handler.return_value = {'statusCode': 200}

        event = {'source': 'ngs360', 'action': 'register_workflow'}

        response = lambda_func.lambda_handler(event, None)

        mock_ngs360_handler.assert_called_once_with(event)
        assert response['statusCode'] == 200
