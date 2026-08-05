#!/usr/bin/env python3
"""
Unit tests for Batch job status updates posted to the NGS360 REST API.

Focused on authentication: these updates previously reached NGS360 with no
credential, which is the last blocker for phase 1c of the RBAC rollout.
"""

import os
import sys
from unittest.mock import patch

import pytest

sys.path.append('..')

os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
os.environ.setdefault('NGS360_API_SERVER', 'http://fake-ngs360')

import batch_event_handler  # noqa: E402
import utils  # noqa: E402


@pytest.fixture(autouse=True)
def clear_token_state():
    """utils caches per key across invocations -- deliberate in Lambda, but it
    would leak between tests."""
    utils._token_cache.clear()
    for key in (utils.GA4GH_TOKEN_KEY, utils.NGS360_TOKEN_KEY, 'ENV_SECRETS'):
        os.environ.pop(key, None)
    yield
    utils._token_cache.clear()
    for key in (utils.GA4GH_TOKEN_KEY, utils.NGS360_TOKEN_KEY, 'ENV_SECRETS'):
        os.environ.pop(key, None)


class TestCredentialSeparation:
    """GA4GH WES and NGS360 use different schemes and must not share a token."""

    def test_ga4gh_and_ngs360_tokens_are_distinct_keys(self):
        os.environ[utils.GA4GH_TOKEN_KEY] = 'ga4gh-token'
        os.environ[utils.NGS360_TOKEN_KEY] = 'ngs360_apikey'
        assert utils.get_auth_token() == 'ga4gh-token'
        assert utils.get_ngs360_token() == 'ngs360_apikey'

    def test_default_key_is_unchanged_for_existing_callers(self):
        """get_auth_token() with no argument must still return the GA4GH token."""
        os.environ[utils.GA4GH_TOKEN_KEY] = 'ga4gh-token'
        assert utils.get_auth_token() == 'ga4gh-token'

    @patch('utils.secrets_client.get_secret_value')
    def test_reads_requested_key_from_secret(self, mock_get):
        mock_get.return_value = {
            'SecretString': '{"AUTH_TOKEN": "ga4gh", "NGS360_API_TOKEN": "ngs360_k"}'
        }
        os.environ['ENV_SECRETS'] = 'some/secret'
        assert utils.get_ngs360_token() == 'ngs360_k'

    @patch('utils.secrets_client.get_secret_value')
    def test_secret_fetched_once_per_key(self, mock_get):
        mock_get.return_value = {'SecretString': '{"NGS360_API_TOKEN": "ngs360_k"}'}
        os.environ['ENV_SECRETS'] = 'some/secret'
        for _ in range(3):
            utils.get_ngs360_token()
        mock_get.assert_called_once()

    def test_returns_none_when_nothing_configured(self):
        assert utils.get_ngs360_token() is None


class TestPostJobAuthentication:

    @patch('batch_event_handler.requests.put')
    def test_authorization_header_sent_when_token_available(self, mock_put):
        os.environ[utils.NGS360_TOKEN_KEY] = 'ngs360_abc123'
        batch_event_handler.post_job('job-1', 'SUCCEEDED', 'stream')
        headers = mock_put.call_args.kwargs['headers']
        assert headers['Authorization'] == 'Bearer ngs360_abc123'

    @patch('batch_event_handler.requests.put')
    def test_request_still_sent_without_a_token(self, mock_put):
        """The endpoint is still open, so a missing credential must not block."""
        batch_event_handler.post_job('job-1', 'SUCCEEDED', 'stream')
        mock_put.assert_called_once()
        assert 'Authorization' not in mock_put.call_args.kwargs['headers']

    @patch('batch_event_handler.requests.put')
    def test_ga4gh_token_is_not_sent_to_ngs360(self, mock_put):
        """A GA4GH credential alone must not be used to authenticate to NGS360."""
        os.environ[utils.GA4GH_TOKEN_KEY] = 'ga4gh-token'
        batch_event_handler.post_job('job-1', 'SUCCEEDED', 'stream')
        assert 'Authorization' not in mock_put.call_args.kwargs['headers']

    @patch('batch_event_handler.requests.put')
    def test_token_is_never_logged(self, mock_put, caplog):
        os.environ[utils.NGS360_TOKEN_KEY] = 'ngs360_supersecret'
        with caplog.at_level('INFO'):
            batch_event_handler.post_job('job-1', 'SUCCEEDED', 'stream')
        assert 'ngs360_supersecret' not in caplog.text

    @patch('batch_event_handler.requests.put')
    def test_body_is_unchanged(self, mock_put):
        """Authentication must not alter the payload NGS360 receives."""
        os.environ[utils.NGS360_TOKEN_KEY] = 'ngs360_abc'
        batch_event_handler.post_job('job-1', 'SUCCEEDED', 'stream')
        assert mock_put.call_args.kwargs['json'] == {
            'status': 'SUCCEEDED',
            'log_stream_name': 'stream',
        }
