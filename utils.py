import json
import os

import boto3

secrets_client = boto3.client('secretsmanager')

# Two different services are called from this Lambda and they do not share a
# credential:
#
#   AUTH_TOKEN        GA4GH WES, sent as the X-Internal-API-Key header
#   NGS360_API_TOKEN  NGS360 REST API, sent as Authorization: Bearer
#
# Both live in the secret named by ENV_SECRETS. Sending one to the other service
# would simply fail to authenticate, so the key name is explicit at every call.
GA4GH_TOKEN_KEY = 'AUTH_TOKEN'
NGS360_TOKEN_KEY = 'NGS360_API_TOKEN'

# Cached across invocations: Lambda reuses the execution context, so the secret
# is read once per container rather than on every event.
_token_cache = {}


def get_auth_token(key=GA4GH_TOKEN_KEY):
    """
    Return a credential from the ENV_SECRETS secret, or None if not configured.

    An environment variable of the same name takes precedence, which keeps local
    runs and tests from needing Secrets Manager.

    Defaults to the GA4GH WES token so existing callers are unaffected.
    """
    if key in _token_cache:
        return _token_cache[key]

    token = os.environ.get(key)

    if not token:
        secret_name = os.environ.get('ENV_SECRETS')
        if secret_name:
            get_secret_value_response = secrets_client.get_secret_value(
                SecretId=secret_name
            )
            secret_string = get_secret_value_response['SecretString']
            secret_dict = json.loads(secret_string)
            token = secret_dict.get(key)

    _token_cache[key] = token
    return token


def get_ngs360_token():
    """Credential for the NGS360 REST API."""
    return get_auth_token(NGS360_TOKEN_KEY)
