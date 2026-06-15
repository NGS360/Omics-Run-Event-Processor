import json
import os

import boto3

secrets_client = boto3.client('secretsmanager')


def get_auth_token():
    if os.environ.get("AUTH_TOKEN"):
        return os.environ.get("AUTH_TOKEN")

    # Retrieve API Server Auth Token from Secrets Manager
    secret_name = os.environ.get('ENV_SECRETS')
    if secret_name:
        get_secret_value_response = secrets_client.get_secret_value(
            SecretId=secret_name
        )
        secret_string = get_secret_value_response['SecretString']
        secret_dict = json.loads(secret_string)
        AUTH_TOKEN = secret_dict.get('AUTH_TOKEN')
        return AUTH_TOKEN

    return None
