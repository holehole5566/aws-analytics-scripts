import boto3
import json
import requests
import os
from requests_aws4auth import AWS4Auth

from dotenv import load_dotenv # type: ignore
load_dotenv()

region = 'us-east-1'  # For example, us-west-1
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key,
                   region, service, session_token=credentials.token)

# The OpenSearch domain endpoint with https:// and without a trailing slash
url = os.environ.get("HTTP_URL")

payload = {
    "size": 1
}

# Elasticsearch 6.x requires an explicit Content-Type header
headers = {"Content-Type": "application/json"}

# Make the signed HTTP request
r = requests.get(
    url, auth=awsauth, headers=headers,data=payload)

print(r.status_code)
print(r.text)
