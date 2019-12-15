import boto3
import json
from flask import Blueprint

s3 = Blueprint('s3', __name__, url_prefix='/s3')
client = boto3.client('s3')


@s3.route('/buckets', methods=['GET'])
def get_buckets():
    response = client.list_buckets()
    bucket_names = []
    for bucket in response['Buckets']:
        bucket_names.append(bucket['Name'])
    return json.dumps(bucket_names)
