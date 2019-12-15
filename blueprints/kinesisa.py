import boto3
import json
from flask import Blueprint
from flask import request

kinesis = Blueprint('kinesis', __name__, url_prefix='/kinesis')
client = boto3.client('kinesis', region_name='us-east-1')


@kinesis.route('/streams', methods=['GET'])
def get_stream_names():
    response = client.list_streams()
    return json.dumps(response['StreamNames'])


@kinesis.route('/stream/<name>', methods=['GET'])
def get_stream_data(name):
    response = client.describe_stream(
        StreamName=name
    )
    return response


@kinesis.route('/stream/<name>/shards', methods=['GET'])
def get_number_of_stream_shards(name):
    response = client.list_shards(
        StreamName=name,
        ExclusiveStartShardId='shardId-000000000000'
    )
    return str(response['Shards'])


@kinesis.route('/stream', methods=['POST'])
def create_stream():
    '''
        curl -X POST http://127.0.0.1:5000/kinesis/stream
        -H 'Content-Type: application/json'
        -d '{"StreamName":"anthony_stream","ShardCount":1}'
    '''
    data = request.get_json()
    response = client.create_stream(
        StreamName=data['StreamName'],
        ShardCount=data['ShardCount']
    )

    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        return '{"created": true}'

    return '{"created": false}'
