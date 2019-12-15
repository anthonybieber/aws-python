import boto3
from kinesis.consumer_worker import KinesisShardConsumerWorker

client = boto3.client('kinesis', region_name='us-east-1')
response = client.list_shards(
    StreamName='anthony_stream',
    ExclusiveStartShardId='shardId-000000000000'
)

shard_ids = [shard['ShardId'] for shard in response['Shards']]

for shard_id in shard_ids:
    worker = KinesisShardConsumerWorker(
        boto3.client('kinesis', region_name='us-east-1'),
        boto3.client('s3'),
        shard_id
    )

    worker.start()


