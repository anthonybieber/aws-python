import threading
import logging
import time
import uuid
import json

logging.basicConfig(
    level=logging.DEBUG,
    format='(%(threadName)-10s) %(message)s',
)


class KinesisShardConsumerWorker(threading.Thread):
    def __init__(self, kinesis_client, s3_client, shard_id):
        super(KinesisShardConsumerWorker, self).__init__()

        self.kinesis_client = kinesis_client
        self.s3_client = s3_client
        self.shard_id = shard_id

    def run(self):
        logging.debug(f'Running worker for shard_id: {self.shard_id}')

        shard_iterator = self.kinesis_client.get_shard_iterator(
            StreamName='anthony_stream',
            ShardId=self.shard_id,
            ShardIteratorType='LATEST',
        )['ShardIterator']

        while True:
            response = self.kinesis_client.get_records(
                ShardIterator=shard_iterator,
                Limit=100
            )

            print(response)

            shard_iterator = response['NextShardIterator']

            combined_records_dict = []
            for record in response['Records']:
                record_metadata = {
                    "shard_id": self.shard_id,
                    "data": record['Data'].decode("utf-8"),
                    "sequence_number": record['SequenceNumber'],
                    "partition_key": record['PartitionKey']
                }
                print(record_metadata['data'])
                data = record['Data'].decode("utf-8")

                combined_records_dict.append(json.loads(data))

            if response['Records']:
                self.s3_client.put_object(
                    Bucket='anthony-kinesis-output',
                    Key='anthony_'+str(uuid.uuid4()),
                    Body=bytes(json.dumps(combined_records_dict), 'utf-8')
                )
            time.sleep(60)
