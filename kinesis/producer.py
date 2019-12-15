import boto3
import random
import json
import uuid
import time

from random import randint

client = boto3.client('kinesis', region_name='us-east-1')
name_list = ['Anthony', 'Daniel', 'Sabrina', 'Anna', 'Tom', 'Kaycee', 'Samantha', 'Stacey', 'Adam']
age_list = [25, 28, 51, 55, 29, 25, 30, 30, 29]

while True:
    number_of_records = randint(1, len(name_list))

    print(f'creating {number_of_records} records')
    for i in range(number_of_records):
        name = random.choice(name_list)
        age = random.choice(age_list)
        record = json.dumps({'name': name, 'age': age})

        bytes_record = bytes(record, 'utf-8')
        partition_key = str(uuid.uuid4())

        response = client.put_record(
            StreamName='anthony_stream',
            Data=bytes_record,
            PartitionKey=partition_key
        )

        response_string = json.dumps(response)
        print(f'Wrote record to stream: {response_string}')

    time.sleep(60)
