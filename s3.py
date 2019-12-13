import boto3

# Get the s3 client, and list all buckets
client = boto3.client('s3')
response = client.list_buckets()
bucket_names = []
for bucket in response['Buckets']:
    bucket_names.append(bucket['Name'])

print(bucket_names)

if 'anthony-bieber-bucky' not in bucket_names:
    client.create_bucket(Bucket='anthony-bieber-bucky')
    exit(0)

print('Bucket already exists')

