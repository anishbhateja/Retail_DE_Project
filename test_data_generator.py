import boto3
import json
import random
import string
import os


def list_s3_objects(bucket_name, prefix):
    """
    List all S3 objects in a specified prefix.
    """
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    obj_list = []
    for obj in response['Contents']:
        if obj['Key'].endswith('.json'):
            obj_list.append(obj['Key'])

    return obj_list

    

def read_json_from_s3(bucket_name, key):
    """
    Read JSON data from a specified S3 object.
    """
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=key)
    return json.loads(response['Body'].read().decode('utf-8'))

def generate_random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
def put_json_to_kinesis(data, stream_name):
    """
    Put JSON data to a Kinesis stream.
    """
    kinesis = boto3.client('kinesis')
    data = json.dumps(data)
    response = kinesis.put_record(StreamName=stream_name, Data=data, PartitionKey=generate_random_string(10))
    print("Data put to Kinesis stream:", response)


def main():
    # Replace these values with your own
    bucket_name = 'anish-retail-db'
    prefix = 'test-data'
    stream_name = 'test-kinesis_stream999'

    # Step 1: List all S3 objects in the prefix
    s3_objects = list_s3_objects(bucket_name, prefix)
    print("S3 Objects in prefix:", s3_objects)

    # Step 2: Read JSON data from each S3 object and put it to Kinesis stream
    for s3_object in s3_objects:
        json_data = read_json_from_s3(bucket_name, s3_object)
        print("Data read from S3 object:", json_data)
        print("Data Read Compl1")
        for txn in json_data:
            put_json_to_kinesis(txn, stream_name)


if __name__ == "__main__":
    os.environ["AWS_PROFILE"] = "dev_user_anish"
    os.environ["AWS_REGION"] = "us-west-1"  # Set your preferred AWS region
    main()
