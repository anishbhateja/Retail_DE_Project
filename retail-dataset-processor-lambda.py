import json
import base64
import boto3
from datetime import datetime


def lambda_handler(event, context):
    print(event)

    # Read data from the incoming kinesis data stream events and print the data
    for record in event['Records']:
        data = base64.b64decode(record['kinesis']['data']).decode('utf-8-sig')
        # print(data)
        # print(type(data))
        event_msg = json.loads(data)
        print(event_msg)

        invoice_num = str(event_msg["RetailTxnData"]["InvoiceNo"])
        # print(event_msg["RetailTxnData"])
        # print(type(invoice_num))

        store_num = str(event_msg["RetailTxnData"]["StoreNo"])
        region = str(event_msg["RetailTxnData"]["Region"])

        print("invoice_num : {}".format(invoice_num))
        print("store_num : {}".format(store_num))
        print("region : {}".format(region))

        print("region : {}".format(region)) 

        print("store_num : {}".format(store_num)) # 

        # processing_date = datetime.now().strftime("%Y-%m-%d")
        # processing_hour = datetime.now().strftime("%H:%M:%S")[0:2]

        invoice_processing_date = str(event_msg["RetailTxnData"]["InvoiceDateTime"])[:10]
        invoice_processing_hour = str(event_msg["RetailTxnData"]["InvoiceDateTime"])[11:13]

        # Use invoice date

        print(invoice_processing_date)
        print(type(invoice_processing_date))
        print(invoice_processing_hour)
        print(type(invoice_processing_hour))

        file_name = "retail_dataset_{region}_{store_num}_{invoice_num}.json".format(region=region, store_num=store_num,
                                                                                    invoice_num=invoice_num)
        # file= "s3://retail-dataset-training/archive/region={region}/store-num={store_num}/processing_date={processing_date}/processing_hour={processing_hour}".format(region=region,store_num=store_num,processing_date=processing_date,processing_hour=processing_hour)
        # s3://retail-dataset-training/archive/region=US/store-num=888/invoice_processing_date=2024-03-04/invoice_processing_hour=07/retail_dataset_US_888_084.json
        # print("Archiving source msg to file : {}".format(file))

        bucket_name = 'anish-retail-db'
        json_file_key = "archive/region={region}/store-num={store_num}/invoice_processing_date={invoice_processing_date}/invoice_processing_hour={invoice_processing_hour}/{file_name}".format(
            region=region, store_num=store_num, invoice_processing_date=invoice_processing_date, invoice_processing_hour=invoice_processing_hour,
            file_name=file_name)
        # message_to_put = {'key': 'value', 'another_key': 'another_value'}

        # Create an S3 client
        s3 = boto3.client('s3', region_name='us-west-1')

        # Convert the Python dictionary to a JSON string
        json_message = json.dumps(event_msg)

        # Put the JSON message into the S3 bucket
        s3.put_object(Body=json_message, Bucket=bucket_name, Key=json_file_key)

        print(f"Message successfully put into archive S3 file s3://: {bucket_name}/{json_file_key}")

        # delivery_stream_name = 'retail-dataset-firehose'

        # # Replace 'your_aws_access_key' and 'your_aws_secret_key' with your AWS credentials

        # aws_region = 'us-east-1'  # Replace with your AWS region

        # # Create a Kinesis Firehose client
        # firehose_client = boto3.client(
        #     'firehose',
        #     region_name=aws_region
        # )

        # # Sample record data (replace with your actual record data)
        # record_data = {
        #     "Data": json_message
        # }

        # # Put record into the Kinesis Firehose delivery stream
        # response = firehose_client.put_record(
        #     DeliveryStreamName=delivery_stream_name,
        #     Record=record_data
        # )

        # print(f"Record put successfully. Response: {response}")
