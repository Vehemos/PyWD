import cv2
import boto3
import numpy as np

# Load Credentials    
key_file = np.loadtxt("rootkey.csv", dtype=str, delimiter='=')

# Create SQS client
sqs = boto3.client('sqs', region_name='ap-south-1', aws_access_key_id=key_file[0][1] , aws_secret_access_key=key_file[1][1] )
queue_url = 'https://sqs.ap-south-1.amazonaws.com/377293540990/scheduler.fifo'
msg_url = 'https://sqs.ap-south-1.amazonaws.com/377293540990/msngr'

# Create S3 client
s3 = boto3.client('s3', aws_access_key_id=key_file[0][1] , aws_secret_access_key=key_file[1][1])
bucket = 'imageblockupload'

# Receive message from SQS queue
response = sqs.receive_message(
    QueueUrl=msg_url,
    MaxNumberOfMessages=1,
    MessageAttributeNames=[
        'All'
    ],
    VisibilityTimeout=0,
    WaitTimeSeconds=0
)

message = response['Messages'][0]
receipt_handle = message['ReceiptHandle']
msg_body = message['Body']

if(msg_body == "Files Uploaded."):
    print(message)
    # Delete received message from queue
    sqs.delete_message(
        QueueUrl=msg_url,
        ReceiptHandle=receipt_handle
    )
else:
    print("Waiting for msg")