import cv2
import sys
import boto3
import socket
import numpy as np
import progressbar
from PIL import Image
from time import sleep
import matplotlib.pyplot as plt

from detector import detect_image
# Load Credentials    
key_file = np.loadtxt("rootkey.csv", dtype=str, delimiter='=')

# Create SQS client
sqs = boto3.client('sqs', region_name='ap-south-1', aws_access_key_id=key_file[0][1] , aws_secret_access_key=key_file[1][1] )
queue_url = 'https://sqs.ap-south-1.amazonaws.com/377293540990/scheduler.fifo'
msg_url = 'https://sqs.ap-south-1.amazonaws.com/377293540990/msngr'
worker_url = 'https://sqs.ap-south-1.amazonaws.com/377293540990/worker'
# Create S3 client
s3 = boto3.client('s3', aws_access_key_id=key_file[0][1] , aws_secret_access_key=key_file[1][1])
bucket = 'imageblockupload'
region="ap-south-1"

ans = 1
image_name = None

def chunk_processor(img, img_name):
    bbox=[]
    
    pimg = Image.fromarray(img)
    response, bbox = detect_image(pimg)
    
    for i in bbox:
        cv2.rectangle(img, (i[0],i[1]), (i[0] + i[2], i[1] + i[3]), (255, 0, 0), 3)
    
    cv2.imwrite("0" + img_name, img)
    
    plt.imshow(img, interpolation='nearest')
    plt.show()
    
    return img

def main(img_name):
    
    s3.download_file(bucket, img_name, img_name)
    
    chunks = chunk_processor(cv2.imread(img_name), img_name)
    
    cv2.imwrite(img_name, chunks)
    
    # Upload File
    s3.upload_file(img_name, bucket, "0" + img_name)
    
    response = sqs.send_message(
        QueueUrl=worker_url,
        DelaySeconds=0,
        MessageAttributes={
                'Worker': {
                    'DataType': 'String',
                    'StringValue': socket.gethostname(),
                }
            },
            MessageBody=(
                'Worker Offline.'
            )
        )
        
    print("=====================")
    print("Worker is now OFFLINE")
    print("=====================")
    print(response)
    print("")
        

def getWork():
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
    
    try:
        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']
        msg_body = message['Body']   
    
        if(msg_body == "Files Uploaded."):
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                MessageAttributeNames=[
                    'All'
                ],
                VisibilityTimeout=30,
                WaitTimeSeconds=0
            )
        
            try:
                message = response['Messages'][0]
            except KeyError:
                print(">>Unable to find files, maybe queue is empty or retry.<<")
                print(response)
                return None
            
            try:
                img_name = message['MessageAttributes']['img']['StringValue']
                receipt_handle = message['ReceiptHandle']
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle
                )
            except KeyError:
                print(">>Unable to find files, please check connection or retry.<<")
                return None
            
            return img_name
    
        else:
            print(">>Unable to find files, please check connection or retry.<<")
            return None
        
    except KeyError:
        print(">>Unable to find files, please check connection or retry.<<") 
        

def init():
    response = sqs.send_message(
        QueueUrl=worker_url,
        DelaySeconds=0,
        MessageAttributes={
            'Worker': {
                'DataType': 'String',
                'StringValue': socket.gethostname(),
            }
        },
        MessageBody=(
            'Worker Online.'
        )
    )
    print("=====================")
    print("Worker is now ONLINE")
    print("=====================")
    print(response)
    print("")

init()
while ans == 1:
    img_name = getWork()
    if img_name is not None:
        main(img_name)
    else:
        ans = int(input ("Unable to find work, submit 1 to retry, submit any other char to exit: "))
        sleep(3)
        print("==============================================================================")