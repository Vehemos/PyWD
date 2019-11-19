# -*- coding: utf-8 -*-
"""
Created on Tue Nov 1 07:03:36 2019

@author: Sarthak Tripathi
"""

import cv2
import logging
import boto3
import numpy as np
from numpy import genfromtxt
from botocore.exceptions import ClientError

#def split():
    






key_file = np.loadtxt("C:\\rootkey.csv", dtype=str, delimiter='=')

# Create SQS client
sqs = boto3.client('sqs', region_name='ap-south-1', aws_access_key_id=key_file[0][1] , aws_secret_access_key=key_file[1][1] )
queue_url = 'https://sqs.ap-south-1.amazonaws.com/377293540990/scheduler.fifo'
msg_url = 'https://sqs.ap-south-1.amazonaws.com/377293540990/msngr'

s3 = boto3.client('s3', aws_access_key_id=key_file[0][1] , aws_secret_access_key=key_file[1][1])
bucket = 'imageblockupload'

num_workers = 2#input("Enter number of workers: ")
img_src = "C:\\tble.png"

chunks = num_workers #division by image

image = cv2.imread(img_src)
#cv2.imshow("Original Image", image)

image = cv2.imread(img_src)

height, width = image.shape[:2]
start_row, start_col = int(0), int(0)
end_row, end_col = int(height * .5), int(width)
cropped_top = image[start_row:end_row , start_col:end_col]
cv2.imwrite("top.png",cropped_top)

start_row, start_col = int(height * .5), int(0)
end_row, end_col = int(height), int(width)
cropped_bot = image[start_row:end_row , start_col:end_col]
cv2.imwrite("bot.png",cropped_bot)

#for i in range (0,chunks):
s3.upload_file("top.png", bucket, "1")

s3.upload_file("bot.png", bucket, "2")

response = sqs.send_message(
            QueueUrl=queue_url,
            MessageAttributes={
                    'URL1':{
                            'DataType': 'String',
                            'StringValue': 'www.URL1.com',
                            },
                    'URL2':{
                            'DataType': 'String',
                            'StringValue': 'www.URL2.com',
                            }
        },
        MessageBody=(
                'Image Urls.'),
        MessageGroupId='ImageBlocks',
        MessageDeduplicationId='ContentBasedDeduplication'
        )

print(response['MessageId'])

# Send message to SQS queue
response = sqs.send_message(
    QueueUrl=msg_url,
    DelaySeconds=10,
    MessageAttributes={
        'Images': {
            'DataType': 'String',
            'StringValue': '2',
        }
    },
    MessageBody=(
        'Files Uploaded.'
    )
)
    


print(response['MessageId'])