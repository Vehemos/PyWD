# -*- coding: utf-8 -*-
"""
Created on Tue Nov 1 07:03:36 2019

@author: Sarthak Tripathi
"""

import cv2
import boto3
import math
import numpy as np

#Function to divide image into chunks
def split(image, chunks):
    #Get info abou image (height, width)
    height, width = image.shape[:2]
    #Calculate where the image will be divided (stride)
    stride = (1/chunks)
    
    x, y = int(0), int(0)                                                       #starting coords for image (up)
    w, z = int(height * stride), int(width)                                     #ending coords where the image will be split (down)
    #Chunking loop
    for i in range (0, chunks):
        start_row, start_col = x, y
        end_row, end_col = w, z
        temp_img = image[start_row:end_row , start_col:end_col]
        cv2.imwrite(str(i+1) + ".png",temp_img)                                 #Save Image as iteration number.png
        #Update Var for next iteration
        x = w
        w += w
        
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

files = split(image, chunks)

'''
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

print(response['MessageId'])'''