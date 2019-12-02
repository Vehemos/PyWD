import sys
import cv2
import boto3
import numpy as np
import random as rnd
from time import sleep

# Load Credentials    
key_file = np.loadtxt("rootkey.csv", dtype=str, delimiter='=')

# Create SQS client
sqs = boto3.client('sqs', region_name='ap-south-1', aws_access_key_id=key_file[0][1] , aws_secret_access_key=key_file[1][1] )
queue_url = 'https://sqs.ap-south-1.amazonaws.com/377293540990/scheduler.fifo'
msg_url = 'https://sqs.ap-south-1.amazonaws.com/377293540990/msngr'
worker_url = "https://sqs.ap-south-1.amazonaws.com/377293540990/worker"
# Create S3 client
s3 = boto3.client('s3', aws_access_key_id=key_file[0][1] , aws_secret_access_key=key_file[1][1])
bucket = 'imageblockupload'

# Initialize program variables
ext = ".jpeg"
img_src = "./images/img2" + ext

# Function to divide image into chunks
def split(image, chunks):
    # Get info abou image (height, width)
    height, width = image.shape[:2]
    # Calculate where the image will be divided (stride)
    stride = (1/chunks)

    x, y = int(0), int(0)                                                       # Starting coords for image (up)
    w, z = int(height), int(width * stride)                                     # Ending coords where the image will be split (down)
    #  Chunking loop
    for i in range (0, chunks):
        start_row, start_col = x, y
        end_row, end_col = w, z
        temp_img = image[start_row:end_row , start_col:end_col]
        cv2.imwrite(str(i+1) + ext,temp_img)                                 # Save Image as iteration number.png
        # Update Var for next iteration
        y = z
        z += z

def main():
    # Read Image
    image = cv2.imread(img_src)
        
    # Chunk Image
    chunks = int(num_workers) # Splits of each image. REDO late to find smarter splits
    split(image, chunks)
    
    # Upload chunks to S3
    for i in range (0,chunks):     
        # Upload File
        s3.upload_file(str(i+1) + ext, bucket, str(i+1) + ext)
    
        # Send File URL
        response = sqs.send_message(
                    QueueUrl=queue_url,
                    MessageAttributes={
                            'img':{
                                    'DataType': 'String',
                                    'StringValue': str(i+1) + ext,
                                    },
                },
                MessageBody=(
                        'Image Urls.'),
                MessageGroupId='ImageBlock' + str(i+1) + str(rnd.randint(0, 999)),
                MessageDeduplicationId=str(i+1) + str(rnd.randint(0, 999)),
                )
        print(response)
    
    # Send message to SQS queue that task is finished.
    response = sqs.send_message(
        QueueUrl=msg_url,
        DelaySeconds=0,
        MessageAttributes={
            'Images': {
                'DataType': 'String',
                'StringValue': str(chunks),
            }
        },
        MessageBody=(
            'Files Uploaded.'
        )
    )

def getWorkers():
    response = sqs.get_queue_attributes(
        QueueUrl=worker_url,
        AttributeNames=[
            'ApproximateNumberOfMessages',
        ]
    )
    num_workers = int(response['Attributes']['ApproximateNumberOfMessages'])
    
    if num_workers == 0:
        print("No workers online.")

    return int(num_workers)

num_workers = getWorkers()

if num_workers == 0:
    sys.exit()

main()

while (getWorkers() == num_workers):
    print("Retrying in 10 seconds...")
    sleep(10)
    continue

print("Workers finished task, retriving chunks")

sqs.purge_queue(
            QueueUrl=worker_url,
        )

img_list = []

for i in range (1,num_workers+1):
    s3.download_file(bucket, "0" + str(i) + ext, "0" + str(i) + ext)
    img_list.append( cv2.imread("0" + str(i) + str(ext)))

vis = np.concatenate(img_list, axis=1)

cv2.imwrite('out.png', vis)