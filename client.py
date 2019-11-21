import cv2
import sys
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
region="ap-south-1"

def chunk_processor(img, img_name):    
    imgHeight, imgWidth = img.shape[:2]
    
    rekognition = boto3.client("rekognition", region, aws_access_key_id=key_file[0][1] , aws_secret_access_key=key_file[1][1])
    response = rekognition.detect_labels(
            Image={
			"S3Object": {
				"Bucket": bucket,
				"Name": img_name,
			}
		},
		MaxLabels=10,
		MinConfidence=50,
	)
    box =[]
    for label in response['Labels']:
        bbox = label['Instances']
        j = 0
        for i in bbox:
            box.append(i['BoundingBox'])
            
            left = int(imgWidth * box[j]['Left'])
            top = int(imgHeight * box[j]['Top'])
            width = int(imgWidth * box[j]['Width'])
            height = int(imgHeight * box[j]['Height'])

            cv2.rectangle(img, (left,top), (left + width, top + height), (255, 255, 00), 2)
            j = j+1
    cv2.imshow('result', img)
    cv2.waitKey()
def main():   
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
    except KeyError:
        print("Unable to find files, please check connection or retry.")
        sys.exit()
    
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
            print("Unable to find files, maybe queue is empty or retry.")
            print(response)
            sys.exit()
            
        receipt_handle = message['ReceiptHandle']
        
        try:
            img_name = message['MessageAttributes']['img']['StringValue']
        except KeyError:
            print("Unable to find files, please check connection or retry.")
        
        s3.download_file(bucket, img_name, img_name)

        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        
        chunk_processor(cv2.imread(img_name), img_name)
        
            
    else:
        print("Unable to find files, please check connectino or retry.")
        
main()
