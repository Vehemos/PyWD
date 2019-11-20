import cv2
import boto3
import numpy as np

# Function to divide image into chunks
def split(image, chunks):
    # Get info abou image (height, width)
    height, width = image.shape[:2]
    # Calculate where the image will be divided (stride)
    stride = (1/chunks)
    
    x, y = int(0), int(0)                                                       # Starting coords for image (up)
    w, z = int(height * stride), int(width)                                     # Ending coords where the image will be split (down)
    #  Chunking loop
    for i in range (0, chunks):
        start_row, start_col = x, y
        end_row, end_col = w, z
        temp_img = image[start_row:end_row , start_col:end_col]
        cv2.imwrite(str(i+1) + ".png",temp_img)                                 # Save Image as iteration number.png
        # Update Var for next iteration
        x = w
        w += w

# Load Credentials    
key_file = np.loadtxt("rootkey.csv", dtype=str, delimiter='=')

# Create SQS client
sqs = boto3.client('sqs', region_name='ap-south-1', aws_access_key_id=key_file[0][1] , aws_secret_access_key=key_file[1][1] )
queue_url = 'https://sqs.ap-south-1.amazonaws.com/377293540990/scheduler.fifo'
msg_url = 'https://sqs.ap-south-1.amazonaws.com/377293540990/msngr'

# Create S3 client
s3 = boto3.client('s3', aws_access_key_id=key_file[0][1] , aws_secret_access_key=key_file[1][1])
bucket = 'imageblockupload'

# Initialize program variables
num_workers = input("Enter number of workers: ")
img_src = "tble.png"
chunks = int(num_workers) # Splits of each image. REDO late to find smarter splits

# Read Image
image = cv2.imread(img_src)

# Chunk Image
split(image, chunks)

# Upload chunks to S3
for i in range (0,chunks):
    
    # Upload File
    s3.upload_file(str(i+1) + ".png", bucket, str(i+1) + ".png")

    # Send File URL
    response = sqs.send_message(
                QueueUrl=queue_url,
                MessageAttributes={
                        'img':{
                                'DataType': 'String',
                                'StringValue': str(i+1) + ".png",
                                },
            },
            MessageBody=(
                    'Image Urls.'),
            MessageGroupId='ImageBlocks',
            MessageDeduplicationId=str(i+1)
            )


# Send message to SQS queue that task is finished.
response = sqs.send_message(
    QueueUrl=msg_url,
    DelaySeconds=10,
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
