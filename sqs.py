import boto3
import json
import pandas
from botocore.exceptions import ClientError
from urllib.parse import urlparse
import io
import os
from dotenv import load_dotenv

load_dotenv()
# Access AWS credentials from environment variables
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

print(aws_access_key_id)

client = boto3.resource('sqs', region_name='us-east-1', aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key)
		

						# SQS

queue = client.get_queue_by_name(QueueName='MyTestQueue')
for message in queue.receive_messages():
    data = message.body
    print(data)

t = urlparse(data)
file_name = t.path[1:]

s3_name = t.netloc.split('.s3')[0]

						#BUCKET


s3 =  boto3.resource('s3',
          aws_access_key_id=aws_access_key_id,
          aws_secret_access_key= aws_secret_access_key)

content_object = s3.Object(s3_name, file_name)
file_content = content_object.get()['Body'].read().decode('utf-8')
file_content = json.loads(file_content)

file_content["prices"] = [element for  element in file_content["prices"] if int(element["tag"])%10 == 1]



my_bucket = s3.Bucket(s3_name)
file_content = str(file_content)
my_bucket.upload_fileobj(io.BytesIO(file_content.encode("utf-8")), "result.txt")


response = queue.send_message(MessageBody='Success!')

                                    #EMAIL


ses_client = boto3.client('ses', region_name='us-east-1',
            aws_access_key_id=aws_access_key_id,
          aws_secret_access_key= aws_secret_access_key)
email = 'youremail@gmail.com'
response = ses_client.send_email(
    Source=email,
    Destination={
        'ToAddresses': [email],
        'CcAddresses': [email],
    },
    ReplyToAddresses=[email],
    Message={
        'Subject': {
            'Data': 'Result',
            'Charset': 'utf-8'
        },
        'Body': {
            'Text': {
                'Data': 'hello from Andrii',
                'Charset': 'utf-8'
            },
            'Html': {
                'Data': 'hello from Andrii',
                'Charset': 'utf-8'
            }
        }
    }
)

print(response)

