# Default params
# default_region_name = 'ap-south-1'

# def send_msg_to_aws(msg):
#     pass

# import boto3
# ## Get the service resource
# sqs = boto3.resource('sqs')
# def send_message_sqs(q_url, msg_body, message_attributes=None):
#     queue_url = 'https://sqs.ap-south-1.amazonaws.com/863372932275/CandleData'
#     resp = sqs.send_message(
#     QueueUrl=queue_url,
#     MessageBody=(
#     msg_body
#     )
#     )
    
import boto3
import json

def sendToSQS(msg_body,queue_url,msg_attr=None, region_name=default_region_name):
    sqs_client = boto3.client('sqs',region_name)
    data = json.dumps(msg_body)
    
    try:
        response = sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody = data
            )
        
    except Exception as e:
            error = f"Error sending message: {e}"
            return error
            
