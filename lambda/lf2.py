import uuid
import datetime
import logging
import boto3
import json
from botocore.exceptions import ClientError
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from base64 import b64encode
import urllib3
import requests
import random


# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

opensearch_endpoint = 'https://search-diningopensearch-ct3nznfv55eeyexbdclrio5poq.us-east-1.es.amazonaws.com' 
opensearch_username = 'sarasa'
opensearch_password = ''

# Function to fetch data from SQS
def get_sqs_data(queue_URL):
    sqs = boto3.client('sqs')
    try:
        response = sqs.receive_message(
            QueueUrl=queue_URL,
            MaxNumberOfMessages=1,
            MessageAttributeNames=['All'],
            VisibilityTimeout=100,  # Adjust the visibility timeout to prevent multiple invocations
            WaitTimeSeconds=20  # Long polling to reduce the number of empty receives
        )
        logger.info("SQS works")
        logger.debug(f"SQS Response: {response}")
        messages = response.get('Messages', [])
        if not messages:
            logger.debug("No messages received")
            return []

        for message in messages:
            receipt_handle = message['ReceiptHandle']
            sqs.delete_message(QueueUrl=queue_URL, ReceiptHandle=receipt_handle)
        return messages
    
    except ClientError as e:
        logger.error(f"Error receiving messages from SQS: {e}")
        return []

# Elasticsearch search function
def es_search(es_host,cuisine):
    query = {
            "size": 100,
            "query": {
                "multi_match": {
                    "query": cuisine,
                    "fields": ["cuisine"]
                                }
                     }
            }
        
    index = 'restaurant'
    url = 'https://' + es_host + '/' + index + '/_search'
    awsauth = (opensearch_username,opensearch_password)
    headers = { "Content-Type": "application/json" }
    response = requests.post(url,auth=awsauth, headers=headers, data=json.dumps(query))
    print("response")
    print(response)
    res = response.json()
    print(res)
    noOfHits = res['hits']['total']
    hits = res['hits']['hits']
    buisinessIds = []
    
    for hit in hits:
        buisinessIds.append(str(hit['_source']['id']))
    
    ids = random.sample(buisinessIds, 3)
    print(ids)
    return ids

# Function to get restaurant data from DynamoDB
def get_dynamo_data(index):
    aws_access_key_id=''
    aws_secret_access_key=''
    dynamodb = boto3.resource('dynamodb',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key,region_name='us-east-1')
    table = dynamodb.Table('yelp-restaurants')
    results = [] 
    try:
        for id in index:
            print("Restaurant id  ",id)
            response = table.get_item(Key={'id': id})
            print("response",response)
            if 'Item' in response:
                name = response['Item']['name']
                address_list = response['Item']['address']
                contact = response['Item']['contact']
                rating = response['Item']['rating']
                print(f"{name}, {address_list}, {contact}, Rating {rating}")
                result = f"{name}, {address_list} , {contact} ,Rating {rating}"
                results.append(result)
        return results
    except ClientError as e:
        logger.error(f"Error fetching data from DynamoDB: {e}")
        return "Error fetching restaurant data"

# Function to send email using SES
def send_email(recipient_email, subject, body):
    ses = boto3.client('ses', region_name='us-east-1')
    try:
        response = ses.send_email(
            Source='sp8049@nyu.edu',
            Destination={'ToAddresses': [recipient_email]},
            Message={
                'Subject': {'Data': subject},
                'Body': {'Text': {'Data': body}}
            }
        )
        logger.debug(f"Email sent successfully: {response}")
        return True
    except ClientError as e:
        logger.error(f"Error sending email: {e}")
        return False

# Lambda handler function
def lambda_handler(event, context):
    logger.debug(f"Received event: {json.dumps(event)}")
    
    sqs_url = 'https://sqs.us-east-1.amazonaws.com/825765404944/dining'
    es_host = 'search-diningopensearch-ct3nznfv55eeyexbdclrio5poq.us-east-1.es.amazonaws.com'
    
    
    # Get SQS messages
    messages = get_sqs_data(sqs_url)
    logger.debug("messages fetched")

    if not messages:
        logger.debug("No SQS messages to process")
        return
    
    
    
    for message in messages:
        # logger.debug(message);
        message_body = json.loads(message['Body'])
        cuisine = message_body.get('Cuisine', 'unknown cuisine')
        recipient_email = message_body.get('Email', '')
        print("cuisine = ",cuisine)
        # Elasticsearch query
        index=es_search(es_host,cuisine)
        
        
        suggested_restaurants = get_dynamo_data(index)
           

        # Prepare the email content
        dining_time = message_body.get('DiningTime', 'unknown time')
        num_people = message_body.get('NumberOfPeople', 'unknown number')
        email_content = f"Hello! Here are {cuisine} suggestions for {num_people} people at {dining_time}:\n"
        print("suggested_restaurants ",suggested_restaurants)
        for i, rest in enumerate(suggested_restaurants):
            email_content += f"({i+1}) {rest}\n"
        
        # Send the email
        email_subject = f"Your {cuisine} Restaurant Recommendation"
        email_status = send_email(recipient_email, email_subject, email_content)
        
        if email_status:
            logger.debug(f"Email sent to {recipient_email}")
        else:
            logger.error(f"Failed to send email to {recipient_email}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Execution completed')
    }
