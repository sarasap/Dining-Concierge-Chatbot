import uuid
import datetime
import logging
import boto3
import json
from botocore.exceptions import ClientError
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

opensearch_endpoint = 'https://search-diningopensearch-ct3nznfv55eeyexbdclrio5poq.us-east-1.es.amazonaws.com' 
opensearch_username = ''
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
def es_search(host, query):
    es_query = {
        "query": {
            "match": {
                "Cuisine": {
                    "query": cuisine,
                    "operator": "or"
                }
            }
        }
    }

    auth_header = b64encode(f"{opensearch_username}:{opensearch_password}".encode()).decode('utf-8')
    headers = {
        'Authorization': f'Basic {auth_header}',
        'Content-Type': 'application/json'
    }

    es_response = http.request(
        'POST',
        f"{opensearch_endpoint}/_search",
        body=json.dumps(es_query),
        headers=headers
    )
    
    if es_response.status != 200:
        raise Exception(f"Error: Received status code {es_response.status} from OpenSearch")

    data = json.loads(es_response.data.decode('utf-8'))
    es_data = data["hits"]["hits"]

    restaurant_ids = [restaurant["_source"]["RestaurantID"] for restaurant in es_data]

    return random.sample(restaurant_ids, min(5, len(restaurant_ids)))

# Function to get restaurant data from DynamoDB
def get_dynamo_data(table, key):
    try:
        response = table.get_item(Key={'id': key})
        if 'Item' in response:
            name = response['Item']['name']
            address_list = response['Item']['address']
            return f"{name}, {address_list}"
        else:
            return "Restaurant data not found"
    except ClientError as e:
        logger.error(f"Error fetching data from DynamoDB: {e}")
        return "Error fetching restaurant data"

# Function to send email using SES
def send_email(recipient_email, subject, body):
    ses = boto3.client('ses', region_name='us-east-1')
    try:
        response = ses.send_email(
            Source='',
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
    sqs_url = 'https://sqs.us-east-1.amazonaws.com/825765404944/dining'
    es_host = 'search-diningopensearch-ct3nznfv55eeyexbdclrio5poq.us-east-1.es.amazonaws.com'
    table_name = 'yelp-restaurants'
    
    # Get SQS messages
    messages = get_sqs_data(sqs_url)
    logger.debug("messages fetched")

    if not messages:
        logger.debug("No SQS messages to process")
        return
    
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table(table_name)
    
    for message in messages:
        # logger.debug(message);
        message_body = json.loads(message['Body'])
        cuisine = message_body.get('Cuisine', 'unknown cuisine')
        recipient_email = message_body.get('Email', '')
        
        # Elasticsearch query
        query = {"query": {"match": {"cuisine": cuisine}}}
        es_search_result = es_search(es_host, query)
        number_of_records_found = int(es_search_result.get("hits", {}).get("total", {}).get("value", 0))
        hits = es_search_result.get('hits', {}).get('hits', [])
        
        suggested_restaurants = []
        for hit in hits:
            restaurant_id = hit['_source']['id']
            suggested_restaurant = get_dynamo_data(table, restaurant_id)
            suggested_restaurants.append(suggested_restaurant)

        # Prepare the email content
        dining_time = message_body.get('DiningTime', 'unknown time')
        num_people = message_body.get('NumberOfPeople', 'unknown number')
        email_content = f"Hello! Here are {cuisine} suggestions for {num_people} people at {dining_time}:\n"
        
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
s