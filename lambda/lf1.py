import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Function to push message to SQS
def push_to_sqs(QueueURL, msg_body):
    sqs = boto3.client('sqs')
    try:
        logger.debug(f"Sending message to SQS: {msg_body}")
        
        response = sqs.send_message(
            QueueUrl=QueueURL,
            DelaySeconds=0,
            MessageAttributes={
                'Cuisine': {
                    'DataType': 'String',
                    'StringValue': msg_body['Cuisine']
                },
                'Location': {
                    'DataType': 'String',
                    'StringValue': msg_body['Location']
                },
                'Email': {
                    'DataType': 'String',
                    'StringValue': msg_body['Email']
                },
                'DiningTime': {
                    'DataType': 'String',
                    'StringValue': msg_body['DiningTime']
                },
                'NumberOfPeople': {
                    'DataType': 'String',
                    'StringValue': msg_body['NumberOfPeople']
                }
            },
            MessageBody=json.dumps(msg_body)
        )

        print("sqs message sent")
        print(msg_body)
        logger.debug(f"SQS Response: {response}")
    except Exception as e:
        print("sqs not received any message")
        logger.error(f"Error sending to SQS: {str(e)}")
        return None
    return response

# Fulfill the intent after all slots are collected
def fulfill_intent(intent_request):
    # Access the slots from the 'sessionState' part of the event
    slots = intent_request['sessionState']['intent']['slots']
    
    # Data to push to SQS
    slot_dict = {
        'Cuisine': slots['Cuisine']['value']['interpretedValue'],
        'Location': slots['Location']['value']['interpretedValue'],
        'DiningTime': slots['DiningTime']['value']['interpretedValue'],
        'NumberOfPeople': slots['NumberOfPeople']['value']['interpretedValue'],
        'Email': slots['Email']['value']['interpretedValue']
    }

    logger.debug(f"Fulfillment data: {slot_dict}")
    
    # Send message to SQS
    res = push_to_sqs('https://sqs.us-east-1.amazonaws.com/825765404944/dining', slot_dict)
    
    if res:
        # Constructing the valid Lex response with the 'intent' included in 'sessionState'
        return {
            "sessionState": {
                "dialogAction": {
                    "type": "Close",
                    "fulfillmentState": "Fulfilled"
                },
                "intent": {
                    "name": intent_request['sessionState']['intent']['name'],
                    "slots": intent_request['sessionState']['intent']['slots'],
                    "state": "Fulfilled"
                }
            },
            "messages": [
                {
                    "contentType": "PlainText",
                    "content": f"We have received your request for {slot_dict['Cuisine']} cuisine. You will receive recommendations at {slot_dict['Email']}. Have a great dining experience!"
                }
            ]
        }
    else:
        return {
            "sessionState": {
                "dialogAction": {
                    "type": "Close",
                    "fulfillmentState": "Failed"
                },
                "intent": {
                    "name": intent_request['sessionState']['intent']['name'],
                    "slots": intent_request['sessionState']['intent']['slots'],
                    "state": "Failed"
                }
            },
            "messages": [
                {
                    "contentType": "PlainText",
                    "content": "Sorry, something went wrong. Please try again later."
                }
            ]
        }

# Dispatcher to route the request based on the intent
def dispatch(intent_request):
    logger.debug(f"dispatch userId={intent_request['sessionId']}, intentName={intent_request['sessionState']['intent']['name']}")
    
    intent_name = intent_request['sessionState']['intent']['name']
    
    if intent_name == 'DiningSuggestionsIntent':  # Ensure this matches the intent name
        return fulfill_intent(intent_request)
    
    raise Exception(f"Intent with name {intent_name} not supported")

# Main Lambda handler function
def lambda_handler(event, context):
    logger.debug(f"Received event: {json.dumps(event)}")
    
    # Proceed with dispatching the event to the right handler
    return dispatch(event)
