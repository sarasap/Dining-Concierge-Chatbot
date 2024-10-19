import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Create a Lex runtime client using Boto3
client = boto3.client('lexv2-runtime')

# Function to call Lex
def call_lex_bot(text_message, user_id, bot_id, bot_alias_id, locale_id):
    try:
        response = client.recognize_text(
            botId=bot_id,              # Bot ID
            botAliasId=bot_alias_id,    # Bot Alias ID
            localeId=locale_id,         # Locale ID (e.g., en_US)
            sessionId=user_id,          # Unique user ID (you can use any unique value per user)
            text=text_message           # The input message from the API request
        )
        return response
    except Exception as e:
        logger.error(f"Error calling Lex: {e}")
        return None

# Lambda function handler
def lambda_handler(event, context):
    logger.debug(f"Received event: {json.dumps(event)}")

    # Step 1: Extract the text message from the API request
    body = json.loads(event.get('body', '{}'))
    text_message = body.get('message')  # Assuming the message is sent as { "message": "text to Lex" }
    
    if not text_message:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Message is required in the API request.'})
        }
    
    user_id = 'testUser'  # You can dynamically generate or use request data to identify users
    bot_id = '9SYFZTHRFB'  # Replace with your Lex Bot ID
    bot_alias_id = 'TSTALIASID'  # Replace with your Lex Bot Alias ID
    locale_id = 'en_US'  # Replace with the correct locale (e.g., en_US)

    # Step 2: Send the message to Lex chatbot
    lex_response = call_lex_bot(text_message, user_id, bot_id, bot_alias_id, locale_id)

    # Step 3: Handle Lex response and return the result to the API caller
    if lex_response:
        lex_message = lex_response.get('messages', [{}])[0].get('content', 'No response from Lex.')
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'POST,GET,OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
            },
            'body': json.dumps({'message': lex_message})
        }
    else:
        return {
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'POST,GET,OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
            },
            'body': json.dumps({'error': 'Failed to get a response from Lex.'})
        }
