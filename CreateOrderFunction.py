import json
import boto3

def lambda_handler(event, context):
    
    # order validation and error handling
    order = event.get('order_details')
    if not order.get('product_id') or not order.get('customer_id') or not order.get('quantity'):
        return {
            'statusCode': 400,
            'body': 'required fields are missing in the request'
        }
    
   
    client = boto3.client("sqs")
    response = client.send_message(
        QueueUrl = "https://sqs.us-east-1.amazonaws.com/591756872681/order_queue",
        MessageBody = json.dumps(event.get('order_details')))
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        return {
            'statusCode': 200,
            'body': 'Order processed'
        }
    return {
            'statusCode': 500,
            'body': 'Internal server error'
        }