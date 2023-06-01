import boto3
import json
import os
import pymysql

# Establish database connection
conn = pymysql.connect(
        host=os.environ.get('endpoint'),
        user=os.environ.get('username'),
        password=os.environ.get('password'),
        database=os.environ.get('database_name')
    )

def lambda_handler(event, context):
    try:
        # Retrieve records from the event
        records = event.get("Records")
        client = boto3.client("sqs")
        cursor = conn.cursor()

        for record in records:
            # Extract product_id, quantity, and customer_id from the message body
            body = json.loads(record.get("body"))
            product_id = body.get("product_id")
            quantity = body.get("quantity")
            customer_id = body.get("customer_id")

            # Send message to the next queue
            response = client.send_message(
                QueueUrl="https://sqs.us-east-1.amazonaws.com/591756872681/updateStock_queue",
                MessageBody=json.dumps(body)
            )

            # Insert data into the database
            insert_query = f"INSERT INTO orders (product_id, quantity, customer_id) VALUES ({product_id}, {quantity}, {customer_id})"
            cursor.execute(insert_query)
            conn.commit()
            
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:

            # Return a successful response
            return {
                'statusCode': 200,
                'body': 'order added'
            }
        return {
            'statusCode': 500,
            'body': 'Internal server error'
        }

    except Exception as e:
        # Handle any exceptions raised during execution
        error_message = f"An error occurred: {str(e)}"
        return {
            'statusCode': 500,
            'body': json.dumps(error_message)
        }

    finally:
        # Close the database connection
        conn.close()
