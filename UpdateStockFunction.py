import boto3
import json
import os
import pymysql

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
            body = json.loads(record.get("body"))
            product = body.get("product_id")
            quantity = body.get("quantity")
        
            #update database
            update_query = f"UPDATE products SET stock = stock - {quantity} WHERE product_id = {product}"
            cursor.execute(update_query)
            conn.commit()
        
        return {
            'statusCode': 200,
            'body': 'updated!'
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