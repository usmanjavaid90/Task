import json
import os
import pymysql
from datetime import datetime

# # Establish database connection
conn = pymysql.connect(
      host=os.environ.get('endpoint'),
      user=os.environ.get('username'),
      password=os.environ.get('password'),
      database=os.environ.get('database_name')
      )

def lambda_handler(event, context):
   
    customer_id = event["customer_id"]

    with conn.cursor() as cursor:
        query = f"SELECT * FROM orders WHERE customer_id = {customer_id}"
        cursor.execute(query)
        results = cursor.fetchall()

        # Get column names from cursor description
        column_names = [column[0] for column in cursor.description]

        # Create response using make_response function
        response = make_response(column_names, results)

    return {
        'statusCode': 200,
        'body': response
    }
    

def make_response(columns: list, rows: list):
    results = {"customer_orders": []}

    for values in rows:
        result = {}
        for column, value in zip(columns, values):
            if isinstance(value, datetime):
                value = value.strftime("%Y-%m-%d %H:%M:%S")
            result[column] = value

        results["customer_orders"].append(result)

    return results