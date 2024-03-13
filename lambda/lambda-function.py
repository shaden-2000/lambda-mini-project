
import json
import boto3
import requests
from datetime import datetime

subprocess.call('pip install pymysql -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
subprocess.call('pip install requests -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

sys.path.insert(1, '/tmp/')

import pymysql
import requests


def lambda_handler(event, context):
    # Extract bucket and key from the S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Read the JSON file from S3
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=key)
    json_data = json.loads(response['Body'].read().decode('utf-8'))

    # Extract customer IDs from JSON data
    customer_ids = json_data['customerID']

    # Query customer names from the RDS database
    db_connection = pymysql.connect(host='aws-simplified.c94quwu6kdey.me-south-1.rds.amazonaws.com', user='admin', password='Asd5350183', database='superstore')
    cursor = db_connection.cursor()
    cursor.execute("SELECT CustomerID, CustomerName FROM customers WHERE CustomerID IN (%s)" % ','.join(map(str, customer_ids)))
    customer_data = cursor.fetchall()
    db_connection.close()

    # Format data including customer ID, customer name, and today's date
    formatted_data = []
    for row in customer_data:
        formatted_data.append({
            'customer_id': row[0],
            'customer_name': row[1],
            'date': datetime.now().strftime('%Y-%m-%d')
        })

    # Send data to API endpoint
    api_url = 'https://virtserver.swaggerhub.com/wcd_de_lab/top10/1.0.0/add'
    headers = {'Content-Type': 'application/json'}
    response = requests.post(api_url, data=json.dumps(formatted_data), headers=headers)

    # Check if the POST request was successful
    if response.status_code == 201:
        print("POST request succeeded")
    else:
        print("POST request failed")

    # Return response from the API endpoint
    return {
        'statusCode': response.status_code,
        'body': response.text
    }

