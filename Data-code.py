import json
import boto3
import csv
import io

def lambda_handler(event, context):
    # Initialize the S3 client
    s3 = boto3.client('s3')
    
    # Define the S3 bucket name and file keys
    bucket_name = 'my-etl-task-bucket'
    csv_file_key = 'Mydata.csv'
    json_file_key = 'mydata.json'
    sql_file_key = 'Mydata.sql'
    output_json_key = 'output_combined_data.json'
    
    try:
        # Process CSV file
        csv_response = s3.get_object(Bucket=bucket_name, Key=csv_file_key)
        csv_content = csv_response['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        csv_json_data = [row for row in csv_reader]
        
        # Process JSON file
        json_response = s3.get_object(Bucket=bucket_name, Key=json_file_key)
        json_content = json_response['Body'].read().decode('utf-8')
        json_data = json.loads(json_content)
        
        # Process SQL file
        sql_response = s3.get_object(Bucket=bucket_name, Key=sql_file_key)
        sql_content = sql_response['Body'].read().decode('utf-8')
        
        # Assume SQL content is a list of SQL statements; wrap them in a JSON array
        sql_json_data = sql_content.split(';')  # Split by SQL statement delimiter
        
        # Combine all data into a single JSON object
        combined_json_data = {
            'csv_data': csv_json_data,
            'json_data': json_data,
            'sql_data': sql_json_data
        }
        
        # Convert combined JSON data to a string
        combined_json_string = json.dumps(combined_json_data)
        
        # Upload the combined JSON data to the S3 bucket
        s3.put_object(
            Bucket=bucket_name,
            Key=output_json_key,
            Body=combined_json_string,
            ContentType='application/json'
        )
        
        # Return success response
        return {
            'statusCode': 200,
            'body': json.dumps('File successfully written to S3.')
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
