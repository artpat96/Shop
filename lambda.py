import json
import boto3

def lambda_handler(event, context):
    client = boto3.client('s3')

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_name = event['Records'][0]['s3']['object']['key']

    file_obj = client.get_object(Bucket=bucket_name, Key=file_name)
    file_content = file_obj['Body'].read().decode('utf-8')

    data = json.loads(file_content)

    processed_data = []
    for item in data:
        processed_item = {
            'id': item['id'],
            'title': item['title'],
            'price': item['price'],
            'category': item['category'],
            'rate': item['rating']['rate'],
            'count': item['rating']['count']
        }
        processed_data.append(processed_item)

    processed_data_json = json.dumps(processed_data)

    processed_file_name = file_name + 's'
    processed_file_path = 'shoppy123_strct/' + processed_file_name

    client.put_object(Bucket=bucket_name, Key=processed_file_path, Body=processed_data_json)