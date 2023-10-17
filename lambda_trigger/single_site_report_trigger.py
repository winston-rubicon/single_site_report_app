import boto3
import os

def lambda_handler(event, context):
    # Extract bucket name from S3 event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    # Extract filename from the S3 event
    filename = event['Records'][0]['s3']['object']['key']

    subnet_ids = os.environ['SUBNET_IDS'].split(',')

    client = boto3.client('ecs')
    response = client.run_task(
        cluster='single_site_report_app_cluster',
        launchType='FARGATE',
        taskDefinition='single_site_report_app_task_definition',
        count=1,
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': subnet_ids,
                'assignPublicIp': 'ENABLED'
            }
        },
        overrides={
            'containerOverrides': [{
                'name': 'single_site_report_app_container',
                'environment': [
                    {
                        'name': 'FILENAME',
                        'value': filename
                    },
                    {
                        'name': 'BUCKET_NAME',
                        'value': bucket_name
                    }
                ]
            }]
        }
    )
    return response