from __future__ import print_function
import boto3
import urllib.parse
import json

print ("*"*80)
print ("Initializing..")
print ("*"*80)

# Initialize the S3 client
s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Log the event for debugging
    print("Received event: " + json.dumps(event, indent=2))
    
    # Check if 'Records' key exists in the event
    if 'Records' not in event:
        print("Error: No 'Records' found in the event.")
        return {"statusCode": 400, "message": "No 'Records' in event."}
    
    # Fetch source bucket and object key
    try:
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    except KeyError as e:
        print(f"KeyError - {e}")
        return {"statusCode": 500, "message": f"KeyError - {e}"}

    # Define the target bucket
    target_bucket = 'just-bucket-output'
    copy_source = {'Bucket': source_bucket, 'Key': object_key}
    
    # Log information
    print ("Source bucket : ", source_bucket)
    print ("Target bucket : ", target_bucket)
    print ("Log Stream name: ", context.log_stream_name)
    print ("Log Group name: ", context.log_group_name)
    print ("Request ID: ", context.aws_request_id)
    print ("Mem. limits (MB): ", context.memory_limit_in_mb)

    try:
        # Wait for the object to persist in S3
        print ("Using waiter to wait for the object to persist in the S3 service")
        waiter = s3.get_waiter('object_exists')
        waiter.wait(Bucket=source_bucket, Key=object_key)
        
        # Copy the object to the target bucket
        s3.copy_object(Bucket=target_bucket, Key=object_key, CopySource=copy_source)
        print(f"File {object_key} copied to {target_bucket}")
        
        # Return success response
        return {"statusCode": 200, "message": "Object copied successfully!"}
        
    except Exception as err:
        # Error handling
        print ("Error -" + str(err))
        return {"statusCode": 500, "message": str(err)}
