import boto3

session = boto3.Session(profile_name="test-user1")
client = session.client("s3")

response = client.list_buckets()

for bucket in response["Buckets"]:
    print(bucket["Name"])




