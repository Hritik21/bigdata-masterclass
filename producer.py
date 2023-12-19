import json
import random
from faker import Faker
import boto3

# Initialize a boto3 session with IAM user credentials
session = boto3.Session(
    aws_access_key_id='YOUR_ACCESS_KEY_ID',
    aws_secret_access_key='YOUR_SECRET_ACCESS_KEY',
    region_name='YOUR_REGION'
)

# Initialize the AWS Kinesis client using the session
kinesis_client = session.client('kinesis')

# Specify the name of your Kinesis stream
stream_name = 'your_stream_name'

# Initialize the Faker instance
fake = Faker()

# Function to generate fake student information
def generate_student_info():
    student_info = {
        'student_id': fake.uuid4(),
        'name': fake.name(),
        'age': random.randint(18, 30),
        'major': fake.random_element(elements=('Computer Science', 'Math', 'Engineering', 'Physics')),
        'gpa': round(random.uniform(2.0, 4.0), 2)
    }
    return student_info

# Function to put a record into the Kinesis stream
def put_record(data):
    partition_key = str(random.randint(1, 100))  # Random partition key
    data = json.dumps(data)
    
    response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=data,
        PartitionKey=partition_key
    )
    return response

# Example usage
if __name__ == "__main__":
    num_records = 10  # Number of fake student records to generate
    
    for _ in range(num_records):
        student_data = generate_student_info()
        response = put_record(student_data)
        
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(f"Record sent successfully to partition {response['ShardId']}")
        else:
            print("Failed to send record")
