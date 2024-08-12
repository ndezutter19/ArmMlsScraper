import boto3
import logging
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize a session using Boto3
session = boto3.Session()

# Initialize DynamoDB resource
dynamodb = session.resource('dynamodb')

# Reference your DynamoDB table
table = dynamodb.Table('HouseListings')

def count_total_items():
    total_count = 0
    scan_kwargs = {
        'ProjectionExpression': 'listing_id'  # Only fetching listing_id for counting
    }

    done = False
    start_key = None

    with tqdm(desc="Counting items in the database") as pbar:
        while not done:
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
            response = table.scan(**scan_kwargs)
            total_count += response['Count']
            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None
            pbar.update(response['Count'])

    return total_count

def main():
    total_items = count_total_items()
    print(f"Total number of items in the database: {total_items}")

if __name__ == "__main__":
    main()
