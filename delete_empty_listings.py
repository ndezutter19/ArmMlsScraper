import boto3
from tqdm import tqdm

# Initialize a session using Boto3
session = boto3.Session()

# Initialize DynamoDB resource
dynamodb = session.resource('dynamodb')

# Reference your DynamoDB table
table = dynamodb.Table('HouseListings')

def delete_all_items():
    scan_kwargs = {
        'ProjectionExpression': 'listing_id',  # Use your partition key 'listing_id'
    }

    done = False
    start_key = None
    total_deleted = 0

    print("Starting to scan and delete all items...")

    with tqdm(desc="Deleting items", unit="item") as pbar:
        while not done:
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
            response = table.scan(**scan_kwargs)
            items = response.get('Items', [])
            for item in items:
                table.delete_item(Key={'listing_id': item['listing_id']})
                total_deleted += 1
                pbar.update(1)
            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None
            
    print(f"Total items deleted: {total_deleted}")

def main():
    delete_all_items()
    print("All items have been deleted.")

if __name__ == "__main__":
    main()
