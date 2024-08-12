import os
import boto3
from boto3.dynamodb.conditions import Attr
import re
from concurrent.futures import ThreadPoolExecutor
import time

# Load AWS credentials from environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# Connect to DynamoDB
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name='us-east-2'  # Replace with your region
)

# Access the HouseListings table
table_name = 'HouseListings'
table = dynamodb.Table(table_name)

# Precompile regex patterns for efficiency
clean_patterns = [
    (re.compile(r'\bUNIT\b'), ''),
    (re.compile(r'\bUNITS\b'), ''),
    (re.compile(r'\bPHASE\b'), ''),
    (re.compile(r'\bPHASES\b'), ''),
    (re.compile(r'\bPARCEL\b'), ''),
    (re.compile(r'\bPARCELS\b'), ''),
    (re.compile(r'\bLOT\b'), ''),
    (re.compile(r'\bLOTS\b'), ''),
    (re.compile(r'\bTRACT\b'), ''),
    (re.compile(r'\bAMENDED\b'), ''),
    (re.compile(r'\bBLK\b'), ''),
    (re.compile(r'\bMCR\b'), ''),
    (re.compile(r'\bREPLAT\b'), ''),
    (re.compile(r'\bPLAT\b'), ''),
    (re.compile(r'\bAMD\b'), ''),
    (re.compile(r'\bAND\b'), ''),
    (re.compile(r'\bHOA\b'), ''),
    (re.compile(r'\b2ND\b'), ''),
    (re.compile(r'\bONE\b'), ''),
    (re.compile(r'\bTWO\b'), ''),
    (re.compile(r'\bTHREE\b'), ''),
    (re.compile(r'\bFOUR\b'), ''),
    (re.compile(r'\bFIVE\b'), ''),
    (re.compile(r'\bII\b'), ''),
    (re.compile(r'\bIII\b'), ''),
    (re.compile(r'\bIV\b'), ''),
    (re.compile(r'\bCONDOMINIUM\b'), ''),
    (re.compile(r'\bCONDOMINIUMS\b'), ''),
    (re.compile(r'\bCONDOMINIMUMS\b'), ''),
    (re.compile(r'\bCONDOMINIMIUM\b'), ''),
    (re.compile(r'\bAPARTMENTS\b'), ''),
    (re.compile(r'\bTOWNHOUSES\b'), ''),
    (re.compile(r'\bTOWNHOUSE\b'), ''),
    (re.compile(r'\bTOWNHOME\b'), ''),
    (re.compile(r'\bTOWNHOMES\b'), ''),
    (re.compile(r'\bAPARTMENT\b'), ''),
    (re.compile(r'\bAPARTMENTS\b'), ''),
    (re.compile(r'\bCONDO\b'), ''),
    (re.compile(r'\bCONDOS\b'), ''),
    (re.compile(r'\bESTATE\b'), ''),
    (re.compile(r'\bESTATES\b'), ''),
    (re.compile(r'\bVILLAS\b'), ''),
    (re.compile(r'\bADDITION\b'), ''),
    (re.compile(r'\bBLOCKS\b'), ''),
    (re.compile(r'\bDETACHED\b'), ''),
    (re.compile(r'\bUNI\b'), ''),
    (re.compile(r'\bNO\b'), ''),
    (re.compile(r'\b[A-Z]\b'), ''),
    (re.compile(r'\s?\d+.*$'), ''),
    (re.compile(r'[^A-Z\s]'), ''),
    (re.compile(r'\s+'), ' ')
]

# Optimized function to clean subdivision names
def clean_subdivision_name(subdivision):
    subdivision = subdivision.upper()
    for pattern, replacement in clean_patterns:
        subdivision = pattern.sub(replacement, subdivision)
    return subdivision.strip()

# Batch get items function
def batch_get_items(table, keys):
    response = table.batch_get_item(RequestItems={table.table_name: {'Keys': keys}})
    return response['Responses'][table.table_name]

# Function to fetch items in parallel
def fetch_items_in_parallel(table, filter_expression, limit_per_page=100):
    items = []
    exclusive_start_key = None

    with ThreadPoolExecutor(max_workers=40) as executor:
        while True:
            scan_kwargs = {
                'FilterExpression': filter_expression,
                'Limit': limit_per_page
            }
            if exclusive_start_key:
                scan_kwargs['ExclusiveStartKey'] = exclusive_start_key

            response = table.scan(**scan_kwargs)
            items.extend(response.get('Items', []))
            exclusive_start_key = response.get('LastEvaluatedKey', None)

            if not exclusive_start_key:
                break

    return items

# Ask for user inputs
city = input("Enter the city to filter house listings or press enter for all of Arizona: ").strip()
min_threshold_input = input("Enter the minimum number of active houses to display a subdivision or press enter for 1: ").strip()
min_threshold = int(min_threshold_input) if min_threshold_input else 1

# Start the timer
start_time = time.time()

# Set the filter expression for city
filter_expression = Attr('address').contains(city)

# Fetch items in parallel
items = fetch_items_in_parallel(table, filter_expression)

# Dictionaries to hold counts and details for active and closed listings
subdivision_active_count = {}
subdivision_closed_count = {}
subdivision_details = {}
subdivision_ids = {}

# Process each item to count houses in each subdivision
for item in items:
    subdivision = item.get('subdivision', 'Unknown')
    clean_name = clean_subdivision_name(subdivision)
    if item.get('active', False):
        if clean_name in subdivision_active_count:
            subdivision_active_count[clean_name] += 1
        else:
            subdivision_active_count[clean_name] = 1
    else:
        if clean_name in subdivision_closed_count:
            subdivision_closed_count[clean_name] += 1
        else:
            subdivision_closed_count[clean_name] = 1

    if clean_name in subdivision_details:
        subdivision_details[clean_name].append(item)
    else:
        subdivision_details[clean_name] = [item]

# Function to print subdivisions that meet the threshold
def print_subdivisions():
    total_active_houses = 0
    total_closed_houses = 0
    subdivision_ids.clear()  # Clear the existing IDs
    for i, subdivision in enumerate(sorted(set(subdivision_active_count.keys()).union(subdivision_closed_count.keys()))):
        active_count = subdivision_active_count.get(subdivision, 0)
        closed_count = subdivision_closed_count.get(subdivision, 0)
        total_active_houses += active_count
        total_closed_houses += closed_count
        if active_count >= min_threshold and subdivision not in ('', ' ', 'UNKNOWN'):
            subdivision_ids[i] = subdivision
            print(f"ID: {i}, {subdivision}: {active_count} active houses, {closed_count} closed houses")
    print(f"Total unique subdivisions retrieved: {len(subdivision_ids)}")
    print(f"Total Active Houses: {total_active_houses}")
    print(f"Total Closed Houses: {total_closed_houses}")
    print(f"Total Houses: {total_active_houses + total_closed_houses}")

# Stop the timer and calculate the elapsed time
end_time = time.time()
execution_time = end_time - start_time

# Print subdivisions and execution time
print_subdivisions()
print(f"Execution Time: {execution_time:.2f} seconds")

# Function to calculate average prices and price per square foot using only closed houses
def calculate_averages(group_houses):
    total_price = 0
    total_sqft = 0
    total_price_per_sqft = 0
    count = 0

    for house in group_houses:
        if house.get('active', False):
            continue  # Skip active houses

        price = house.get('price', '').replace('$', '').replace(',', '')
        sqft = house.get('sqft', '').replace(',', '')
        try:
            price = float(price)
            sqft = float(sqft)
            price_per_sqft = price / sqft if sqft else 0
            total_price += price
            total_sqft += sqft
            total_price_per_sqft += price_per_sqft
            count += 1
        except ValueError:
            continue

    if count == 0:
        return None, None

    avg_price = total_price / count
    avg_price_per_sqft = total_price_per_sqft / count

    return avg_price, avg_price_per_sqft

# Function to display houses in a subdivision
def display_houses(subdivision):
    houses = subdivision_details.get(subdivision, [])
    if not houses:
        print(f"No houses found in {subdivision}")

    # Sort the houses by active status first, then by number of bedrooms and bathrooms
    sorted_houses = sorted(
        houses,
        key=lambda x: (x.get('beds', 0), x.get('baths', 0), not x.get('active', False))
    )

    groups = {}
    for house in sorted_houses:
        beds = house.get('beds', 'N/A')
        baths = house.get('baths', 'N/A')
        group = (beds, baths)
        if group not in groups:
            groups[group] = []
        groups[group].append(house)

    for group, group_houses in groups.items():
        beds, baths = group
        avg_price, avg_price_per_sqft = calculate_averages(group_houses)
        print(f"\n--- {beds} Beds, {baths} Baths ---")

        if avg_price is None or avg_price_per_sqft is None:
            print("Not enough data for average calculations (only active houses).")
        else:
            print(f"Average Price: ${avg_price:,.2f}, Average Price per Sqft: ${avg_price_per_sqft:,.2f}")

        for house in group_houses:
            status = 'Active' if house.get('active', False) else 'Closed'
            price = house.get('price', 'N/A')
            address = house.get('address', 'N/A')
            sqft = house.get('sqft', 'N/A')
            try:
                price_per_sqft = float(price.replace('$', '').replace(',', '')) / float(sqft.replace(',', ''))
                price_per_sqft_diff = price_per_sqft - avg_price_per_sqft if avg_price_per_sqft else 0
            except (ValueError, ZeroDivisionError):
                price_per_sqft = price_per_sqft_diff = 0

            price_per_sqft_str = f"${price_per_sqft:.2f} per Sqft"
            diff_str = f"({price_per_sqft_diff:+.2f} per Sqft)" if status == 'Active' and avg_price_per_sqft else ""

            print(f"Listing ID: {house.get('listing_id', 'N/A')}, Status: {status}, Address: {address}, Price: {price}, Sqft: {sqft}, {price_per_sqft_str} {diff_str}")

# Function to find and display best deals based on price per square foot
def find_best_deals():
    best_deals = []

    # Prompt user for the minimum price
    min_price_input = input("Enter the minimum price for the house or press enter for 0: ").strip()
    min_price = float(min_price_input) if min_price_input else 0
    if min_price < 0 or min_price > 500000:
        print("Invalid input. Minimum price must be between 0 and 500000. Setting to 0.")
        min_price = 0

    for subdivision, houses in subdivision_details.items():
        if not houses or subdivision in ('', 'UNKNOWN'):
            continue

        groups = {}
        for house in houses:
            beds = house.get('beds', 'N/A')
            baths = house.get('baths', 'N/A')
            group = (beds, baths)
            if group not in groups:
                groups[group] = []
            groups[group].append(house)

        for group, group_houses in groups.items():
            avg_price, avg_price_per_sqft = calculate_averages(group_houses)

            if avg_price is None or avg_price_per_sqft is None:
                continue  # Skip if not enough data for averages

            for house in group_houses:
                if not house.get('active', False):
                    continue  # Skip non-active houses

                price = house.get('price', 'N/A').replace('$', '').replace(',', '')
                sqft = house.get('sqft', 'N/A').replace(',', '')
                try:
                    price_value = float(price)
                    price_per_sqft = price_value / float(sqft) if sqft else 0
                except ValueError:
                    price_value = price_per_sqft = 0

                if price_value >= min_price and price_per_sqft < avg_price_per_sqft:
                    best_deals.append((subdivision, house, price_per_sqft, avg_price_per_sqft))

    # Sort the best deals by price per square foot
    best_deals = sorted(best_deals, key=lambda x: x[2])[:50]  # Get top 50 best deals
    best_deals = [deal for deal in best_deals if deal[1].get('price') and float(deal[1].get('price').replace('$', '').replace(',', '')) >= min_price]

    print(f"\n--- Top 50 Best Deals (Below Average Price per Sqft) ---")
    for subdivision, house, price_per_sqft, avg_price_per_sqft in best_deals:
        listing_id = house.get('listing_id', 'N/A')
        address = house.get('address', 'N/A')
        price = house.get('price', 'N/A')
        sqft = house.get('sqft', 'N/A')
        delta = price_per_sqft - avg_price_per_sqft
        print(f"Listing ID: {listing_id}, Subdivision: {subdivision}, Address: {address}, Price: {price}, Sqft: {sqft}, Price per Sqft: ${price_per_sqft:.2f} (Delta: ${delta:.2f})")

# Loop to prompt user for subdivision ID and display details
while True:
    choice = input("Enter subdivision ID to view details, 'p' to print subdivisions again, 'r' for best deals, or 'e' to exit: ").strip()
    if choice.lower() == 'e':
        break
    elif choice.lower() == 'p':
        print_subdivisions()
    elif choice.lower() == 'r':
        find_best_deals()
    else:
        try:
            sub_id = int(choice)
            if sub_id in subdivision_ids:
                display_houses(subdivision_ids[sub_id])
            else:
                print("Invalid subdivision ID. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a valid subdivision ID, 'p' to print subdivisions again, 'r' for best deals, or 'e' to exit.")
