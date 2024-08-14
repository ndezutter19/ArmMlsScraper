import requests
import json
import boto3
from bs4 import BeautifulSoup
import re
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import tqdm
import datetime
import time


from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from boto3.dynamodb.conditions import Attr

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get the number of CPU cores
cpu_count = os.cpu_count()

# Define a multiplier for I/O-bound tasks (e.g., 4 times the CPU cores)
worker_multiplier = 4
max_workers = cpu_count * worker_multiplier

logging.info(f"Number of CPU cores: {cpu_count}")
logging.info(f"Max workers (for I/O-bound tasks): {max_workers}")

# Initialize a session using Boto3
session = boto3.Session()

# Initialize DynamoDB resource
dynamodb = session.resource('dynamodb')

# Reference your DynamoDB table
table = dynamodb.Table('HouseListings')

# URL for the POST request
url = "https://mymonsoon.com/api/properties/thin-search"

# List of zip codes
# List of zip codes
zip_code_numbers = [
    206273, 30003, 30004, 30005, 30006, 30008, 30009, 30011, 206274, 30012, 
    30013, 30014, 30015, 30016, 30017, 30018, 30019, 30020, 30021, 30022, 
    30023, 30024, 30025, 206142, 30026, 30027, 30028, 30029, 30030, 30031, 
    30032, 30033, 30034, 30035, 30036, 30037, 30038, 30039, 30040, 206275, 
    30041, 30042, 30043, 30044, 30045, 206143, 30047, 30048, 206144, 206145, 
    206146, 30049, 30050, 30051, 30052, 30053, 206147, 206148, 206149, 30054, 
    206150, 206151, 206152, 206153, 206154, 206155, 206276, 206156, 206157, 
    30055, 206158, 206159, 30056, 206160, 206451, 206161, 206162, 206163, 
    206164, 206165, 206166, 206167, 206168, 30058, 30059, 30060, 30061, 
    30062, 30063, 30064, 30065, 30066, 30067, 206169, 30068, 30069, 206170, 
    30070, 206171, 30076, 30077, 30078, 30080, 30081, 30082, 30086, 30087, 
    30088, 30089, 30090, 30091, 30092, 30093, 30094, 30095, 30096, 30097, 
    30098, 30099, 206172, 30100, 30101, 30102, 30103, 30104, 30105, 206173, 
    30106, 30107, 30109, 30110, 30111, 30112, 30113, 206277, 206450, 30116, 
    30117, 30118, 30119, 206174, 30120, 30121, 30122, 30123, 30124, 30125, 
    30126, 30127, 30128, 30129, 30130, 30131, 30132, 30133, 30134, 206278, 
    206279, 30135, 30136, 206280, 30137, 30138, 30139, 206281, 30140, 30141, 
    206282, 30142, 30143, 30144, 30145, 30146, 30147, 206283, 30148, 30149, 
    206284, 206285, 206286, 30150, 30151, 206287, 30152, 30153, 30154, 
    206288, 206289, 30155, 206290, 30156, 30157, 206291, 30158, 206292, 
    206293, 30159, 206294, 206295, 30160, 30161, 30162, 30163, 30164, 
    30165, 30166, 30167, 30168, 30169, 30170, 206175, 30171, 30172, 30173, 
    30175, 30176, 30177, 206296, 206297, 206298, 206299, 206300, 206301, 
    206302, 206176, 206303, 206304, 206305, 206306, 206307, 206308, 206309, 
    206310, 206311, 206312, 206313, 206187, 206177, 206314, 206315, 206316, 
    206317, 206318, 206319, 206320, 206188, 206189, 206321, 206322, 206323, 
    206178, 206190, 206324, 206325, 206191, 206179, 206326, 206327, 206328, 
    206329, 206330, 206192, 206331, 206180, 206332, 206333, 206226, 206334, 
    206335, 206193, 206336, 206337, 206194, 206338, 206339, 206340, 206181, 
    206341, 206182, 206195, 206196, 206197, 206198, 206227, 206199, 206228, 
    206200, 206201, 206202, 206203, 206204, 206205, 206206, 206207, 206208, 
    206209, 206342, 206230, 206231, 206210, 206211, 206212, 206183, 206213, 
    206214, 206215, 206234, 206216, 206217, 206218, 206219, 206220, 206221, 
    206343, 206222, 206223, 206224, 206344, 206345, 206346, 206347, 206348, 
    206349, 206350, 206351, 206352, 206353, 206354, 206355, 206356, 206357, 
    206358, 206359, 206360, 206361, 206362, 206363, 206364, 206365, 206366, 
    206367, 206368, 206369, 206370, 206371, 206372, 206373, 206374, 206375, 
    206376, 206377, 206378, 206379, 206380, 206381, 206382, 206383, 206384, 
    206385, 206386, 206387, 206389, 206390, 206391, 206392, 206393, 206396, 
    206397, 206398, 206399, 206400, 206401, 206402, 206403, 206404, 206405, 
    206406, 206407, 206408, 206409, 206410, 206411, 206412, 206413, 206414, 
    206415, 206416, 206417, 206418, 206419, 206420, 206421, 206422, 206423, 
    139999, 206424, 206425, 206426, 206427, 206428, 206429, 206430, 206431, 
    206432, 206433, 206434, 206435, 206436, 206437, 206438, 206439, 206440, 
    206441, 206442, 206443, 206444, 206445, 206446, 206447, 206449
]

# Set the headers for the request
headers = {
    "Content-Type": "application/json"
}

# Initialize an empty list to hold all data
all_data_active = []
all_data_closed = []

# Fetch cookies using Selenium
def fetch_cookies():
    # Set up the Firefox driver without headless mode
    firefox_options = Options()
    service = Service()
    driver = webdriver.Firefox(service=service, options=firefox_options)

    # Navigate to the login page
    driver.get("https://auth.armls.com/u/login?state=hKFo2SBqTWFVVnBKcGFJdk5leHl1NUVjME1pQlVUQ2hMcFllN6Fur3VuaXZlcnNhbC1sb2dpbqN0aWTZIHlMTTFuOEpFa2FsRUFhUHJoYVA3YXZtWmNCc3BrRlY4o2NpZNkgWVpZTENreHVpTXV0WlZ1UUViYmNTY0dFVmRPMFlxeGw")

    # Fill in the login form and submit it
    username = "mm814"
    password = "MGroup123!"

    # Locate the username and password fields and enter the credentials
    username_input = driver.find_element(By.ID, "username")
    password_input = driver.find_element(By.NAME, "password")

    username_input.send_keys(username)
    password_input.send_keys(password)

    # Submit the form
    password_input.send_keys(Keys.RETURN)

    # Wait for the login to complete and page to load
    time.sleep(7)

    # Enter the search term into the active element and press RETURN
    search_term = "6715534"
    driver.switch_to.active_element.send_keys(search_term)
    time.sleep(7)
    driver.switch_to.active_element.send_keys(Keys.RETURN)

    # Wait for the search results to load
    time.sleep(7)

    # Retrieve all cookies
    all_cookies = driver.get_cookies()

    # Define the important cookies to filter
    important_cookie_names = [
        '_openid_rp_session',
        'agent_tech_id',
        'ajs_anonymous_id',
        'cid',
        'flex_private_oauth_sso_session_id',
        'fpjs_user_id',
        'mp_098dee69-35a9-4daf-bf51-e181f1a314a2_perfalytics',
        'mp_ccc8d7180ac4672131d86f835d133338_mixpanel',
        'quick_launch_history_20080516021715125287000000',
        'Ticket'
    ]

    # Filter and display only the important cookies
    important_cookies = {cookie['name']: cookie['value'] for cookie in all_cookies if cookie['name'] in important_cookie_names}

    if important_cookies:
        print("Cookies successfully retrieved:")
    else:
        print("No important cookies were found.")

    driver.quit()
    return important_cookies

# Get cookies using the fetch_cookies function
cookies = fetch_cookies()

# Function to make the POST request and append data to the list
def fetch_data_from_api(zip_code, listing_status, all_data):
    current_date = datetime.datetime.now()
    one_year_ago = current_date - datetime.timedelta(days=365)
    high_value_date = current_date.strftime("%m/%d/%Y")
    low_value_date = one_year_ago.strftime("%m/%d/%Y")
    
    payload = {
        "pageNumber": 1,
        "searchType": "ListingResidentialSale",
        "addedSearchFields": [
            {"id": 96, "name": "GeographicShapeSet", "placeholderText": "Geographic Shape", "placeholderTextMin": "Min", "placeholderTextMax": "Max", "defaultDisplayIndex": -1, "lowValue": "", "highValue": "", "listValues": [], "listValuesByName": [], "isNot": False, "searchTextType": 0, "selectedTextType": "Contains", "showIsNotButton": False, "isDisabled": True, "shapes": [], "notButtonText": "Is "},
            {"id": 20, "name": "ListingNumber", "placeholderText": "MLS Number", "placeholderTextMin": "Min", "placeholderTextMax": "Max", "defaultDisplayIndex": 1, "lowValue": "", "highValue": "", "listValues": [], "listValuesByName": [], "isNot": False, "searchTextType": 0, "selectedTextType": "Contains", "showIsNotButton": False, "isDisabled": True, "shapes": [], "notButtonText": "Is "},
            {"id": 116, "name": "SourceMLSId", "placeholderText": "MLS", "placeholderTextMin": "Min", "placeholderTextMax": "Max", "defaultDisplayIndex": 2, "lowValue": "", "highValue": "", "listValues": [], "listValuesByName": [], "isNot": False, "searchTextType": 0, "selectedTextType": "Contains", "showIsNotButton": False, "isDisabled": True, "shapes": [], "notButtonText": "Is "},
            {"id": 18, "name": "ListingStatusId", "placeholderText": "Listing Status", "placeholderTextMin": "Min", "placeholderTextMax": "Max", "defaultDisplayIndex": 3, "lowValue": "", "highValue": "", "listValues": [listing_status], "listValuesByName": [], "isNot": False, "searchTextType": 0, "selectedTextType": "Contains", "showIsNotButton": False, "isDisabled": True, "shapes": [], "notButtonText": "Is "}, #change this value to a 3 for closed listings
            {"id": 17, "name": "ListingPrice", "placeholderText": "Listing Price", "placeholderTextMin": "Min", "placeholderTextMax": "Max", "defaultDisplayIndex": 4, "lowValue": "", "highValue": "500000", "listValues": [], "listValuesByName": [], "isNot": False, "searchTextType": 0, "selectedTextType": "Contains", "showIsNotButton": False, "isDisabled": True, "shapes": [], "notButtonText": "Is "}, #change this line for the maximum and minimum price
            {"id": 24, "name": "PropertyTypeId", "placeholderText": "Dwelling Type", "placeholderTextMin": "Min", "placeholderTextMax": "Max", "defaultDisplayIndex": 5, "lowValue": "", "highValue": "", "listValues": [], "listValuesByName": [], "isNot": False, "searchTextType": 0, "selectedTextType": "Contains", "showIsNotButton": False, "isDisabled": True, "shapes": [], "notButtonText": "Is "},
            {"id": 6, "name": "CountyId", "placeholderText": "County", "placeholderTextMin": "Min", "placeholderTextMax": "Max", "defaultDisplayIndex": 6, "lowValue": "", "highValue": "", "listValues": [], "listValuesByName": [], "isNot": False, "searchTextType": 0, "selectedTextType": "Contains", "showIsNotButton": False, "isDisabled": True, "shapes": [], "notButtonText": "Is "},
            {"id": 97, "name": "ListingAddress", "placeholderText": "Address", "placeholderTextMin": "Min", "placeholderTextMax": "Max", "defaultDisplayIndex": 7, "lowValue": "", "highValue": "", "listValues": [], "listValuesByName": [], "isNot": False, "searchTextType": 0, "selectedTextType": "Contains", "showIsNotButton": False, "isDisabled": True, "shapes": [], "notButtonText": "Is "},
            {"id": 5, "name": "CityId", "placeholderText": "City", "placeholderTextMin": "Min", "placeholderTextMax": "Max", "defaultDisplayIndex": 8, "lowValue": "", "highValue": "", "listValues": [], "listValuesByName": [], "isNot": False, "searchTextType": 0, "selectedTextType": "Contains", "showIsNotButton": False, "isDisabled": True, "shapes": [], "notButtonText": "Is "},
            {"id": 31, "name": "ZipId", "placeholderText": "Zip Code", "placeholderTextMin": "Min", "placeholderTextMax": "Max", "defaultDisplayIndex": 9, "lowValue": "", "highValue": "", "listValues": [zip_code], "listValuesByName": [], "isNot": False, "searchTextType": 0, "selectedTextType": "Contains", "showIsNotButton": False, "isDisabled": True, "shapes": [], "notButtonText": "Is "},
            {"id": 10, "name": "InteriorAreaSqFt", "placeholderText": "SqFt", "placeholderTextMin": "Min", "placeholderTextMax": "Max", "defaultDisplayIndex": 10, "lowValue": "", "highValue": "", "listValues": [], "listValuesByName": [], "isNot": False, "searchTextType": 0, "selectedTextType": "Contains", "showIsNotButton": False, "isDisabled": True, "shapes": [], "notButtonText": "Is "},
            {"id": 4, "name": "BedroomCount", "placeholderText": "# of Bedrooms", "placeholderTextMin": "Min", "placeholderTextMax": "Max", "defaultDisplayIndex": 11, "lowValue": "", "highValue": "", "listValues": [], "listValuesByName": [], "isNot": False, "searchTextType": 0, "selectedTextType": "Contains", "showIsNotButton": False, "isDisabled": True, "shapes": [], "notButtonText": "Is "},
            {"id": 3, "name": "BathroomCount", "placeholderText": "# of Bathrooms", "placeholderTextMin": "Min", "placeholderTextMax": "Max", "defaultDisplayIndex": 12, "lowValue": "", "highValue": "", "listValues": [], "listValuesByName": [], "isNot": False, "searchTextType": 0, "selectedTextType": "Contains", "showIsNotButton": False, "isDisabled": True, "shapes": [], "notButtonText": "Is "},
            {"id": 70, "name": "ListingTypeId", "placeholderText": "-", "placeholderTextMin": "Min", "placeholderTextMax": "Max", "defaultDisplayIndex": 13, "lowValue": "", "highValue": "", "listValues": [1], "listValuesByName": [], "isNot": False, "searchTextType": 0, "selectedTextType": "Contains", "showIsNotButton": False, "isDisabled": True, "shapes": [], "notButtonText": "Is "},
            {"id": 102, "name": "SubdivisionName", "placeholderText": "Subdivision Name", "placeholderTextMin": "Min", "placeholderTextMax": "Max", "defaultDisplayIndex": 14, "lowValue": "", "highValue": "", "listValues": [], "listValuesByName": [], "isNot": False, "searchTextType": 0, "selectedTextType": "Contains", "showIsNotButton": False, "isDisabled": True, "shapes": [], "notButtonText": "Is "},
            {"id": 19, "name": "ListingStatusTime", "placeholderText": "Status Change Date", "placeholderTextMin": "Min", "placeholderTextMax": "Max", "defaultDisplayIndex": 15, "lowValue": low_value_date, "highValue": high_value_date, "listValues": [], "listValuesByName": [], "isNot": False, "searchTextType": 0, "selectedTextType": "Contains", "showIsNotButton": False, "isDisabled": True, "shapes": [], "notButtonText": "Is "}, # Dynamically set date range
            {"id": 73, "name": "OffMarketDate", "placeholderText": "Off Market Date", "placeholderTextMin": "Min", "placeholderTextMax": "Max", "defaultDisplayIndex": 16, "lowValue": "", "highValue": "", "listValues": [], "listValuesByName": [], "isNot": False, "searchTextType": 0, "selectedTextType": "Contains", "showIsNotButton": False, "isDisabled": True, "shapes": [], "notButtonText": "Is "},
            {"id": 92, "name": "LastCOEPrice", "placeholderText": "Sold Price", "placeholderTextMin": "Min", "placeholderTextMax": "Max", "defaultDisplayIndex": 17, "lowValue": "", "highValue": "", "listValues": [], "listValuesByName": [], "isNot": False, "searchTextType": 0, "selectedTextType": "Contains", "showIsNotButton": False, "isDisabled": True, "shapes": [], "notButtonText": "Is "}
        ],
        "savedSearches": [],
        "hasNoSavedSearches": False,
        "isAddingSavedSearch": False,
        "isMobile": False,
        "sortList": [],
        "selectedSort": 25,
        "hasSearched": False,
        "isDirty": True
    }

    # Make the POST request
    response = requests.post(url, headers=headers, data=json.dumps(payload), cookies=cookies)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        
        # Append the data to the list
        all_data.extend(data)
    else:
        logging.warning(f"Failed to fetch data for zip code {zip_code} with status {listing_status}. Status code: {response.status_code}")

# Fetch data for all zip codes with listing status "1" (active)
for zip_code in tqdm.tqdm(zip_code_numbers, desc="Fetching Active Listings"):
    try:
        fetch_data_from_api(zip_code, "1", all_data_active)
    except Exception as e:
        logging.error(f"Error fetching data for zip code {zip_code} with status '1': {e}")


# Fetch data for all zip codes with listing status "3" (closed)
for zip_code in tqdm.tqdm(zip_code_numbers, desc="Fetching Closed Listings"):
    try:
        fetch_data_from_api(zip_code, "3", all_data_closed)
    except Exception as e:
        logging.error(f"Error fetching data for zip code {zip_code} with status '3': {e}")

# Filter and save the data
def filter_data(data):
    # Filter elements that don't have mlsSource as "armls"
    filtered_data = [item for item in data if item.get('mlsSource') == 'armls']
    return filtered_data

# Filter the active and closed data
all_data_active = filter_data(all_data_active)
all_data_closed = filter_data(all_data_closed)

# Save the data to JSON files
with open('response_data_active.json', 'w') as f:
    json.dump(all_data_active, f, indent=4) 

with open('response_data_closed.json', 'w') as f:
    json.dump(all_data_closed, f, indent=4) 

# Function to get text from HTML element
def get_text(soup, label):
    try:
        element = soup.find(string=label)
        if element:
            next_sibling = element.parent.find_next_sibling(string=True)
            if next_sibling:
                return next_sibling.strip()
        return "N/A"
    except Exception as e:
        logging.error(f"Error getting text for label {label}: {e}")
        return "N/A"

# Function to get beds and baths
def get_beds_baths(soup):
    try:
        beds_baths_label = soup.find('span', string='Beds/Baths:')
        if beds_baths_label:
            beds_baths = beds_baths_label.find_next_sibling(string=True)
            if beds_baths:
                return beds_baths.strip()
        return "N/A"
    except Exception as e:
        logging.error(f"Error getting beds/baths: {e}")
        return "N/A"

# Function to clean square footage data
def clean_sqft(sqft):
    try:
        if sqft:
            return sqft.split('/')[0].strip()
        return "N/A"
    except Exception as e:
        logging.error(f"Error cleaning sqft: {e}")
        return "N/A"

# Function to get subdivision
def get_subdivision(soup):
    try:
        subdivision_label = soup.find('span', string='Subdivision:')
        if subdivision_label:
            subdivision = subdivision_label.find_next_sibling(string=True)
            if subdivision:
                return subdivision.strip()
        return "N/A"
    except Exception as e:
        logging.error(f"Error getting subdivision: {e}")
        return "N/A"

# Function to get number of exterior stories
def get_exterior_stories(soup):
    try:
        stories_label = soup.find('span', style="font-weight: bold;", string='Exterior Stories:')
        if stories_label:
            stories = stories_label.find_next_sibling(string=True)
            if stories:
                return stories.strip()
        return "N/A"
    except Exception as e:
        logging.error(f"Error getting exterior stories: {e}")
        return "N/A"

# Function to get encoded features
def get_encoded_features(soup):
    try:
        encoded_features_label = soup.find('span', style="font-weight: bold;", string='Encoded Features:')
        if encoded_features_label:
            encoded_features = encoded_features_label.find_next_sibling(string=True)
            if encoded_features:
                return encoded_features.strip()
        return "N/A"
    except Exception as e:
        logging.error(f"Error getting encoded features: {e}")
        return "N/A"

# Function to get pool section
def get_pool_section(soup):
    try:
        pool_label = soup.find('span', style="font-weight: bold;", string='Pool:')
        if pool_label:
            pool = pool_label.find_next_sibling(string=True)
            if pool:
                return pool.strip()
        return "N/A"
    except Exception as e:
        logging.error(f"Error getting pool section: {e}")
        return "N/A"

# Function to get basement
def get_basement(soup):
    try:
        basement_label = soup.find('span', style="font-weight: bold;", string='Basement Y/N:')
        if basement_label:
            basement = basement_label.find_next_sibling(string=True)
            if basement:
                return basement.strip()
        return "N/A"
    except Exception as e:
        logging.error(f"Error getting basement: {e}")
        return "N/A"

# Function to get dwelling type
def get_dwelling_type(soup):
    try:
        dwelling_type_label = soup.find('span', style="font-weight: bold;", string='Dwelling Type:')
        if dwelling_type_label:
            dwelling_type = dwelling_type_label.find_next_sibling(string=True)
            if dwelling_type:
                return dwelling_type.strip()
        return "N/A"
    except Exception as e:
        logging.error(f"Error getting dwelling type: {e}")
        return "N/A"

# Function to get ttl monthly fee equivalent
def get_ttl_monthly_fee(soup):
    try:
        ttl_monthly_fee_label = soup.find('span', string='Ttl Mthly Fee Equiv: ')
        if ttl_monthly_fee_label:
            value_element = ttl_monthly_fee_label.next_sibling
            if value_element and isinstance(value_element, str):
                return value_element.strip()
        return "N/A"
    except Exception as e:
        logging.error(f"Error getting ttl monthly fee: {e}")
        return "N/A"

# Function to get land lease fee
def get_land_lease_fee(soup):
    try:
        land_lease_fee_label = soup.find('span', style="font-weight: bold;", string='Land Lease Fee Y/N:')
        if land_lease_fee_label:
            land_lease_fee = land_lease_fee_label.find_next_sibling(string=True)
            if land_lease_fee:
                return land_lease_fee.strip()
        return "N/A"
    except Exception as e:
        logging.error(f"Error getting land lease fee: {e}")
        return "N/A"

# Function to get list date
def get_list_date(soup):
    try:
        list_date_label = soup.find('span', style="font-size: 8pt; font-weight: bold;", string='Status Change Date:')
        if list_date_label:
            parent_td = list_date_label.find_parent('td')
            if parent_td:
                next_td = parent_td.find_next_sibling('td')
                if next_td:
                    list_date_span = next_td.find('span', style="font-size: 8pt;")
                    if list_date_span and list_date_span.text.strip():
                        return list_date_span.text.strip()
        return "N/A"
    except Exception as e:
        logging.error(f"Error getting list date: {e}")
        return "N/A"

# Function to fetch data from the website
def fetch_data(listing_number, active_status):
    try:
        # Step 1: Initial request to get the listing ID
        initial_url = f'https://apps.flexmls.com/quick_launch/herald?callback=lookupCallback&_filter={listing_number}&ql=true&search_id=4ff5ce15&client=flexmls&_selfirst=false&parse={listing_number}&_=1718042971014'
        initial_response = requests.get(initial_url, cookies=cookies)
        initial_data = initial_response.text

        # Extract the full listing ID from the initial response
        match = re.search(r'"Id":"(\d+)"', initial_data)
        if match:
            full_listing_id = match.group(1)
        else:
            logging.warning(f"Listing ID not found in the initial response for listing number {listing_number}.")
            return

        # Step 2: Use the listing ID in the next request
        final_url = f'https://armls.flexmls.com/cgi-bin/mainmenu.cgi?cmd=url%20reports/dispatcher/display_custom_report.html&wait_var=5&please_wait_override=Y&report_grid=&report_title=&fontsize=&spacing=&auto_print_report=&allow_linkbar=N&s_supp=Y&report=c,20080718115541627326000000,wysr&linkbar_toggle=&report_type=private&buscardselect=generic&override_copyright=system&qcount=1&c1=x%27{full_listing_id}%27&srch_rs=true'

        # Send the final request and get the response
        final_response = requests.get(final_url, cookies=cookies)

        # Extract relevant data from the HTML response
        soup = BeautifulSoup(final_response.text, 'html.parser')
        address_element = soup.find("td", style="text-align: center; vertical-align: top;")
        if address_element:
            address = address_element.text.strip()
            zip_code = re.search(r'\b\d{5}\b', address).group() if re.search(r'\b\d{5}\b', address) else "N/A"
        else:
            address = "N/A"
            zip_code = "N/A"

        beds_baths = get_beds_baths(soup)
        beds, baths = beds_baths.split("/") if beds_baths != "N/A" else ("N/A", "N/A")

        sqft_raw = get_text(soup, "Approx SqFt:")
        sqft = clean_sqft(sqft_raw)

        price_element = soup.find("td", style="text-align: right; vertical-align: top;")
        price = price_element.text.strip() if price_element else "N/A"

        subdivision = get_subdivision(soup)
        exterior_stories = get_exterior_stories(soup)
        encoded_features = get_encoded_features(soup)
        pool_section = get_pool_section(soup)
        basement = get_basement(soup)
        dwelling_type = get_dwelling_type(soup)
        ttl_monthly_fee = get_ttl_monthly_fee(soup)
        land_lease_fee = get_land_lease_fee(soup)
        list_date = get_list_date(soup)

        data = {
            "listing_id": listing_number,
            "address": address,
            "zip_code": zip_code,
            "price": price,
            "sqft": sqft,
            "# of Interior Levels": get_text(soup, "# of Interior Levels:"),
            "price_per_sqft": get_text(soup, "Price/SqFt:"),
            "year_built": get_text(soup, "Year Built:"),
            "beds": beds.strip(),
            "baths": baths.strip(),
            "subdivision": subdivision,
            "exterior_stories": exterior_stories,
            "encoded_features": encoded_features,
            "pool_section": pool_section,
            "basement": basement,
            "dwelling_type": dwelling_type,
            "ttl_monthly_fee": ttl_monthly_fee,
            "land_lease_fee": land_lease_fee,
            "status_change_date": list_date,
            "active": active_status  # Update the "active" field based on file type
        }

        return data
    except Exception as e:
        logging.error(f"Error processing listing number {listing_number}: {e}")

# Function to insert data into DynamoDB table in bulk
def bulk_insert_data(items):
    try:
        with table.batch_writer() as batch:
            for item in tqdm.tqdm(items, desc="Bulk Inserting Items"):
                batch.put_item(Item=item)
        logging.info("Bulk insert completed successfully.")
    except Exception as e:
        logging.error(f"Error during bulk insert: {e}")


def load_and_process_json(file_path, active_status):
    with open(file_path, 'r') as file:
        response_data = json.load(file)
    
    listing_numbers = [item['listingNumber'] for item in response_data]
    
    return listing_numbers, active_status

# Load active and closed listings
active_file = 'response_data_active.json'
closed_file = 'response_data_closed.json'

active_listings, active_status = load_and_process_json(active_file, True)
closed_listings, closed_status = load_and_process_json(closed_file, False)

# Combine listings with their respective active status
all_listings = [(ln, active_status) for ln in active_listings] + [(ln, closed_status) for ln in closed_listings]

# Use ThreadPoolExecutor to run the requests concurrently
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    fetch_futures = [executor.submit(fetch_data, ln, active_status) for ln, active_status in all_listings]
    results = []
    for future in tqdm.tqdm(as_completed(fetch_futures), total=len(fetch_futures), desc="Processing Listings"):
        try:
            result = future.result()
            if result:
                results.append(result)
        except Exception as e:
            logging.error(f"Error processing listing: {e}")

# Perform bulk insert
bulk_insert_data(results)

logging.info("Data inserted into the DynamoDB table")

