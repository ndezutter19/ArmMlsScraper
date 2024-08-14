from pyairtable import Api
import os

airtable_api_key = os.getenv('AIRTABLE_API_KEY')
airtable_base_id = os.getenv('AIRTABLE_BASE_ID')
airtable_table_name = 'Deals Table'

api = Api(airtable_api_key)
table = api.table(airtable_base_id, airtable_table_name)

# Test creating a dummy record
table.create({
    'TestField': 'This is a test'
})
