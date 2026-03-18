from asyncio.log import logger

import pandas as pd
import sys
import os
import ckanapi
import requests
from glob import glob
import pdb
import json
import logging 
import datetime
from dotenv import load_dotenv

load_dotenv()


APIKEY = os.getenv("APIKEY")
if not APIKEY:
    raise ValueError("APIKEY not found in environment variables")

ckan = ckanapi.RemoteCKAN('http://15.207.1.169/', apikey=APIKEY)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def convert_date(date_str: str) -> str:
    """Convert date string to required format."""
    try:
        date_obj = datetime.strptime(date_str, "%d-%b-%Y %I:%M %p")
        return date_obj.strftime("%Y-%d-%m %H:%M")
    except ValueError as e:
        logger.warning(f"Could not parse date {date_str}: {str(e)}")
        return date_str

def upload_dataset(pkg_dict):
    # Check if 'buyer' key exists and has at least one item
    #org_name = "Public Works Roads Department"  # Default name
    if 'buyer' in pkg_dict and pkg_dict['buyer']:
        buyer = pkg_dict['buyer']
        # Handle case where buyer might be a dict instead of list
        if isinstance(buyer, dict):
            buyer = [buyer]
        if buyer and len(buyer) > 0:
            name = buyer[0].get('name', '')
            if isinstance(name, list):
                name = name[0]  # Extract the first element if it's a list
            org_name = name.split('|')[0] if isinstance(name, str) else "Public Works Roads Department"
            print(org_name)

    # Ensure pkg_name has a valid name even if 'ocid' is missing
    ocid = pkg_dict.get('ocid', 'default_ocid')
    if isinstance(ocid, list):
        ocid = ocid[0] if ocid else 'default_ocid'  # Use the first element if it's a list
    pkg_name = str(ocid).replace(" ", "_").lower().replace("’", "").replace("–", '-').replace(',', "-").replace(":", "--").replace("?", "").replace("&amp;", "-").replace("(", "").replace(")", "").replace("&", "-").replace(".", "").replace("'", "")[:100]

    # Extract fiscal year from JSON or use default
    fiscal_year = pkg_dict.get('fiscalYear')

    try:
        package_data = {
            'name': pkg_name,
            'owner_org': org_name,
            'fiscal_year': str(fiscal_year),  # Use the extracted fiscal year
            'type': 'tender_dataset',
            'ocid': pkg_dict['ocid'],
            'id_': pkg_dict['id'],
            'date': str(pkg_dict['date']),
            'initiationType': pkg_dict['initiationType'],
            'tender': pkg_dict.get('tender', []),
            'bids': pkg_dict.get('bids', []),
            'awards': pkg_dict.get('awards', []),
            'parties': pkg_dict.get('parties', []),
            'buyer': pkg_dict.get('buyer', []),
            'statistics': pkg_dict.get('statistics', [])
        }
        package = ckan.action.package_create(**package_data)
        print(pkg_dict['tender'][0]['id'] if "tender" in pkg_dict and pkg_dict['tender'] else "No tender ID")

    except Exception as error:
        print("Error uploading dataset:", error)

    # except ckanapi.ValidationError as e:
    #     print(e)
        # if (e.error_dict['__type'] == 'Validation Error' and
        #    e.error_dict['name'] == ['That URL is already in use.']):
        #     print ('package already exists')
        #     return
        # else:
        #     raise


def clean_data(data):
    for key in data:
        if isinstance(data[key], dict):
            current_data = data[key]
            current_data = clean_data(current_data)
            current_data = [current_data]
            data[key] = current_data

        elif isinstance(data[key], list):
            data[key] = [
                clean_data(item) if isinstance(item, dict) else
                [{'value': item}] if isinstance(item, (str, int, float, bool))
                else item
                for item in data[key]
            ]

        elif isinstance(data[key], (str, int, float, bool)):
            data[key] = [{'value': data[key]}]

    return data

def main(): 
    path = r"D:\CDL\ckan-api\assam_tenders_2024_09"
    for root, _, files in os.walk(path):
        for file_name in files:
            if file_name.endswith('.json'):
                file_path = os.path.join(root, file_name)
                print(file_path)
                with open(file_path, 'r') as file:
                    try: 
                        json_data = json.load(file)
                        json_data = clean_data(json_data)
                        upload_dataset(json_data)

                    except json.JSONDecodeError:
                        print(f"Error decoding JSON from file: {path}")

# Example usage
    
    # for index, row in raw_data.iterrows():
    #     # if index < 6968:
    #     #     continue
    #     # if index > 10:
    #     #     break
    #     upload_dataset(row)
    #     print (index)


if __name__ == '__main__':
    
    main()


