import pandas as pd
import sys
import os
import ckanapi
import requests
import json
import pdb
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


APIKEY = os.getenv("APIKEY")
if not APIKEY:
    raise ValueError("APIKEY not found in environment variables")

ckan = ckanapi.RemoteCKAN('http://15.207.1.169/', apikey=APIKEY)

def convert_date(date_str):
    try:
        # Convert the string to datetime object in the given format
        date_obj = datetime.strptime(date_str, "%d-%b-%Y %I:%M %p")
        # Convert to the required format
        return date_obj.strftime("%Y-%m-%d %H:%M")
    except ValueError:
        # Return the original date if it can't be parsed
        return date_str

def update_dataset(pkg_dict):
    pkg_name = pkg_dict['ocid'].replace(" ", "_").lower().replace("’", "").replace("–", '-').replace(',', "-").replace(":", "--").replace("?", "").replace("&amp;", "-").replace("(", "").replace(")", "").replace("&", "-").replace(".", "").replace("'", "")[:100]
    formated_date = convert_date(pkg_dict['bidOpeningDate'])
    try:
        package = ckan.action.package_patch(
        id=pkg_name,
        extras=[
        {"key": "fiscal_year", "value": str(pkg_dict['fiscalYear'])},
        {"key": "tender_mainprocurementcategory", "value": pkg_dict['mainProcurementCategory']},
        {"key": "tender_bid_opening_date", "value": str(formated_date)},
        {"key": "tender_value_amount", "value": str(pkg_dict['tenderValueAmount'])}
        ]
)
        print(pkg_dict['ocid'])
        print(formated_date)

    except Exception as e:
        print (e)

def clean_data(data):
    for key in data:
        if isinstance(data[key], dict) : 
            current_data = data[key]
            current_data = clean_data(current_data)
            current_data = [current_data]
    return data



def main(): 
    path = r"C:\Users\AAKASH\Desktop\go\Assam Tenders json 2023\publishdate"
    for root, _, files in os.walk(path):
        for index,file_name in enumerate(files):
            print(index)
            if file_name.endswith('.json'):
                file_path = os.path.join(root, file_name)
                with open(file_path, 'r') as file:
                    try: 
                        json_data = json.load(file)
                        print("Keys in JSON:", json_data.keys())
                        print("OCID:", json_data.get('ocid'))
                        print("fiscalYear:", json_data.get('fiscalYear'))
                        json_data = clean_data(json_data)
                        update_dataset(json_data)

                    except json.JSONDecodeError:
                        print(f"Error decoding JSON from file: {path}")


if __name__ == '__main__':
    main()
    



