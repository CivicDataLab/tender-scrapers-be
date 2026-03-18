import pandas as pd
import sys
import os
import ckanapi
import requests
from dotenv import load_dotenv

load_dotenv()


APIKEY = os.getenv("APIKEY")
if not APIKEY:
    raise ValueError("APIKEY not found in environment variables")

ckan = ckanapi.RemoteCKAN('http://15.206.122.72/', apikey=APIKEY)

def update_dataset(pkg_dict):
    pkg_name = pkg_dict['ocid'].replace(" ", "_").lower().replace("’", "").replace("–", '-').replace(',', "-").replace(":", "--").replace("?", "").replace("&amp;", "-").replace("(", "").replace(")", "").replace("&", "-").replace(".", "").replace("'", "")[:100] 
    print('----',pkg_name)
    # Create Package
    try:
        package = ckan.action.package_patch(id=pkg_name, 
                    awards_date=pkg_dict['awards/date'], 
                    awards_value_amount=pkg_dict['awards/value/amount'], 
                    )

        print (package['id'])
        
    except Exception as e:
        print (e)
    except ckanapi.ValidationError as e:
        print("error")
        if (e.error_dict['__type'] == 'Validation Error' and
           e.error_dict['name'] == ['That URL is already in use.']):
            print ('package already exists')
            return
        else:
            pass



def main(): 

    raw_data = pd.read_csv(r'D:\Assam-Tenders\assam-tenders-data\code\transformation_scripts\data_to_upload_latest.csv') 
    raw_data = raw_data.fillna('')
    print (len(raw_data))


    for index, row in raw_data.iterrows():
        if index > 2230:
            update_dataset(row)
            print (index)
        # break


if __name__ == '__main__':
    main()
    

