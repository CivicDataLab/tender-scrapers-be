import pandas as pd
import sys
import os
import ckanapi
import requests
from glob import glob
import pdb
import json
from dotenv import load_dotenv

load_dotenv()


APIKEY = os.getenv("APIKEY")
if not APIKEY:
    raise ValueError("APIKEY not found in environment variables")

ckan = ckanapi.RemoteCKAN('http://15.207.1.169/', apikey=APIKEY)

def upload_dataset(pkg_dict):
    pkg_name = pkg_dict['ocid'].replace(" ", "_").lower().replace("’", "").replace("–", '-').replace(',', "-").replace(":", "--").replace("?", "").replace("&amp;", "-").replace("(", "").replace(")", "").replace("&", "-").replace(".", "").replace("'", "")[:100]
    pdb.set_trace()
    org_name = pkg_dict['buyer'][0]['name'].split('|')[0]
    org_name = org_name.replace(" ", "_").lower().replace("’", "").replace("–", '-').replace(',', "-").replace(":", "--").replace("?", "").replace("&amp;", "-").replace("(", "").replace(")", "").replace("&", "-").replace(".", "").replace("'", "")[:100]
    # Create Package
    # print(type(pkg_dict['tender']['communication'][0]['documentAvailabilityPeriod']))
    try:

        package = ckan.action.package_create(name=pkg_name,owner_org=org_name, type='tender_dataset',
                    title=pkg_dict['tender'][0]['title'], ocid = pkg_dict['ocid'], id_ = pkg_dict['id'], date = pkg_dict['date'],
                    initiationType = pkg_dict['initiationType'],parties = pkg_dict['parties'] if "parties" in pkg_dict else [],
                    buyer = pkg_dict['buyer'] if "buyer" in pkg_dict else [] , tender = pkg_dict['tender'] if "tender" in pkg_dict else [],
                    bids = pkg_dict['bids'] if 'bids' in pkg_dict else [], awards = pkg_dict['awards'] if "awards" in pkg_dict else pkg_dict,
                    statistics = pkg_dict['statistics'] if "statistics" in pkg_dict else [] )

        print (package['parties'])
        
    except Exception as error:
        print (error)
    # except ckanapi.ValidationError as e:
    #     print(e)
        # if (e.error_dict['__type'] == 'Validation Error' and
        #    e.error_dict['name'] == ['That URL is already in use.']):
        #     print ('package already exists')
        #     return
        # else:
        #     raise

def get_all_dataset():
        package_list = ckan.action.package_list()
        # package_list = package_list['result']
        return package_list

def delete_dataset(each):
    try:
        ckan.action.package_delete(id=each)
    except Exception as e:
        print(e)

def main(): 
    dataset_list = get_all_dataset()
    for index, each in enumerate(dataset_list):
        if each not in ["percentage-of-tenders-completed","proportion-of-bids","proportion-of-procurement-method-types","proportion-of-saving", "proportion-of-value-awarded-in-single-bid-tenders"]:
            print("---------------------",index,each)
            delete_dataset(each)
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
    

