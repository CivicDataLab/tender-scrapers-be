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

ckan = ckanapi.RemoteCKAN('http://15.207.1.169/', apikey=APIKEY)



def get_datasets(q, fq, rows, start):

    datasets = []
    try:
        query = ckan.action.package_search(q=q, fq=fq, rows=rows, start=start)
        datasets = query['results']
    except Exception as e:
        print(e)

    return datasets

def edit_dataset(pkg_dict):
    try:
        #  if len(pkg_dict['fiscal_year']) < 5:
        #     fiscal_year = pkg_dict['fiscal_year'] + "-" + str(int(pkg_dict['fiscal_year'][2:]) + 1)
        #     print (fiscal_year)
        package_updated = ckan.action.package_patch(id=pkg_dict['name'], fiscal_year="2016-2017")
        print (package_updated['name'])
        
    except ckanapi.ValidationError as e:
        print(e)


def main(): 

# read all the datasets of Budgets for Justice group
    q     = ''
    fq    = 'type:tender_dataset AND fiscal_year:2016-17'   # fiscal_year:2020 AND private:false 'sector:other AND private:false' #'organization:open-budgets-india AND private:false' 
    rows  = 1000
    start = 0
    count = 0
    

    try:
        datasets = get_datasets(q, fq, rows, start)
        print(len(datasets))
        for dataset in datasets:
            edit_dataset(dataset)
    except Exception as e:
        print(e)

    # while count < 36:
    
    #     start_idx = start * count
    #     print (start_idx)
    
    #     try:
    #         datasets = get_datasets(q, fq, rows, start_idx)
    #         for dataset in datasets:
    #             edit_dataset(dataset)
    #             # return
    #     except Exception as e:
    #         print(e)
            
    #     count = count + 1


if __name__ == '__main__':
    main()
    

