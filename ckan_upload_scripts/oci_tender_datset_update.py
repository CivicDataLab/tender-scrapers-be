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

def update_dataset(pkg_dict):
    pkg_name = pkg_dict['ocid'].replace(" ", "_").lower().replace("’", "").replace("–", '-').replace(',', "-").replace(":", "--").replace("?", "").replace("&amp;", "-").replace("(", "").replace(")", "").replace("&", "-").replace(".", "").replace("'", "")[:100]
    org_name = pkg_dict['buyer/name'].split('|')[0]
    org_name = org_name.replace(" ", "_").lower().replace("’", "").replace("–", '-').replace(',', "-").replace(":", "--").replace("?", "").replace("&amp;", "-").replace("(", "").replace(")", "").replace("&", "-").replace(".", "").replace("'", "")[:100]
    
    print('----',pkg_name)
    
    print(org_name)
    # Create Package
    try:
        package = ckan.action.package_patch(id=pkg_name, title=pkg_dict['tender/title'],
                    owner_org=org_name, type='tender_dataset', fiscal_year=pkg_dict['Fiscal Year'], 
                    ocid=pkg_dict['ocid'], initiation_type=pkg_dict['initiationType'], tag=pkg_dict['tag'], data_id=pkg_dict['id'],  
                    date=str(pkg_dict['date']),
                    tender_id=pkg_dict['tender/id'], tender_externalreference=pkg_dict['tender/externalReference'], 
                    tender_title=pkg_dict['tender/title'],tender_mainprocurementcategory=pkg_dict['tender/mainProcurementCategory'],
                    tender_procurementmethod=pkg_dict['tender/procurementMethod'], tender_contracttype=pkg_dict['tender/contractType'], 
                    tenderclassification_description=pkg_dict['tenderclassification/description'],
                    tender_submission_method_details=pkg_dict['tender/submissionMethodDetails'],
                    tender_participationFee_0_multicurrencyallowed=pkg_dict['tender/participationFee/0/multiCurrencyAllowed'], 
                    tender_allowtwostagetender=pkg_dict['tender/allowTwoStageTender'], 
                    tender_value_amount=pkg_dict['tender/value/amount'], 
                    tender_datepublished=str(pkg_dict['tender/datePublished']),  tender_milestones_duedate=str(pkg_dict['tender/milestones/dueDate']), 
                    tender_tenderperiod_durationindays=pkg_dict['tender/tenderPeriod/durationInDays'], 
                    tender_allowpreferentialbidder=pkg_dict['tender/allowPreferentialBidder'],
                    payment_mode=pkg_dict['Payment Mode'],
                    tender_status=pkg_dict['tender/status'], tender_stage=pkg_dict['tender/stage'],
                    tender_numberoftenderers=pkg_dict['tender/numberOfTenderers'],
                    tender_bid_opening_date=str(pkg_dict['tender/bidOpening/date']), tender_milestones_title=str(pkg_dict['tender/milestones/title']),
                    tender_documents_id=pkg_dict['tender/documents/id'], 
                    buyer_name=pkg_dict['buyer/name'],)
                
                    # awards_0_suppliers_0_name=pkg_dict['awards/0/suppliers/0/name'], awards_0_value_currency=pkg_dict['awards/0/value/currency'], 
                    # awards_0_value_amount=pkg_dict['awards/0/value/amount'] )

        print (package['id'])
        
    except Exception as e:
        print (e)
    # except ckanapi.ValidationError as e:
    #     print("error")
        # if (e.error_dict['__type'] == 'Validation Error' and
        #    e.error_dict['name'] == ['That URL is already in use.']):
        #     print ('package already exists')
        #     return
        # else:
        #     raise



def main(): 

    raw_data = pd.read_csv(r'D:\Assam-Tenders\assam-tenders-data\code\transformation_scripts\data_to_upload_latest.csv') 
    raw_data = raw_data.fillna('')
    print (len(raw_data))


    for index, row in raw_data.iterrows():
        update_dataset(row)
        print (index)


if __name__ == '__main__':
    main()
    

