import ckanapi
import os
import json
import logging



# APIKEY = os.getenv("APIKEY")
# if not APIKEY:
#     raise ValueError("APIKEY not found in environment variables")

ckan = ckanapi.RemoteCKAN('http://15.207.1.169/', apikey="2ef8ee32-6bed-4246-aaef-d4e22cce9cce")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def download_schema():

    schema = ckan.action.scheming_dataset_schema_show(type='tender_dataset')

    with open('tender_dataset_schema.json', 'w') as f:
        json.dump(schema, f, indent=4)
    return

def download_dataset_json():

    dataset = ckan.action.package_show(id='ocds-f5kvwu-2024_botc_36678_1')
    with open('dataset_sample.json', 'w') as f:
        json.dump(dataset, f, indent=2)
    return

if __name__ == "__main__":
    # download_schema()
    download_dataset_json()