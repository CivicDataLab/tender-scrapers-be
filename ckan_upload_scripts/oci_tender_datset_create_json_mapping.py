import os
import json
import logging
import ckanapi
from dotenv import load_dotenv
import datetime

load_dotenv()

APIKEY = os.getenv("APIKEY")
if not APIKEY:
    raise ValueError("APIKEY not found in environment variables")

CKAN_URL = os.getenv("CKAN_URL", "http://15.207.1.169/")
ckan = ckanapi.RemoteCKAN(CKAN_URL, apikey=APIKEY)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load CKAN schema for repeating_subfields conversion
SCHEMA_PATH = os.getenv(
    "CKAN_SCHEMA_PATH",
    os.path.normpath(os.path.join(
        os.path.dirname(os.path.abspath(__file__)), '..', '..', 'tender-scrapers-be/tender_dataset_schema.json'
    ))
)
with open(SCHEMA_PATH) as f:
    CKAN_SCHEMA = json.load(f)
DATASET_FIELDS = CKAN_SCHEMA['dataset_fields']


def convert_for_ckan(data, schema_fields):

    if not isinstance(data, dict):
        return data

    field_defs = {f['field_name']: f for f in schema_fields}

    result = {}
    for key, value in data.items():
        field_def = field_defs.get(key)

        if field_def and 'repeating_subfields' in field_def:
            subfields = field_def['repeating_subfields']

            if isinstance(value, dict):
                converted = convert_for_ckan(value, subfields)
                result[key] = [converted]
            elif isinstance(value, list):
                result[key] = [
                    convert_for_ckan(item, subfields) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                result[key] = value
        else:
            result[key] = value

    return result

def convert_date(date_str: str) -> str:
    """Convert date string from '%d-%b-%Y %I:%M %p' to '%Y-%m-%d %H:%M'."""
    try:
        date_obj = datetime.strptime(date_str, "%d-%b-%Y %I:%M %p")
        return date_obj.strftime("%Y-%m-%d %H:%M")
    except ValueError as e:
        logger.warning(f"Could not parse date {date_str}: {str(e)}")
        return date_str


def extract_top_level_fields(data):

    tender = data.get('tender', {})
    if isinstance(tender, list):
        tender = tender[0] if tender else {}

    fields = {}

    if 'fiscalYear' not in data and 'fiscalYear' in tender:
        fields['fiscalYear'] = tender['fiscalYear']

    if 'mainProcurementCategory' not in data and 'mainProcurementCategory' in tender:
        fields['mainProcurementCategory'] = tender['mainProcurementCategory']

    value = tender.get('value', {})
    if isinstance(value, list):
        value = value[0] if value else {}
    if 'tenderValueAmount' not in data and isinstance(value, dict) and 'amount' in value:
        fields['tenderValueAmount'] = value['amount']

    bid_opening = tender.get('bidOpening', {})
    if isinstance(bid_opening, list):
        bid_opening = bid_opening[0] if bid_opening else {}
    if 'bidOpeningDate' not in data and isinstance(bid_opening, dict) and 'date' in bid_opening:
        fields['bidOpeningDate'] = bid_opening['date']

    tag = data.get('tag', [])
    if 'tenderStatus' not in data and tag:
        fields['tenderStatus'] = tag[0] if isinstance(tag, list) else tag

    return fields


def sanitize_package_name(name):

    replacements = {
        " ": "_", "'": "", "\u2013": "-", ",": "-", ":": "--",
        "?": "", "&amp;": "-", "(": "", ")": "", "&": "-",
        ".": "", "\u2019": ""
    }
    sanitized = name.lower()
    for old, new in replacements.items():
        sanitized = sanitized.replace(old, new)
    return sanitized[:100]


def add_missing_ids(pkg_dict):

    # bids: source is a dict {details: [...]}, CKAN needs [{id, details}]
    if 'bids' in pkg_dict and isinstance(pkg_dict['bids'], dict):
        bids = {'id': 1}
        bids.update(pkg_dict['bids'])
        pkg_dict['bids'] = bids

    # communication inside tender: CKAN schema requires an id subfield
    tender = pkg_dict.get('tender')
    if isinstance(tender, dict):
        comm = tender.get('communication')
        if isinstance(comm, dict) and 'id' not in comm:
            comm['id'] = 1
    elif isinstance(tender, list):
        for t in tender:
            if isinstance(t, dict):
                comm = t.get('communication')
                if isinstance(comm, dict) and 'id' not in comm:
                    comm['id'] = 1


def upload_dataset(pkg_dict):

    # Pre-process: add missing id fields
    add_missing_ids(pkg_dict)

    # Extract top-level fields before conversion (reads from raw nested data)
    top_fields = extract_top_level_fields(pkg_dict)

    # Apply schema-aware conversion (dict -> list wrapping)
    converted = convert_for_ckan(pkg_dict, DATASET_FIELDS)

    # Merge extracted top-level fields
    converted.update(top_fields)

    # Build package name and owner_org
    ocid = converted.get('ocid', 'default_ocid')
    pkg_name = sanitize_package_name(str(ocid))

    buyer = converted.get('buyer', [])
    if isinstance(buyer, list) and buyer:
        org_name = sanitize_package_name(buyer[0].get('name', ''))
    elif isinstance(buyer, dict):
        org_name = sanitize_package_name(buyer.get('name', ''))
    else:
        org_name = ''

    # Set title from tender
    tender_data = converted.get('tender', [])
    title = pkg_name
    if isinstance(tender_data, list) and tender_data:
        title = tender_data[0].get('title', pkg_name)

    try:
        package_data = {
            'id': pkg_name,
            'name': pkg_name,
            'title': title,
            'owner_org': org_name,
            'type': 'tender_dataset',
            'ocid': converted.get('ocid', ''),
            'id_': converted.get('id', ''),
            'date': str(converted.get('date', '')),
            'initiationType': converted.get('initiationType', 'tender'),
            'tenderStatus': converted.get('tenderStatus', ''),
            'fiscalYear': str(converted.get('fiscalYear', '')),
            'mainProcurementCategory': converted.get('mainProcurementCategory', ''),
            'tenderValueAmount': str(converted.get('tenderValueAmount', '')),
            'bidOpeningDate': str(converted.get('bidOpeningDate', '')),
            'tender': converted.get('tender', []),
            'bids': converted.get('bids', []),
            'awards': converted.get('awards', []),
            'parties': converted.get('parties', []),
            'buyer': converted.get('buyer', []),
            'statistics': converted.get('statistics', []),
        }

        ckan.action.package_create(**package_data)
        logger.info(f"Created: {pkg_name}")
    except ckanapi.ValidationError as e:
        err_dict = getattr(e, 'error_dict', {})
        if ('name' in err_dict and 'That URL is already in use.' in str(err_dict.get('name', ''))) or \
           ('id' in err_dict and 'Dataset id already exists' in str(err_dict.get('id', ''))):
            logger.warning(f"Package already exists: {pkg_name}, attempting update...")
            try:
                ckan.action.package_patch(**package_data)
                logger.info(f"Updated: {pkg_name}")
            except Exception as update_error:
                logger.error(f"Error updating {pkg_name}: {update_error}")
        else:
            logger.error(f"Validation error for {pkg_name}: {e}")
    except Exception as error:
        logger.error(f"Error uploading {pkg_name}: {error}")
    


def main():
    path = "/home/aakash/cdl/oci/tender-scrapers-be/data/extract/assam_tenders_2025_oct-2026_march"
    success = 0
    errors = 0
    for root, _, files in os.walk(path):
        for file_name in files:
            if file_name.endswith('.json'):
                file_path = os.path.join(root, file_name)
                try:
                    with open(file_path, 'r') as file:
                        json_data = json.load(file)
                        upload_dataset(json_data)
                        success += 1
                except json.JSONDecodeError:
                    logger.error(f"Error decoding JSON: {file_path}")
                    errors += 1
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {e}")
                    errors += 1

    logger.info(f"Done. Success: {success}, Errors: {errors}")
    


if __name__ == '__main__':
    main()
