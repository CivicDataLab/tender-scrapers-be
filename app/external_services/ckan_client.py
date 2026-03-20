import ckanapi
import logging
import sys
import os
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

# Load CKAN schema for repeating_subfields conversion
SCHEMA_PATH = os.getenv(
    "CKAN_SCHEMA_PATH",
    os.path.normpath(os.path.join(
        os.path.dirname(os.path.abspath(__file__)), '..', '..', 'tender_dataset_schema.json'
    ))
)
with open(SCHEMA_PATH) as f:
    CKAN_SCHEMA = json.load(f)
DATASET_FIELDS = CKAN_SCHEMA['dataset_fields']


class CKANClient:
    def __init__(self) -> None:
        self.client = ckanapi.RemoteCKAN(
            os.getenv("CKAN_URL", "http://15.207.1.169/"),
            apikey=os.getenv("CKAN_API_KEY"),
        )

    @staticmethod
    def convert_for_ckan(data, schema_fields):
        """Recursively convert nested dicts to lists of dicts
        where the CKAN schema expects repeating_subfields."""
        if not isinstance(data, dict):
            return data

        field_defs = {f['field_name']: f for f in schema_fields}

        result = {}
        for key, value in data.items():
            field_def = field_defs.get(key)

            if field_def and 'repeating_subfields' in field_def:
                subfields = field_def['repeating_subfields']

                if isinstance(value, dict):
                    converted = CKANClient.convert_for_ckan(value, subfields)
                    result[key] = [converted]
                elif isinstance(value, list):
                    result[key] = [
                        CKANClient.convert_for_ckan(item, subfields)
                        if isinstance(item, dict) else item
                        for item in value
                    ]
                else:
                    result[key] = value
            else:
                result[key] = value

        return result

    @staticmethod
    def add_missing_ids(pkg_dict):
        """Add missing 'id' fields required by CKAN schema before conversion."""
        if 'bids' in pkg_dict and isinstance(pkg_dict['bids'], dict):
            bids = {'id': 1}
            bids.update(pkg_dict['bids'])
            pkg_dict['bids'] = bids

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

    @staticmethod
    def extract_top_level_fields(data):
        """Extract top-level CKAN fields from nested tender data."""
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

    @staticmethod
    def sanitize_package_name(name: str) -> str:
        """Sanitize package name according to CKAN requirements."""
        replacements = {
            " ": "_", "'": "", "\u2013": "-", ",": "-", ":": "--",
            "?": "", "&amp;": "-", "(": "", ")": "", "&": "-",
            ".": "", "\u2019": ""
        }
        sanitized = name.lower()
        for old, new in replacements.items():
            sanitized = sanitized.replace(old, new)
        return sanitized[:100]

    @staticmethod
    def convert_date(date_str: str) -> str:
        """Convert date string from '%d-%b-%Y %I:%M %p' to '%Y-%m-%d %H:%M'."""
        try:
            date_obj = datetime.strptime(date_str, "%d-%b-%Y %I:%M %p")
            return date_obj.strftime("%Y-%m-%d %H:%M")
        except ValueError as e:
            logger.warning(f"Could not parse date {date_str}: {str(e)}")
            return date_str

    def create_package(self, package_data: dict):
        try:
            return self.client.action.package_create(**package_data)
        except ckanapi.ValidationError as e:
            raise ValueError(str(e))

    def get_package(self, package_id: str):
        return self.client.action.package_show(id=package_id)

    def update_package(self, pkg_dict: dict):
        """Update a CKAN package from JSON data with format conversion."""
        # Pre-process: add missing id fields
        self.add_missing_ids(pkg_dict)

        # Extract top-level fields before conversion
        top_fields = self.extract_top_level_fields(pkg_dict)

        # Apply schema-aware conversion (dict -> list wrapping)
        converted = self.convert_for_ckan(pkg_dict, DATASET_FIELDS)

        # Merge extracted top-level fields
        converted.update(top_fields)

        # Build package name and owner_org
        pkg_name = self.sanitize_package_name(converted.get('ocid', ''))

        buyer = converted.get('buyer', [])
        if isinstance(buyer, list) and buyer:
            org_name = self.sanitize_package_name(buyer[0].get('name', ''))
        elif isinstance(buyer, dict):
            org_name = self.sanitize_package_name(buyer.get('name', ''))
        else:
            org_name = ''

        # Get title from tender
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

            self.client.action.package_patch(**package_data)
            logger.info(f"Updated package: {pkg_name}")

        except ckanapi.ValidationError as e:
            logger.error(f"Validation error for {pkg_name}: {e}")
        except KeyError as e:
            logger.error(f"Missing required field: {str(e)}")
        except Exception as e:
            logger.error(f"Error updating {pkg_name}: {str(e)}")

    def process_json_file(self, file_path: str) -> None:
        """Process single JSON file."""
        try:
            with open(file_path, 'r') as file:
                json_data = json.load(file)
                self.update_package(json_data)
        except json.JSONDecodeError as e:
            logger.error(
                f"Error decoding JSON from file {file_path}: {str(e)}")
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")

    def process_directory(self, directory_path: str) -> None:
        """Process all JSON files in directory."""
        try:
            for root, _, files in os.walk(directory_path):
                for index, file_name in enumerate(files):
                    if file_name.endswith('.json'):
                        file_path = os.path.join(root, file_name)
                        logger.info(
                            f"Processing file {index + 1}: {file_name}")
                        self.process_json_file(file_path)
        except Exception as e:
            logger.error(
                f"Error processing directory {directory_path}: {str(e)}")

    def delete_package(self, package_id: str):
        return self.client.action.package_delete(id=package_id)


def main():
    try:
        ckan_client = CKANClient()
        json_directory = "/home/aakash/cdl/oci/tender-scrapers-be/data/extract/assam_tenders_2024_09"
        ckan_client.process_directory(json_directory)

    except Exception as e:
        logger.error(f"Main process error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
