import ckanapi
import logging
import sys
import os
import json
from datetime import datetime
from app.config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


class CKANClient:
    def __init__(self) -> None:
        self.client = ckanapi.RemoteCKAN(
            settings.CKAN_URL, apikey=settings.CKAN_API_KEY)

    @staticmethod
    def sanitize_package_name(name: str) -> str:
        """Sanitize package name according to CKAN requirements."""
        replacements = {
            " ": "_",
            "'": "",
            "–": "-",
            ",": "-",
            ":": "--",
            "?": "",
            "&amp;": "-",
            "(": "",
            ")": "",
            "&": "-",
            ".": "",
            "'": ""
        }

        sanitized = name.lower()
        for old, new in replacements.items():
            sanitized = sanitized.replace(old, new)

        return sanitized[:100]

    @staticmethod
    def convert_date(date_str: str) -> str:
        """Convert date string to required format."""
        try:
            date_obj = datetime.strptime(date_str, "%d-%b-%Y %I:%M %p")
            return date_obj.strftime("%Y-%d-%m %H:%M")
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

        # Clean tender value amount by removing commas
        tender_value = pkg_dict['tenderValueAmount'].replace(",", "")

        # Extract tender details
        tender_details = pkg_dict.get('tender', [{}])[
            0] if pkg_dict.get('tender') else {}

        print()
        try:

         # Create package update data
            package_data = {
                'id': str(self.sanitize_package_name(pkg_dict['ocid'])),
                'ocid': pkg_dict['ocid'],
                'data_id_': pkg_dict['id'],
                'date': str(pkg_dict['date']),
                'initiation_type': 'tender',
                'tender_bid_opening_date': str(self.convert_date(pkg_dict['bidOpeningDate'])),
                'tender_mainprocurementcategory': pkg_dict['mainProcurementCategory'],
                'tender_value_amount': tender_value,
                'fiscal_year': str(pkg_dict['fiscalYear']),
                # 'fiscal_year': "2023-2024",
                # Additional tender details
                'tender_title': tender_details.get('title', ''),
                'tender_description': tender_details.get('description', ''),
                'tender_procurement_method': tender_details.get('procurementMethod', ''),
                'tender_contract_type': tender_details.get('contractType', ''),
                'tender_date_published': str(self.convert_date(tender_details.get('datePublished', ''))) if tender_details.get('datePublished') else ''
            }
            self.client.action.package_update(**package_data)
            logger.info(f"Successfully updated package: {pkg_dict}")
            logger.info(f"OCID: {pkg_dict['ocid']}")

        except KeyError as e:
            logger.info(f"Missing required field in package data: {str(e)}")
        except Exception as e:
            logger.info(f"Error updating dataset: {str(e)}")
            logger.info(f"Package data: {pkg_dict}")

        except ckanapi.ValidationError as e:
            raise ValueError(str(e))

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
        json_directory = r"/home/prajna/civicdatalab/ocds-ckan/data/test"

        ckan_client.process_directory(json_directory)

    except Exception as e:
        logger.error(f"Main process error: {str(e)}")
        sys.exit(1)
