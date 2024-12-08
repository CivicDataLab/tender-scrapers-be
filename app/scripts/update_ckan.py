import pandas as pd
import sys
import os
import ckanapi
import requests
import json
import logging
from datetime import datetime
from zipfile import ZipFile
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Constants
APIKEY = "2ef8ee32-6bed-4246-aaef-d4e22cce9cce"
CKAN_URL = 'http://15.207.1.169/'
MAX_PACKAGE_NAME_LENGTH = 100


class DatasetUpdater:
    def __init__(self):
        self.ckan = ckanapi.RemoteCKAN(CKAN_URL, apikey=APIKEY)

    @staticmethod
    def unzip_file(zip_path: str, extract_path: str) -> None:
        """Unzip file to specified path."""
        try:
            with ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_path)
            logger.info(f"Successfully unzipped file to {extract_path}")
        except Exception as e:
            logger.error(f"Error unzipping file: {str(e)}")
            raise

    @staticmethod
    def convert_date(date_str: str) -> str:
        """Convert date string to required format."""
        try:
            date_obj = datetime.strptime(date_str, "%d-%b-%Y %I:%M %p")
            return date_obj.strftime("%Y-%d-%m %H:%M")
        except ValueError as e:
            logger.warning(f"Could not parse date {date_str}: {str(e)}")
            return date_str

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

        return sanitized[:MAX_PACKAGE_NAME_LENGTH]

    def update_dataset(self, pkg_dict: Dict[str, Any]) -> None:

        try:
            # Sanitize package name
            pkg_name = pkg_dict['ocid'].replace(" ", "_").lower().replace("'", "").replace("–", '-').replace(',', "-").replace(
                ":", "--").replace("?", "").replace("&amp;", "-").replace("(", "").replace(")", "").replace("&", "-").replace(".", "").replace("'", "")[:100]

            # Clean tender value amount by removing commas
            tender_value = pkg_dict['tenderValueAmount'].replace(",", "")

            # Extract tender details
            tender_details = pkg_dict.get('tender', [{}])[
                0] if pkg_dict.get('tender') else {}

            # Create package update data
            package_data = {
                'id': pkg_name,
                'ocid': pkg_dict['ocid'],
                'data_id_': pkg_dict['id'],
                'date': str(pkg_dict['date']),
                'initiation_type': 'tender',
                'bidOpeningDate': str(self.convert_date(pkg_dict['bidOpeningDate'])),
                'mainProcurementCategory': tender_details['mainProcurementCategory'],
                'tenderValueAmount': tender_value,
                'fiscalYear': str(tender_details['fiscalYear']),
                # 'fiscal_year': "2022-2024",
                # Additional tender details
                'title': tender_details.get('title', ''),
                'description': tender_details.get('description', ''),
                'procurementMethod': tender_details.get('procurementMethod', ''),
                # 'tender_contract_type': tender_details.get('contractType', ''),
                # 'tender_date_published': str(self.convert_date(tender_details.get('datePublished', ''))) if tender_details.get('datePublished') else ''
            }

            # Update the package
            package = self.ckan.action.package_patch(**package_data)
            print(f"Successfully updated package: {pkg_name}")
            print(f"OCID: {pkg_dict['ocid']}")

        except KeyError as e:
            print(f"Missing required field in package data: {str(e)}")
        except Exception as e:
            print(f"Error updating dataset: {str(e)}")
            print(f"Package data: {pkg_dict}")

    def process_json_file(self, file_path: str) -> None:
        """Process single JSON file."""
        try:
            with open(file_path, 'r') as file:
                json_data = json.load(file)
                self.update_dataset(json_data)
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


def main():
    try:
        # Initialize the updater
        updater = DatasetUpdater()

        # Define paths
        json_directory = r"/home/prajna/civicdatalab/ocds-ckan/data/test"

        # Optional: If you need to unzip first
        # zip_path = r"/home/prajna/civicdatalab/ocds-ckan/data/json_data/Raw_json.zip"
        # updater.unzip_file(zip_path, json_directory)

        # Process all JSON files
        updater.process_directory(json_directory)

    except Exception as e:
        logger.error(f"Main process error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
