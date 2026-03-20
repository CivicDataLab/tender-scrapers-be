import os
import json
import ckanapi
from dotenv import load_dotenv

load_dotenv()


APIKEY = os.getenv("APIKEY")
if not APIKEY:
    raise ValueError("APIKEY not found in environment variables")

JSON_DIR = r"C:\Users\AAKASH\Desktop\go\Assam Tenders json 2023\2024-2025"  # Directory with your JSON files

# Initialize CKAN client
ckan = ckanapi.RemoteCKAN('http://15.207.1.169/', apikey=APIKEY)

def sanitize_pkg_name(ocid):
    # Sanitizing the ocid to match your dataset name logic
    pkg_name = (ocid.replace(" ", "_").lower().replace("’", "").replace("–", '-').replace(',', "-").replace(":", "--").replace("?", "").replace("&amp;", "-").replace("(", "").replace(")", "").replace("&", "-").replace(".", "").replace("'", "")[:100])
    print(pkg_name)
    return pkg_name

def update_fiscal_year(ocid, id, fiscal_year="2024-2025"):
    pkg_name = sanitize_pkg_name(ocid)
    try:
        ckan.action.package_patch(
            name=pkg_name,
            id=id,
            fiscal_year=fiscal_year
        )
        print(f"Updated '{pkg_name}' to fiscal year {fiscal_year}")
    except ckanapi.ValidationError as e:
        print(f"Validation error for '{pkg_name}':", e)
    except Exception as ex:
        print(f"Error updating '{pkg_name}':", ex)

def main():
    # Iterate over each file in the JSON_DIR
    for filename in os.listdir(JSON_DIR):
        if filename.lower().endswith(".json"):
            file_path = os.path.join(JSON_DIR, filename)
            # Load the JSON data
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                    ocid = json_data.get("ocid")
                    id = json_data.get("id")
                    if ocid:
                        update_fiscal_year(ocid, "2024-2025",id)
                    else:
                        print(f"No 'ocid' found in {filename}")
            except json.JSONDecodeError as jde:
                print(f"JSON decode error in {filename}:", jde)

if __name__ == "__main__":
    main()
