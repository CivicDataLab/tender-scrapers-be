import os
import json
import shutil

# Directory containing the JSON files
json_dir = r"C:\Users\AAKASH\Desktop\go\Assam Tenders json 2023\publishdate"  # Update this path

# Base directory where you want the fiscal year folders to be created
target_base_dir = r"C:\Users\AAKASH\Desktop\go\Assam Tenders json 2023"  # Update this path

for filename in os.listdir(json_dir):
    if filename.lower().endswith('.json'):
        file_path = os.path.join(json_dir, filename)

        # Read and preprocess file content
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Replace NaN with null to avoid JSON parsing errors
        # (If your JSON doesn't contain NaN, you can skip this step)
        content = content.replace('NaN', 'null')

        # Parse the JSON content
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            print(f"Error decoding {filename}: {e}")
            continue

        # Extract the fiscalYear
        fiscal_year = data.get("fiscalYear")
        if not fiscal_year:
            print(f"No fiscalYear found in {filename}")
            continue

        # Construct the target directory path
        target_dir = os.path.join(target_base_dir, fiscal_year)

        # Create the target directory if it doesn't exist
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        # Move the file into the target directory
        target_path = os.path.join(target_dir, filename)
        shutil.move(file_path, target_path)

        print(f"Moved {filename} to {target_dir}")
