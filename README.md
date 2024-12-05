# FastAPI CKAN Integration

A FastAPI application that provides a REST API interface for CKAN dataset management, with support for bulk uploads and CRUD operations.

## Features

- CRUD operations for CKAN datasets
- Bulk upload support via CSV
- Field mapping for complex CKAN schemas
- Error handling and validation
- Environment-based configuration

## Prerequisites

- Python 3.8+
- Git
- CKAN instance with API access

## Installation

1. Clone the repository:
```bash
git@github.com:Prajna1999/ocds-ckan.git
cd ocds-ckan
```


2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. To update the any field from the JSON file into CKAN. Change below code

```bash

def main():
    try:
        ckan_client = CKANClient()
        # update the json direcctory path name that contains OCDS formatted tenders
        json_directory = r"/home/prajna/civicdatalab/ocds-ckan/data/test"

        ckan_client.process_directory(json_directory)

    except Exception as e:
        logger.error(f"Main process error: {str(e)}")
        sys.exit(1)


```
  
 5. Run in the terminal
 ```bash

    python3 run_ckan.py

  ```


# Ignore below lines for now

## Running the Application

1. Start the FastAPI server:
```bash
 fastapi dev app/main.py    
```

2. Access the API documentation:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## API Endpoints

### Dataset Management

#### Create Dataset
```http
POST /api/v1/datasets/
Content-Type: application/json

{
  "title": "Dataset Title",
  "owner_org": "organization-name",
  ...
}
```

#### Get Dataset
```http
GET /api/v1/datasets/{dataset_id}
```

#### Update Dataset
```http
PUT /api/v1/datasets/{dataset_id}
Content-Type: application/json

{
  "title": "Updated Title",
  ...
}
```

#### Delete Dataset
```http
DELETE /api/v1/datasets/{dataset_id}
```

### Bulk Upload

#### Upload CSV
```http
POST /api/v1/datasets/bulk-upload/
Content-Type: multipart/form-data

file: your-csv-file.csv
```

## CSV Format

Your CSV file should have the following headers mapping to CKAN fields:

```csv
tender/title,buyer/name,Fiscal Year,ocid,initiationType,tag,id,date,tender/id,...
```

Example row:
```csv
"Sample Tender","Organization Name|Department",2024,ocid-123,tender,planning,1,2024-01-01,tender-123,...
```

## Project Structure

```
app/
├── __init__.py
├── main.py
├── config.py
├── dependencies.py
├── routers/
│   ├── __init__.py
│   └── dataset_router.py
├── crud/
│   ├── __init__.py
│   └── dataset.py
├── schemas/
│   ├── __init__.py
│   └── dataset.py
├── models/
│   └── __init__.py
├── external_services/
│   ├── __init__.py
│   └── ckan_client.py
└── utils/
    ├── __init__.py
    ├── field_mapper.py
    └── name_formatter.py
```

## Requirements

```txt
fastapi==0.104.1
uvicorn==0.24.0
python-multipart==0.0.6
pandas==2.1.3
ckanapi==4.7
python-dotenv==1.0.0
pydantic==2.5.1
pydantic-settings==2.1.0
```

## Development

1. Install development dependencies:
```bash
pip install pytest black isort flake8
```

2. Run tests:
```bash
pytest tests/
```

3. Format code:
```bash
black app/
isort app/
```

## Common Issues

1. CKAN Connection Error
   - Check if CKAN_URL is accessible
   - Verify CKAN_API_KEY is valid

2. CSV Upload Errors
   - Ensure CSV headers match the expected format
   - Check for missing required fields
   - Verify data types match schema

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

Distributed under the MIT License. See `LICENSE` for more information.