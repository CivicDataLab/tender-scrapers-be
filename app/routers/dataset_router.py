from fastapi import APIRouter, HTTPException, UploadFile, File
from app.crud.dataset import DatasetCRUD
from app.schemas.dataset import DatasetCreate, Dataset, DatasetUpdate
from app.utils.field_mapper import CSV_TO_MODEL_MAPPING
import pandas as pd
from io import StringIO
router = APIRouter()
crud = DatasetCRUD()


@router.post("/datasets/", response_model=Dataset)
async def create_dataset(dataset: DatasetCreate):
    try:
        return await crud.create(dataset)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/datasets/{dataset_id}", response_model=Dataset)
async def get_dataset(dataset_id: str):
    try:
        return await crud.get(dataset_id)
    except Exception as e:
        raise HTTPException(status_code=404, detail="Dataset not found")


@router.put("/datasets/{dataset_id}", response_model=Dataset)
async def update_dataset(dataset_id: str, dataset: DatasetUpdate):
    try:
        return await crud.update(dataset_id, dataset)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/datasets/{dataset_id}")
async def delete_dataset(dataset_id: str):
    try:
        crud.delete(dataset_id)
        return {"message": "Dataset deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=404, detail="Dataset not found")

# bulk upload


@router.post("/datasets/bulk-upload")
async def bulk_upload_datasets(
    file: UploadFile = File(...,
                            description="CSV File containing dataset records")
):
    try:
        # Validate file type
        if not file.filename.endswith('.csv'):
            raise HTTPException(
                status_code=400,
                detail="Invalid file format. Please upload a CSV file."
            )

        # Read the contents of the file
        contents = await file.read()
        try:
            # Decode bytes to string and create DataFrame
            csv_string = contents.decode()
            df = pd.read_csv(StringIO(csv_string))
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail="Error parsing CSV file. Please check the file format."
            )

        df = df.fillna("")

        results = []
        errors = []

        for index, row in df.iterrows():
            try:
                # Map CSV fields to model fields
                mapped_data = {
                    CSV_TO_MODEL_MAPPING[col]: value
                    for col, value in row.items()
                    if col in CSV_TO_MODEL_MAPPING
                }

            # Add required default fields
                mapped_data['type'] = 'tender_dataset'
                mapped_data['owner_org'] = mapped_data['buyer_name'].split(
                    '|')[0].strip().lower().replace(' ', '_')

                dataset = DatasetCreate(**mapped_data)
                result = crud.create(dataset)
                results.append(
                    {"row": index + 1, "status": "success", "id": result.get("id")})

            except Exception as e:
                errors.append({"row": index + 1, "error": str(e)})

        return {
            "success_count": len(results),
            "error_count": len(errors),
            "successes": results,
            "errors": errors
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
