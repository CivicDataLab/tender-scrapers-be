from app.external_services.ckan_client import CKANClient
from app.schemas.dataset import DatasetCreate, DatasetUpdate
from app.utils.name_formatter import format_package_name

class DatasetCRUD:
    def __init__(self) -> None:
        self.ckan_client=CKANClient()
        
    def create(self, dataset: DatasetCreate):
        package_name=format_package_name(dataset.ocid)
        package_data=dataset.model_dump()
        package_data["name"]=package_name
        return self.ckan_client.create_package(package_data)
    def get(self,dataset_id:str):
        return self.ckan_client.get_package(dataset_id)
    
    def update(self,dataset_id:str,dataset:DatasetUpdate):
        package_data=dataset.model_dump()
        package_data["id"]=dataset_id
        return self.ckan_client.update_package(package_data)
    def delete(self,dataset_id:str):
        return self.ckan_client.delete_package(dataset_id)
    
    