import ckanapi
from app.config import settings

class CKANClient:
    def __init__(self) -> None:
        self.client=ckanapi.RemoteCKAN(settings.CKAN_URL, apikey=settings.CKAN_API_KEY)
        
    def create_package(self,package_data:dict):
        try:
            return self.client.action.package_create(**package_data)
        except ckanapi.ValidationError as e:
            raise ValueError(str(e))
    
    def get_package(self, package_id:str):
        
        return self.client.action.package_show(id=package_id)
      
    def update_package(self, package_data:dict):
        try:
            return self.client.action.package_update(**package_data)
        except ckanapi.ValidationError as e:
            raise ValueError(str(e))
    
    def delete_package(self, package_id:str):
        return self.client.action.package_delete(id=package_id)  