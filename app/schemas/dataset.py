from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel


class DatasetBase(BaseModel):
    name: Optional[Any] = None
    title: Optional[Any] = None
    owner_org: Optional[Any] = None
    type: Optional[Any] = "tender_dataset"
    fiscal_year: Optional[Any] = None
    ocid: Optional[Any] = None
    initiation_type: Optional[Any] = None
    tag: Optional[Any] = None
    data_id: Optional[Any] = None
    date: datetime
    tender_id: Optional[Any]
    tender_externalreference: Optional[Any]
    tender_title: Optional[Any]
    tender_mainprocurementcategory: Optional[Any]
    tender_procurementmethod: Optional[Any]
    tender_contracttype: Optional[Any]
    tenderclassification_description: Optional[Any]
    tender_submission_method_details: Optional[Any]
    tender_participationFee_0_multicurrencyallowed: Optional[Any]
    tender_allowtwostagetender: Optional[Any]
    tender_value_amount: float
    tender_datepublished: datetime
    tender_milestones_duedate: datetime
    tender_tenderperiod_durationindays: int
    tender_allowpreferentialbidder: Optional[Any]
    payment_mode: Optional[Any]
    tender_status: Optional[Any]
    tender_stage: Optional[Any]
    tender_numberoftenderers: int
    tender_bid_opening_date: datetime
    tender_milestones_title: Optional[Any]
    tender_documents_id: Optional[Any]
    buyer_name: Optional[Any]


class DatasetCreate(DatasetBase):
    pass


class DatasetUpdate(DatasetBase):
    pass


class Dataset(DatasetBase):
    id: str

    class Config:
        from_attributes = True
