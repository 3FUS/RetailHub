from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List


class CommissionBase(BaseModel):
    fiscal_month: str
    store_code: str
    fiscal_period: Optional[str] = None
    status: Optional[str] = "saved"
    remarks: Optional[str] = None

class CommissionCreate(CommissionBase):
    pass


class CommissionUpdate(CommissionBase):
    pass


class CommissionStaffCreate(BaseModel):
    fiscal_month: str
    staff_code: str
    store_code: str
    amount: float


class BatchApprovedCommission(BaseModel):
    fiscal_month: str
    store_codes: List[str]
    status: str = "approved"
    remarks: Optional[str] = None

class WithdrawnCommission(BaseModel):
    fiscal_month: str
    store_code: str
    status: str = "saved"

class StoreTypeUpdate(BaseModel):
    fiscal_month: str
    store_code: str
    store_type: str


class FiscalPeriodUpdate(BaseModel):
    fiscal_month: str
    store_code: str
    fiscal_period: List[str]
