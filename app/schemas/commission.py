from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class CommissionBase(BaseModel):
    name: str
    amount: float
    period: Optional[str] = None
    status: Optional[str] = "draft"
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
