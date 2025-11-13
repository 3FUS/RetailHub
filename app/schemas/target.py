from pydantic import BaseModel
from datetime import datetime, date
from typing import Optional, List
from pydantic import field_validator, model_validator
from decimal import Decimal


class TargetBase(BaseModel):
    name: str
    value: Decimal
    period: Optional[str] = None
    status: Optional[str] = "saved"
    remarks: Optional[str] = None


class TargetCreate(TargetBase):
    pass


class TargetUpdate(TargetBase):
    pass


# 新增TargetStore相关的schema
class TargetStoreBase(BaseModel):
    store_code: str
    fiscal_month: str
    target_value: Optional[Decimal] = None
    staff_status: Optional[str] = "saved"
    store_status: Optional[str] = "saved"
    creator_code: Optional[str] = None


class TargetStoreCreate(TargetStoreBase):
    pass


class TargetStoreUpdate(TargetStoreBase):
    pass


class TargetStoreWeekBase(BaseModel):
    week_number: int
    percentage: Optional[Decimal] = None


class TargetStoreWeekCreate(BaseModel):
    store_code: str
    fiscal_month: str
    weeks: list[TargetStoreWeekBase]
    creator_code: Optional[str] = None


class TargetStoreDailyBase(BaseModel):
    target_date: date
    percentage: Optional[Decimal] = None


class TargetStoreDailyCreate(BaseModel):
    store_code: str
    fiscal_month: str
    days: list[TargetStoreDailyBase]
    store_status: str = "saved"
    creator_code: Optional[str] = None


class StaffAttendanceBase(BaseModel):
    staff_code: str
    expected_attendance: Optional[Decimal] = None
    position: Optional[str] = None
    salary_coefficient: Optional[Decimal] = None
    deletable: Optional[bool] = False


class StaffAttendanceCreate(BaseModel):
    store_code: str
    fiscal_month: str
    staffs: List[StaffAttendanceBase]
    staff_status: str = "saved"
    creator_code: Optional[str] = None


class Staff_Actual_Attendance(BaseModel):
    staff_code: str
    actual_attendance: Optional[Decimal] = None
    deletable: Optional[bool] = False


class StaffAttendanceUpdate(BaseModel):
    store_code: str
    fiscal_month: str
    staff_actual_attendance: list[Staff_Actual_Attendance]
    staff_status: str = "saved"


class BatchApprovedTarget(BaseModel):
    fiscal_month: str
    store_codes: List[str]
    store_status: Optional[str] = None
    staff_status: Optional[str] = None
    remarks: Optional[str] = None


class WithdrawnTarget(BaseModel):
    fiscal_month: str
    store_code: str
    store_status: Optional[str] = None
    staff_status: Optional[str] = None
