from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List
from pydantic import field_validator, model_validator


class TargetBase(BaseModel):
    name: str
    value: float
    period: Optional[str] = None
    status: Optional[str] = "draft"
    remarks: Optional[str] = None


class TargetCreate(TargetBase):
    pass


class TargetUpdate(TargetBase):
    pass


# 新增TargetStore相关的schema
class TargetStoreBase(BaseModel):
    store_code: str
    fiscal_month: str
    target_value: Optional[float] = None
    staff_status: Optional[str] = "draft"
    store_status: Optional[str] = "draft"
    creator_code: Optional[str] = None


class TargetStoreCreate(TargetStoreBase):
    pass


class TargetStoreUpdate(TargetStoreBase):
    pass


class TargetStoreWeekBase(BaseModel):
    week_number: int
    percentage: float

    # @field_validator('week_number')
    # @classmethod
    # def week_number_must_be_between_1_and_5(cls, v: int) -> int:
    #     if v not in [1, 2, 3, 4, 5]:
    #         raise ValueError('week_number must be one of 1, 2, 3, 4, 5')
    #     return v


class TargetStoreWeekCreate(BaseModel):
    store_code: str
    fiscal_month: str
    weeks: list[TargetStoreWeekBase]
    creator_code: Optional[str] = None


class TargetStoreDailyBase(BaseModel):
    target_date: datetime
    percentage: float


class TargetStoreDailyCreate(BaseModel):
    store_code: str
    fiscal_month: str
    days: list[TargetStoreDailyBase]
    store_status: str = "draft"
    creator_code: Optional[str] = None


class StaffAttendanceBase(BaseModel):
    staff_code: str
    expected_attendance: float
    position: str
    salary_coefficient: float


class StaffAttendanceCreate(BaseModel):
    store_code: str
    fiscal_month: str
    staffs: List[StaffAttendanceBase]
    deletable: bool = False
    staff_status: str = "draft"
    creator_code: Optional[str] = None

class Staff_Actual_Attendance(BaseModel):
    staff_code: str
    actual_attendance: float

class StaffAttendanceUpdate(BaseModel):
    store_code: str
    fiscal_month: str
    staff_actual_attendance: list[Staff_Actual_Attendance]
