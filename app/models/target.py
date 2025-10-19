from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, Date
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class TargetStoreMain(Base):
    """
    门店目标类
    需要门店代码，财月，目标数值，数据状态
    """
    __tablename__ = "target_stores_main"

    store_code = Column(String(30), primary_key=True)
    fiscal_month = Column(String(50), primary_key=True)
    store_type = Column(String(50))
    target_value = Column(Float)
    sales_value = Column(Float)
    sales_value_ec = Column(Float)
    sales_value_store = Column(Float)  # 线下门店销售金额
    store_status = Column(String(20), default="draft")  # draft, submitted, approved, rejected
    staff_status = Column(String(20), default="draft")
    created_at = Column(DateTime, default=datetime.utcnow)
    creator_code = Column(String(30))
    updated_by = Column(String(30))  # 修改人
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    store_saved_by = Column(String(30))
    store_saved_at = Column(DateTime, default=datetime.utcnow)
    store_submit_by = Column(String(30))
    store_submit_at = Column(DateTime)
    store_approved_by = Column(String(30))  # 审核人
    store_approved_at = Column(DateTime)  # 审核时间
    store_rejected_by = Column(String(30))
    store_rejected_at = Column(DateTime)
    store_reject_remarks = Column(Text)
    staff_saved_by = Column(String(30))
    staff_saved_at = Column(DateTime, default=datetime.utcnow)
    staff_submit_by = Column(String(30))
    staff_submit_at = Column(DateTime)
    staff_approved_by = Column(String(30))  # 审核人
    staff_approved_at = Column(DateTime)  # 审核时间
    staff_rejected_by = Column(String(30))
    staff_rejected_at = Column(DateTime)
    staff_reject_remarks = Column(Text)


class TargetStoreWeek(Base):
    """
    门店周目标类
    需要门店代码，财月，第几周，每周目标占比，每周目标数值
    """
    __tablename__ = "target_store_weeks"

    store_code = Column(String(30), primary_key=True)
    fiscal_month = Column(String(50), primary_key=True)
    week_number = Column(Integer, primary_key=True)
    percentage = Column(Float, nullable=False)  # 每周目标占比
    target_value = Column(Float)  # 每周目标数值
    sales_value_ly = Column(Float)
    sales_value_ly_percentage = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)
    creator_code = Column(String(30))
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class TargetStoreDaily(Base):
    """
    门店日目标类
    需要门店代码，财月，日期，每日目标占比，每日目标数值
    """
    __tablename__ = "target_store_daily"

    store_code = Column(String(50), primary_key=True)
    fiscal_month = Column(String(50), nullable=False)
    target_date = Column(Date, primary_key=True)  # 日期
    percentage = Column(Float, nullable=False)  # 每日目标占比
    monthly_percentage = Column(Float)
    target_value = Column(Float)  # 每日目标数值
    sales_value_ly = Column(Float)
    sales_value_ly_percentage = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)
    creator_code = Column(String(30))
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
