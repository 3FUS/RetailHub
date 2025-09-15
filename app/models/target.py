from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class TargetStoreMain(Base):
    """
    门店目标类
    需要门店代码，财月，目标数值，数据状态
    """
    __tablename__ = "target_stores_main"

    store_code = Column(String(30),  primary_key=True)
    fiscal_month = Column(String(50), primary_key=True)
    store_type = Column(String(50))
    target_value = Column(Float)
    sales_value = Column(Float)
    store_status = Column(String(20), default="draft")  # draft, submitted, approved, rejected
    staff_status = Column(String(20), default="draft")
    created_at = Column(DateTime, default=datetime.utcnow)
    creator_code = Column(String(30))
    updated_by = Column(String(30))  # 修改人
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    approved_by = Column(String(30))  # 审核人
    approved_at = Column(DateTime)  # 审核时间


class TargetStoreWeek(Base):
    """
    门店周目标类
    需要门店代码，财月，第几周，每周目标占比，每周目标数值
    """
    __tablename__ = "target_store_weeks"

    store_code = Column(String(30),  primary_key=True)
    fiscal_month = Column(String(50),  primary_key=True)
    week_number = Column(Integer,  primary_key=True)
    percentage = Column(Float, nullable=False)  # 每周目标占比
    target_value = Column(Float)  # 每周目标数值
    created_at = Column(DateTime, default=datetime.utcnow)
    creator_code = Column(String(30))
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class TargetStoreDaily(Base):
    """
    门店日目标类
    需要门店代码，财月，日期，每日目标占比，每日目标数值
    """
    __tablename__ = "target_store_daily"

    store_code = Column(String(50),  primary_key=True)
    fiscal_month = Column(String(50), nullable=False)
    target_date = Column(DateTime,  primary_key=True)  # 日期
    percentage = Column(Float, nullable=False)  # 每日目标占比
    target_value = Column(Float)  # 每日目标数值
    created_at = Column(DateTime, default=datetime.utcnow)
    creator_code = Column(String(30))
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
