from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Numeric
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class DimensionDayWeek(Base):
    __tablename__ = 'dimension_dayweek'

    finance_year = Column(Integer, nullable=False)
    fiscal_month = Column(String(10))
    actual_date = Column(DateTime, primary_key=True, nullable=False)
    actual_date_ly = Column(DateTime)
    week_number = Column(Integer, nullable=False)
    month_number = Column(Integer, nullable=False)
    quarter_number = Column(Integer, nullable=True, default=None)
    day_number = Column(Integer, nullable=False)
    vs_finance_year = Column(Integer, nullable=True, default=None)
    vs_day_number = Column(Integer, nullable=True, default=None)
    create_time = Column(DateTime, nullable=True, default=None, onupdate=datetime.now)
    create_user = Column(String(60), nullable=True, default=None)


class StoreModel(Base):
    """
    门店信息模型
    对应数据库中的store表
    """
    __tablename__ = "store"

    store_code = Column(String(30), primary_key=True)  # 门店代码
    store_name = Column(String(255), nullable=False)  # 门店名称
    store_nameCHS = Column(String(255))  # 中文门店名称
    country = Column(String(30))  # 国家
    state = Column(String(30))  # 省
    City = Column(String(50))  # 城市
    county = Column(String(30))
    Area = Column(String(30))  # 区域
    Address1 = Column(String(255))  # 地址1
    Address2 = Column(String(255))  # 地址2
    floor = Column(String(10))  # 楼层
    zip_code = Column(String(10))  # 邮政编码
    email = Column(String(60))  # 地址邮件
    telephone1 = Column(String(20))  # 电话1
    telephone2 = Column(String(20))  # 电话2
    LATITUDE = Column(Numeric(17, 6))  # 纬度
    LONGITUDE = Column(Numeric(17, 6))  # 经度
    open_date = Column(DateTime)  # 开店日期
    close_date = Column(DateTime)  # 关店日期
    org_level_code = Column(String(30))  # 组织层级
    org_level_value = Column(String(60))  # 组织代码
    store_type = Column(String(30))  # 店铺类型
    currency = Column(String(10))  # 货币类型
    open_id = Column(String(60))  # 门店微信OPENID
    union_id = Column(String(60))  # 门店UNIONID
    store_area = Column(Numeric(17, 3))  # 门店面积
    store_net_area = Column(Numeric(17, 3))  # 门店Net面积
    reference_code = Column(String(30))
    manage_region = Column(String(30))
    manage_channel = Column(String(30))
    manage_region_group = Column(String(30))
    manage_region_code = Column(String(30))
    manage_area = Column(String(30))
    brand = Column(String(30))
    inactive_flag = Column(Integer)
    create_user = Column(String(60))
    create_time = Column(DateTime)
    update_user = Column(String(60))
    update_time = Column(DateTime)
