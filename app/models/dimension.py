from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Numeric, Text, Date
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class DimensionDayWeek(Base):
    __tablename__ = 'dimension_dayweek'

    finance_year = Column(Integer, nullable=False)
    fiscal_month = Column(String(10))
    actual_date = Column(Date, primary_key=True, nullable=False)
    actual_date_ly = Column(Date)
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

class RoleOrgJoin(Base):
    """
    角色与组织关联模型
    对应数据库中的 role_org_join 表
    """
    __tablename__ = "role_org_join"

    role_code = Column(String(60),  primary_key=True)
    org_level = Column(String(60), primary_key=True)
    org_level_value = Column(String(60), primary_key=True)
    update_time = Column(DateTime, nullable=True, default=None)
    update_user = Column(String(120), nullable=True, default=None)
    TS_ID = Column(DateTime, nullable=True, default=datetime.utcnow)

class StoreTypeModel(Base):
    """
    门店类型模型
    对应数据库中的store_type表
    """
    __tablename__ = "store_type"
    store_type_code = Column(String(30), primary_key=True)  # 门店类型代码
    store_type_name = Column(String(120))  # 门店类型名称
    store_type_group = Column(String(30))  # 门店类型组
    is_active = Column(Boolean, default=True)  # 是否启用该规则分配
    created_at = Column(DateTime, default=datetime.utcnow)
    creator_code = Column(String(30))
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class SysMenu(Base):
    """
    系统菜单模型
    对应数据库中的sys_menu表
    """
    __tablename__ = "sys_menu"

    id = Column(String(60), primary_key=True)  # 菜单ID
    create_time = Column(DateTime)  # 创建时间
    create_user = Column(String(60))  # 创建人
    deleted = Column(String(1), default='0')  # 是否删除（0:未删除，1:已删除）
    update_time = Column(DateTime)  # 更新时间
    update_user = Column(String(60))  # 更新人
    description = Column(Text)  # 描述
    icon = Column(String(1024))  # 图标
    is_home_display = Column(String(1), default='0')  # 是否在首页显示（0:否，1:是）
    is_outer_chain = Column(String(1), default='1')  # 是否为外链（0:否，1:是）
    menu_name = Column(Text)  # 菜单名称
    menu_url = Column(String(255))  # 菜单URL
    parent_id = Column(String(60))  # 父级菜单ID
    sort = Column(Integer)  # 排序
    type = Column(String(32))  # 类型