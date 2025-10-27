from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class StaffAttendanceModel(Base):
    """
    员工考勤和薪资模型
    用于保存员工每个财月的应出勤、实际出勤、职位和工资系数、财务目标
    """
    __tablename__ = "target_staff_attendances"

    staff_code = Column(String(30), primary_key=True)  # 员工ID
    store_code = Column(String(30), primary_key=True)
    fiscal_month = Column(String(50), primary_key=True)  # 财月
    expected_attendance = Column(Float)  # 应出勤天数
    actual_attendance = Column(Float)  # 实际出勤天数
    position = Column(String(100), nullable=False)  # 职位
    salary_coefficient = Column(Float, nullable=False)  # 目标系数
    target_value_ratio = Column(Float)
    target_value = Column(Float)  # 个人销售目标
    sales_value = Column(Float)  # 个人总销金额 线下+线上
    sales_value_ec = Column(Float)  # 线上销售金额
    sales_value_store = Column(Float)  # 线下门店销售金额
    del_flag = Column(Boolean, default=False, nullable=False)
    deletable = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    creator_code = Column(String(30))
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class StaffModel(Base):
    """
    员工信息模型
    对应数据库中的staff表
    """
    __tablename__ = "staff"

    staff_key = Column(String(60), primary_key=True)  # 员工ID
    staff_code = Column(String(30))  # 员工代码
    first_name = Column(String(80))  # 员工姓名
    last_name = Column(String(80))
    join_date = Column(DateTime)  # 入职日期
    position_code = Column(String(60))  # 职位代码
    position = Column(String(60))
    salary_coefficient = Column(Float)
    gender = Column(String(10))  # 性别
    birth_date = Column(DateTime)  # 生日
    suffix = Column(String(10))
    mobile = Column(String(20))  # 手机
    telephone = Column(String(20))  # 电话
    email = Column(String(60))  # 电子邮箱
    avatar = Column(String(255))  # 头像
    open_id = Column(String(60))  # 微信OPENID
    union_id = Column(String(60))  # 微信UNIONID
    state = Column(String(10))  # 状态 1 在职 0 离职
    store_code = Column(String(30))  # 所在组织
    del_flag = Column(Boolean, default=False, nullable=False)
    password = Column(String(100))  # 密码
    WORKDAY_ID = Column(String(100))
    CRM_ID = Column(String(100))
    create_user = Column(String(60))
    create_time = Column(DateTime)
    update_user = Column(String(60))
    update_time = Column(DateTime)


class PositionModel(Base):
    """
    岗位信息模型
    用于存储岗位代码、描述和默认计算系数
    """
    __tablename__ = "positions"

    position = Column(String(30), primary_key=True)  # 岗位代码
    position_code = Column(String(60), primary_key=True)
    description = Column(String(255))  # 岗位描述
    default_coefficient = Column(Float, default=1.0)  # 默认计算系数
    is_active = Column(Boolean, default=True)  # 是否启用该规则分配
    # allocation_type = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
#
# class StoreTypeModel(Base):
#     """
#     门店类型模型
#     对应数据库中的store_type表
#     """
#     __tablename__ = "store_type"
#     store_type_code = Column(String(30), primary_key=True)  # 门店类型代码
#     store_type_name = Column(String(120))  # 门店类型名称
#     store_type_group = Column(String(30))  # 门店类型组
#     is_active = Column(Boolean, default=True)  # 是否启用该规则分配
#     created_at = Column(DateTime, default=datetime.utcnow)
#     creator_code = Column(String(30))
#     updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
