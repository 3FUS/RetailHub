from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, DECIMAL
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class CommissionMainModel(Base):
    __tablename__ = "commissions_main"
    fiscal_month = Column(String(50), primary_key=True)
    month_end = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.now)
    created_by = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now)
    updated_by = Column(String(50))


class CommissionStoreModel(Base):
    __tablename__ = "commissions_store"

    store_code = Column(String(30), primary_key=True)
    fiscal_month = Column(String(50), primary_key=True)
    store_type = Column(String(50))
    fiscal_period = Column(String(50))
    status = Column(String(20), default="draft")  # saved, submitted, approved, rejected
    created_at = Column(DateTime, default=datetime.utcnow)
    creator_code = Column(String(30))
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    remarks = Column(Text)
    merged_store_codes = Column(String(255))
    merged_flag = Column(Boolean, default=False)
    opening_days = Column(Integer)
    version = Column(Integer, default=1)
    saved_by = Column(String(30))
    saved_at = Column(DateTime, default=datetime.utcnow)
    submit_by = Column(String(30))
    submit_at = Column(DateTime)
    approved_by = Column(String(30))  # 审核人
    approved_at = Column(DateTime)  # 审核时间
    rejected_by = Column(String(30))
    rejected_at = Column(DateTime)
    reject_remarks = Column(Text)


class CommissionStaffModel(Base):
    __tablename__ = "commissions_staff"

    fiscal_month = Column(String(50), primary_key=True)
    staff_code = Column(String(30), primary_key=True)  # 员工ID
    store_code = Column(String(30), primary_key=True)
    amount = Column(DECIMAL(12, 2))
    rule_detail_code = Column(String(60), primary_key=True)  # rule_detail_code
    remarks = Column(Text)
    total_days_store_work = Column(DECIMAL(12, 2))
    created_at = Column(DateTime, default=datetime.utcnow)
    creator_code = Column(String(30))
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class CommissionStaffDetailModel(Base):
    __tablename__ = "commissions_staff_detail"
    fiscal_month = Column(String(50), primary_key=True)
    store_code = Column(String(30), primary_key=True)
    staff_code = Column(String(30), primary_key=True)
    store_target_value = Column(DECIMAL(12, 2))
    store_sales_value = Column(DECIMAL(12, 2))
    store_achievement_rate = Column(DECIMAL(12, 2))
    staff_target_value = Column(DECIMAL(12, 2))
    staff_sales_value = Column(DECIMAL(12, 2))
    staff_achievement_rate = Column(DECIMAL(12, 2))
    expected_attendance = Column(DECIMAL(12, 2))
    actual_attendance = Column(DECIMAL(12, 2))
    position = Column(String(100))  # 职位
    salary_coefficient = Column(DECIMAL(12, 2))
    amount = Column(DECIMAL(12, 2))
    rule_code = Column(String(30), primary_key=True)
    rule_detail_code = Column(String(60), primary_key=True)
    remarks = Column(Text)
    total_days_store_work = Column(DECIMAL(12, 2))
    created_at = Column(DateTime, default=datetime.utcnow)
    creator_code = Column(String(30))


class CommissionRuleModel(Base):
    __tablename__ = "commissions_rule"

    rule_code = Column(String(30), primary_key=True)
    rule_name = Column(String(120))
    rule_type = Column(String(20), nullable=False)  # 'commission' 或 'incentive'
    # 基于什么计算业绩 - 个人业绩或店铺业绩
    rule_basis = Column(String(30), nullable=False)  # 'individual' (个人), 'store' (店铺)
    # 规则分类 - 团队、个人、人工调整、Incentive等
    rule_class = Column(String(30))  # 'individual', 'team', 'adjustment', 'incentive'
    minimum_guarantee = Column(DECIMAL(12, 2))  # 保底金额字段
    consider_attendance = Column(Integer, default=0)  # 是否考虑出勤比例
    minimum_guarantee_on_attendance = Column(DECIMAL(12, 2))
    attendance_calculation_logic = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    creator_code = Column(String(30))
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class CommissionRuleDetailModel(Base):
    __tablename__ = "commissions_rule_detail"

    rule_detail_code = Column(String(60), primary_key=True)
    rule_code = Column(String(30), nullable=False)  # 关联到 commissions_rule 的 rule_code
    start_value = Column(DECIMAL(12, 2))  # 区间起始值，例如 0.0, 80.0
    end_value = Column(DECIMAL(12, 2))  # 区间结束值，>=140% 时可为 None
    value = Column(DECIMAL(12, 2))  # 对应的数值，如 0.5%, 1500 RMB
    created_at = Column(DateTime, default=datetime.utcnow)
    creator_code = Column(String(30))
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class CommissionRuleAssignmentModel(Base):
    """
    规则分配模型
    用于定义特定店铺类型和岗位对应的佣金规则
    例如：POP-STORE 类型店铺的 SA 销售岗位使用 rule_code 为 R_01 的规则
    """
    __tablename__ = "commissions_rule_assignment"

    rule_code = Column(String(30), primary_key=True)  # 关联的规则代码
    store_type = Column(String(50), primary_key=True)  # 店铺类型，如 POP-STORE
    position = Column(String(60), primary_key=True)  # 岗位代码，如 SA
    created_at = Column(DateTime, default=datetime.utcnow)
    creator_code = Column(String(30))
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_active = Column(Boolean, default=True)  # 是否启用该规则分配

    # 可以添加唯一约束确保同一店铺类型和岗位不会重复分配规则
    # __table_args__ = (UniqueConstraint('store_type', 'position_code', name='uq_store_position_rule'),)
