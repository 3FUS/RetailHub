from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models.commission import CommissionStaffModel, CommissionStoreModel, CommissionRuleModel, \
    CommissionRuleAssignmentModel, CommissionRuleDetailModel
from app.models.dimension import StoreModel
from app.models.staff import StaffAttendanceModel
from app.schemas.commission import CommissionCreate, CommissionUpdate, CommissionStaffCreate
from app.models.target import TargetStoreMain

from app.database import get_db
from datetime import datetime
from sqlalchemy import func
from sqlalchemy.orm import aliased
from sqlalchemy import delete


class CommissionService:
    @staticmethod
    async def create_commission(db: AsyncSession, commission_data: CommissionCreate):
        commission = CommissionStaffModel(**commission_data.dict())
        db.add(commission)
        await db.commit()
        await db.refresh(commission)
        return commission

    @staticmethod
    async def update_commission(db: AsyncSession, attendance_update) -> bool:
        try:
            # 获取传入的参数
            store_code = attendance_update.store_code
            fiscal_month = attendance_update.fiscal_month
            staff_attendances = attendance_update.staff_actual_attendance

            updated_count = 0

            # 遍历所有需要更新的员工
            for staff_attendance in staff_attendances:
                staff_code = staff_attendance.staff_code
                actual_attendance = staff_attendance.actual_attendance

                # 查询现有的员工考勤记录
                result = await db.execute(
                    select(StaffAttendanceModel)
                        .where(StaffAttendanceModel.staff_code == staff_code)
                        .where(StaffAttendanceModel.store_code == store_code)
                        .where(StaffAttendanceModel.fiscal_month == fiscal_month)
                )

                staff_record = result.scalar_one_or_none()

                # 如果找到记录，则更新实际出勤字段
                if staff_record:
                    staff_record.actual_attendance = actual_attendance
                    staff_record.updated_at = datetime.utcnow()
                    updated_count += 1

            # 提交更改
            await db.commit()
            # 返回更新结果
            return True

        except Exception as e:
            # 发生异常时回滚事务
            await db.rollback()
            raise e

    @staticmethod
    async def get_all_commissions_by_key(fiscal_month: str, key_word: str, db: AsyncSession):
        store_subquery = (
            select(
                StoreModel.store_code,
                StoreModel.store_name,
                StoreModel.store_type
            )
                .where(StoreModel.manage_channel.in_(['ROCN', 'RFCN']))
                .subquery()
        )
        store_alias = aliased(StoreModel, store_subquery)

        # 使用ORM模型模式查询佣金数据，并与门店信息关联
        result = await db.execute(
            select(
                CommissionStoreModel.store_code,
                store_alias.store_name,
                CommissionStoreModel.store_type,
                CommissionStoreModel.fiscal_period,
                CommissionStoreModel.status,
                CommissionRuleModel.rule_class,
                func.sum(CommissionStaffModel.amount).label('amount')
            )
                .select_from(CommissionStoreModel)
                .join(CommissionStaffModel,
                      (CommissionStoreModel.fiscal_month == CommissionStaffModel.fiscal_month) &
                      (CommissionStoreModel.store_code == CommissionStaffModel.store_code),
                      isouter=True
                      )
                .join(CommissionRuleModel,
                      CommissionStaffModel.rule_code == CommissionRuleModel.rule_code,
                      isouter=True
                      )
                .join(store_alias,
                      CommissionStoreModel.store_code == store_alias.store_code,
                      isouter=True)  # 使用LEFT JOIN
                .where(CommissionStoreModel.fiscal_month == fiscal_month)
                .group_by(
                CommissionStoreModel.store_code,
                store_alias.store_name,
                CommissionStoreModel.fiscal_period,
                CommissionStoreModel.store_type,
                CommissionStoreModel.status,
                CommissionRuleModel.rule_class

            )
        )

        commissions = result.fetchall()

        commission_dict = {}
        for commission in commissions:
            store_code = commission.store_code

            if store_code not in commission_dict:
                commission_dict[store_code] = {
                    'store_code': store_code,
                    'store_name': commission.store_name if commission.store_name is not None else '',
                    'store_type': commission.store_type if commission.store_type is not None else '',
                    'fiscal_period': commission.fiscal_period if commission.fiscal_period is not None else '',
                    'status': commission.status if commission.status is not None else '',
                    'amount_individual': 0.0,
                    'amount_team': 0.0,
                    'amount_adjustment': 0.0
                }

            rule_class = commission.rule_class
            amount = float(commission.amount) if commission.amount is not None else 0.0

            if rule_class == 'individual':
                commission_dict[store_code]['amount_individual'] = amount
            elif rule_class == 'team':
                commission_dict[store_code]['amount_team'] = amount
            elif rule_class == 'adjustment':
                commission_dict[store_code]['amount_adjustment'] = amount

        # 转换为列表格式返回
        formatted_commissions = list(commission_dict.values())

        return formatted_commissions

    @staticmethod
    async def get_commission_by_store_code(db: AsyncSession, id: int):
        pass

    @staticmethod
    async def get_commission_by_staff_code(db: AsyncSession, staff_code: str, store_code: str, fiscal_month: str):
        result = await db.execute(
            select(
                CommissionRuleModel.rule_name,
                CommissionRuleModel.rule_class,
                CommissionRuleModel.rule_type,
                CommissionRuleModel.rule_basis,
                CommissionRuleDetailModel.start_value,
                CommissionRuleDetailModel.end_value,
                CommissionRuleDetailModel.value,
                CommissionStaffModel.amount
            )
                .select_from(CommissionStaffModel)
                .join(CommissionRuleDetailModel,
                      CommissionStaffModel.rule_detail_code == CommissionRuleDetailModel.rule_detail_code)
                .join(CommissionRuleModel,
                      CommissionRuleDetailModel.rule_code == CommissionRuleModel.rule_code)
                .where(CommissionStaffModel.staff_code == staff_code)
                .where(CommissionStaffModel.store_code == store_code)
                .where(CommissionStaffModel.fiscal_month == fiscal_month)
        )
        commissions = result.fetchall()
        formatted_commissions = []
        for commission in commissions:
            # 格式化公式显示
            if commission.rule_type == 'commission':
                # 对于佣金类型，显示为百分比形式
                formula = f" * {commission.value}%"
            elif commission.rule_type == 'incentive':
                # 对于激励类型，显示为固定金额
                formula = f"=> ¥{commission.value}"
            else:
                # 默认显示
                formula = f"{commission.value}"

            # 添加区间信息到公式中
            if commission.start_value is not None:
                if commission.end_value is not None:
                    formula = f"≥ {commission.start_value}% < {commission.end_value}%  {formula}"
                else:
                    formula = f"≥ {commission.start_value}%  {formula}"
            elif commission.end_value is not None:
                formula = f"< {commission.end_value}%  {formula}"

            formatted_commissions.append({
                "rule_name": commission.rule_name,
                "rule_class": commission.rule_class,
                "rule_type": commission.rule_type,
                "rule_basis": commission.rule_basis,
                "formula": formula,
                "amount": float(commission.amount) if commission.amount is not None else 0.0
            })

        return formatted_commissions

    @staticmethod
    async def create_add_adjustment(db: AsyncSession, adjustment: CommissionStaffCreate):
        adjustment_commission = CommissionStaffModel(
            fiscal_month=adjustment.fiscal_month,
            staff_code=adjustment.staff_code,
            store_code=adjustment.store_code,
            amount=adjustment.amount,
            rule_code="adjustment"  # 设置为调整类型
        )

        db.add(adjustment_commission)
        await db.commit()
        await db.refresh(adjustment_commission)

        return adjustment_commission

    @staticmethod
    async def audit_commission(db: AsyncSession, id: int) -> dict:
        result = await db.execute(select(CommissionStoreModel).where(CommissionStoreModel.id == id))
        commission = result.scalar_one_or_none()

        if not commission:
            raise ValueError("Commission not found")

        commission.status = "approved"
        commission.approver_id = 1  # Assuming current user ID is 1 for demo
        commission.updated_at = datetime.utcnow()
        await db.commit()
        return {"message": "Commission approved successfully"}

    @staticmethod
    async def calculate_commissions_for_store(db: AsyncSession, store_code: str, fiscal_month: str):
        """
        为指定店铺计算员工佣金，支持一个岗位对应多个规则的情况

        Args:
            db: 数据库会话
            store_code: 店铺代码
            fiscal_month: 财月

        Returns:
            bool: 计算是否成功
        """
        try:
            # 1. 获取店铺类型和数据
            store_result = await db.execute(
                select(
                    TargetStoreMain.store_type,
                    TargetStoreMain.target_value,
                    TargetStoreMain.sales_value,
                ).where(
                    TargetStoreMain.store_code == store_code,
                    TargetStoreMain.fiscal_month == fiscal_month
                )
            )
            store_data = store_result.fetchone()

            if not store_data:
                raise ValueError(f"Store {store_code} not found or has no data for {fiscal_month}")

            store_type = store_data.store_type
            store_target_value = store_data.target_value
            store_sales_value = store_data.sales_value

            if not store_type:
                raise ValueError(f"Store {store_code} has no store_type")

            # 计算店铺达成率
            store_achievement_rate = (
                store_sales_value / store_target_value * 100
                if store_target_value and store_target_value > 0
                else 0
            )

            # 2. 获取该店铺所有员工的考勤和岗位信息
            staff_attendances_result = await db.execute(
                select(
                    StaffAttendanceModel.staff_code,
                    StaffAttendanceModel.position,
                    StaffAttendanceModel.actual_attendance,
                    StaffAttendanceModel.expected_attendance,
                    StaffAttendanceModel.salary_coefficient,
                    StaffAttendanceModel.target_value,
                    StaffAttendanceModel.sales_value,
                ).where(
                    StaffAttendanceModel.store_code == store_code,
                    StaffAttendanceModel.fiscal_month == fiscal_month
                )
            )
            staff_attendances = staff_attendances_result.all()

            # 3. 删除该店铺该财月的所有现有佣金记录
            await db.execute(
                delete(CommissionStaffModel)
                    .where(
                    CommissionStaffModel.fiscal_month == fiscal_month,
                    CommissionStaffModel.store_code == store_code
                )
            )

            # 4. 如果没有员工数据，直接提交事务并返回
            if not staff_attendances:
                await db.commit()
                return True

            # 5. 收集所有需要的规则代码（一个岗位可能对应多个规则）
            positions = list(set(staff.position for staff in staff_attendances))
            rule_assignment_result = await db.execute(
                select(
                    CommissionRuleAssignmentModel.position,
                    CommissionRuleAssignmentModel.rule_code
                ).where(
                    CommissionRuleAssignmentModel.store_type == store_type,
                    CommissionRuleAssignmentModel.position.in_(positions),
                    CommissionRuleAssignmentModel.is_active == True
                )
            )

            # 构建岗位到规则代码的映射（一个岗位可能有多个规则）
            position_to_rules = {}
            for row in rule_assignment_result.fetchall():
                if row.position not in position_to_rules:
                    position_to_rules[row.position] = []
                position_to_rules[row.position].append(row.rule_code)

            # 6. 获取所有需要用到的规则信息
            all_rule_codes = []
            for rule_codes in position_to_rules.values():
                all_rule_codes.extend(rule_codes)

            all_rule_codes = list(set(all_rule_codes))  # 去重

            if not all_rule_codes:
                await db.commit()
                return True

            rules_result = await db.execute(
                select(
                    CommissionRuleModel.rule_code,
                    CommissionRuleModel.rule_basis,
                    CommissionRuleModel.rule_class,
                    CommissionRuleModel.rule_type,
                    CommissionRuleModel.minimum_guarantee,
                    CommissionRuleModel.consider_attendance
                ).where(
                    CommissionRuleModel.rule_code.in_(all_rule_codes)
                )
            )
            rules_info = {
                row.rule_code: row
                for row in rules_result.fetchall()
            }

            commission_records = []

            # 7. 为每个员工计算佣金（应用所有适用的规则）
            for staff in staff_attendances:
                # 检查员工目标值
                if not staff.target_value or staff.target_value == 0:
                    continue

                # 计算员工达成率
                staff_achievement_rate = staff.sales_value / staff.target_value * 100

                # 获取适用于该岗位的所有规则代码
                rule_codes = position_to_rules.get(staff.position, [])
                if not rule_codes:
                    continue

                # 为每个适用的规则计算佣金
                for rule_code in rule_codes:
                    # 获取规则信息
                    rule_info = rules_info.get(rule_code)
                    if not rule_info:
                        continue

                    # 根据 rule_basis 选择合适的达成率和销售额
                    if rule_info.rule_basis == 'individual':
                        target_achievement_rate = staff_achievement_rate
                        sales_value = staff.sales_value
                    elif rule_info.rule_basis == 'store':
                        target_achievement_rate = store_achievement_rate
                        sales_value = store_sales_value
                    else:
                        continue

                    # 获取匹配的规则详情
                    rule_detail_result = await db.execute(
                        select(
                            CommissionRuleDetailModel.rule_detail_code,
                            CommissionRuleDetailModel.value
                        ).where(
                            CommissionRuleDetailModel.rule_code == rule_code,
                            CommissionRuleDetailModel.start_value <= target_achievement_rate,
                            (
                                    CommissionRuleDetailModel.end_value.is_(None) |
                                    (CommissionRuleDetailModel.end_value > target_achievement_rate)
                            )
                        )
                    )
                    matching_detail = rule_detail_result.fetchone()

                    if not matching_detail:
                        continue

                    # 计算佣金
                    commission_amount = 0.0
                    if rule_info.rule_type == 'commission':
                        commission_amount = sales_value * (matching_detail.value / 100)
                    elif rule_info.rule_type == 'incentive':
                        commission_amount = matching_detail.value

                    if rule_info.consider_attendance:
                        expected_attendance = staff.expected_attendance or 0
                        actual_attendance = staff.actual_attendance or 0

                        if expected_attendance > 0:  # 避免除零错误
                            commission_amount = commission_amount / expected_attendance * actual_attendance
                        # else:
                        #     commission_amount = 0

                    if rule_info.minimum_guarantee and commission_amount < rule_info.minimum_guarantee:
                        commission_amount = rule_info.minimum_guarantee

                    # 只有佣金金额大于0时才保存
                    if commission_amount and commission_amount > 0:
                        commission_record = CommissionStaffModel(
                            fiscal_month=fiscal_month,
                            staff_code=staff.staff_code,
                            store_code=store_code,
                            amount=commission_amount,
                            rule_detail_code=matching_detail.rule_detail_code
                        )
                        commission_records.append(commission_record)

            # 8. 批量添加佣金记录
            if commission_records:
                db.add_all(commission_records)

            # 9. 提交事务
            await db.commit()
            return True

        except Exception as e:
            await db.rollback()
            raise e
