from typing import List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models.commission import CommissionStaffModel, CommissionStoreModel, CommissionRuleModel, \
    CommissionRuleAssignmentModel, CommissionRuleDetailModel, CommissionMainModel
from app.models.dimension import StoreModel, DimensionDayWeek, RoleOrgJoin
from app.models.staff import StaffAttendanceModel, StaffModel
from app.schemas.commission import CommissionCreate, CommissionUpdate, CommissionStaffCreate, BatchApprovedCommission
from app.models.target import TargetStoreMain

from datetime import datetime
from sqlalchemy import func, or_, exists
from sqlalchemy.orm import aliased
from sqlalchemy import delete
from app.utils.permissions import build_store_permission_query
from app.utils.logger import app_logger


class CommissionRPTService:

    @staticmethod
    async def get_rpt_commission_by_store(db: AsyncSession, fiscal_month: str, key_word: str = None):
        try:
            # 构建查询，包含所有需要的字段
            query = (
                select(
                    CommissionStoreModel.store_code,
                    StoreModel.store_name,
                    CommissionStoreModel.store_type,
                    StaffAttendanceModel.staff_code,
                    StaffModel.first_name,
                    StaffAttendanceModel.position,
                    StaffAttendanceModel.salary_coefficient,
                    CommissionStoreModel.fiscal_period,
                    CommissionStoreModel.status,
                    StaffAttendanceModel.sales_value,
                    (TargetStoreMain.target_value * StaffAttendanceModel.target_value_ratio).label('target_value'),
                    StaffAttendanceModel.expected_attendance,
                    StaffAttendanceModel.actual_attendance,
                    CommissionRuleModel.rule_class,
                    func.sum(CommissionStaffModel.amount).label('amount')
                )
                    .select_from(CommissionStoreModel)
                    .join(StoreModel,
                          CommissionStoreModel.store_code == StoreModel.store_code)
                    .join(StaffAttendanceModel,
                          (CommissionStoreModel.fiscal_month == StaffAttendanceModel.fiscal_month) &
                          (CommissionStoreModel.store_code == StaffAttendanceModel.store_code),
                          isouter=True)
                    .join(StaffModel,
                          StaffAttendanceModel.staff_code == StaffModel.staff_code,
                          isouter=True)
                    .join(TargetStoreMain,
                          (CommissionStoreModel.fiscal_month == TargetStoreMain.fiscal_month) &
                          (CommissionStoreModel.store_code == TargetStoreMain.store_code),
                          isouter=True)
                    .join(CommissionStaffModel,
                          (CommissionStoreModel.fiscal_month == CommissionStaffModel.fiscal_month) &
                          (CommissionStoreModel.store_code == CommissionStaffModel.store_code) &
                          (StaffAttendanceModel.staff_code == CommissionStaffModel.staff_code),
                          isouter=True)
                    .join(CommissionRuleDetailModel,
                          CommissionStaffModel.rule_detail_code == CommissionRuleDetailModel.rule_detail_code,
                          isouter=True)
                    .join(CommissionRuleModel,
                          CommissionRuleModel.rule_code == CommissionRuleDetailModel.rule_code,
                          isouter=True)
                    .where(CommissionStoreModel.fiscal_month == fiscal_month)
                    .where(StoreModel.manage_channel.in_(['ROCN', 'RFCN']))
            )

            # 如果提供了关键词，则添加过滤条件
            if key_word:
                query = query.where(
                    or_(
                        CommissionStoreModel.store_code.contains(key_word),
                        StoreModel.store_name.contains(key_word),
                        StaffAttendanceModel.staff_code.contains(key_word),
                        StaffAttendanceModel.first_name.contains(key_word)
                    )
                )

            # 按所有非聚合字段分组
            query = query.group_by(
                CommissionStoreModel.store_code,
                StoreModel.store_name,
                CommissionStoreModel.store_type,
                StaffAttendanceModel.staff_code,
                StaffModel.first_name,
                StaffAttendanceModel.position,
                StaffAttendanceModel.salary_coefficient,
                CommissionStoreModel.fiscal_period,
                CommissionStoreModel.status,
                StaffAttendanceModel.sales_value,
                TargetStoreMain.target_value,
                StaffAttendanceModel.target_value_ratio,
                StaffAttendanceModel.expected_attendance,
                StaffAttendanceModel.actual_attendance,
                CommissionRuleModel.rule_class
            )

            result = await db.execute(query)
            commissions = result.fetchall()

            # 使用 defaultdict 简化数据处理逻辑
            from collections import defaultdict

            commission_dict = defaultdict(lambda: {
                'store_code': '',
                'store_name': '',
                'store_type': '',
                'staff_code': '',
                'first_name': '',
                'position': '',
                'salary_coefficient': 0.0,
                'fiscal_period': '',
                'status': '',
                'sales_value': 0.0,
                'target_value': 0.0,
                'expected_attendance': 0.0,
                'actual_attendance': 0.0,
                'achievement_rate': 0.0,  # 计算达成率
                'attendance_rate': 0.0,  # 考勤率
                'amount_individual': 0.0,
                'amount_team': 0.0,
                'amount_incentive': 0.0,
                'amount_adjustment': 0.0
            })

            fiscal_periods = set(commission.fiscal_period for commission in commissions if commission.fiscal_period)

            # 为每个 fiscal_period 获取日期范围
            date_ranges = {}
            for fp in fiscal_periods:
                if fp:
                    # 分割可能包含多个财月的 fiscal_period
                    months = [month.strip() for month in fp.split(',')]

                    # 查询日期范围
                    date_query = select(
                        func.min(DimensionDayWeek.actual_date).label('min_date'),
                        func.max(DimensionDayWeek.actual_date).label('max_date')
                    ).where(
                        DimensionDayWeek.fiscal_month.in_(months)
                    )

                    date_result = await db.execute(date_query)
                    date_row = date_result.fetchone()

                    if date_row and date_row.min_date and date_row.max_date:
                        date_ranges[
                            fp] = f"{date_row.min_date.strftime('%Y-%m-%d')} to {date_row.max_date.strftime('%Y-%m-%d')}"
                    else:
                        date_ranges[fp] = "N/A"

            for commission in commissions:
                store_code = commission.store_code
                staff_code = commission.staff_code or ''

                # 创建唯一键，包含 store_code 和 staff_code
                key = f"{store_code}_{staff_code}" if staff_code else store_code

                # 初始化店铺和员工信息
                if commission_dict[key]['store_code'] == '':
                    # 计算达成率
                    sales_value = float(commission.sales_value) if commission.sales_value is not None else 0.0
                    target_value = float(commission.target_value) if commission.target_value is not None else 0.0
                    achievement_rate = 0.0
                    if target_value > 0:
                        achievement_rate = (sales_value / target_value) * 100

                    # 计算考勤率
                    expected_attendance = float(
                        commission.expected_attendance) if commission.expected_attendance is not None else 0.0
                    actual_attendance = float(
                        commission.actual_attendance) if commission.actual_attendance is not None else 0.0
                    attendance_rate = 0.0
                    if expected_attendance > 0:
                        attendance_rate = (actual_attendance / expected_attendance) * 100

                    commission_dict[key].update({
                        'store_code': store_code or '',
                        'store_name': commission.store_name or '',
                        'store_type': commission.store_type or '',
                        'staff_code': staff_code,
                        'first_name': commission.first_name or '',
                        'position': commission.position or '',
                        'salary_coefficient': float(
                            commission.salary_coefficient) if commission.salary_coefficient is not None else 0.0,
                        'fiscal_period': date_ranges.get(commission.fiscal_period, ''),
                        'status': commission.status or '',
                        'sales_value': sales_value,
                        'target_value': target_value,
                        'expected_attendance': expected_attendance,
                        'actual_attendance': actual_attendance,
                        'achievement_rate': round(achievement_rate, 2),
                        'attendance_rate': round(attendance_rate, 2)
                    })

                # 根据规则类别累加金额
                rule_class = commission.rule_class
                amount = float(commission.amount) if commission.amount is not None else 0.0

                if rule_class == 'individual':
                    commission_dict[key]['amount_individual'] += amount
                elif rule_class == 'team':
                    commission_dict[key]['amount_team'] += amount
                elif rule_class == 'incentive':
                    commission_dict[key]['amount_incentive'] += amount
                elif rule_class == 'adjustment':
                    commission_dict[key]['amount_adjustment'] += amount

            # 转换为列表格式返回
            formatted_commissions = list(commission_dict.values())

            status_count_query = (
                select(
                    CommissionStoreModel.status,
                    func.count(CommissionStoreModel.store_code).label('count')
                )
                    .where(CommissionStoreModel.fiscal_month == fiscal_month)
                    .group_by(CommissionStoreModel.status)
            )
            status_count_result = await db.execute(status_count_query)
            status_counts = status_count_result.fetchall()

            # 转换为字典格式
            status_count_dict = {row.status: row.count for row in status_counts}

            field_translations = {
                "store_code": {"en": "Store Code", "zh": "店铺代码"},
                "store_name": {"en": "Store Name", "zh": "店铺名称"},
                "store_type": {"en": "Store Type", "zh": "店铺类型"},
                "staff_code": {"en": "Staff Code", "zh": "员工代码"},
                "first_name": {"en": "Staff Name", "zh": "员工姓名"},
                "position": {"en": "Position", "zh": "职位"},
                "salary_coefficient": {"en": "Salary Coefficient", "zh": "薪资系数"},
                "fiscal_period": {"en": "Fiscal Period", "zh": "计算周期"},
                "status": {"en": "Status", "zh": "状态"},
                "sales_value": {"en": "Sales Value", "zh": "销售额"},
                "target_value": {"en": "Target Value", "zh": "目标值"},
                "expected_attendance": {"en": "Expected Attendance", "zh": "应出勤"},
                "actual_attendance": {"en": "Actual Attendance", "zh": "实际出勤"},
                "achievement_rate": {"en": "Achievement Rate (%)", "zh": "达成率 (%)"},
                "attendance_rate": {"en": "Attendance Rate (%)", "zh": "出勤率 (%)"},
                "amount_individual": {"en": "Individual Amount", "zh": "个人提成"},
                "amount_team": {"en": "Team Amount", "zh": "团队提成"},
                "amount_incentive": {"en": "Incentive Amount", "zh": "激励金额"},
                "amount_adjustment": {"en": "Adjustment Amount", "zh": "调整金额"}
            }

            return {"data": formatted_commissions, "status_counts": status_count_dict,
                    "field_translations": field_translations}

        except Exception as e:
            # 记录异常信息（在实际应用中可以使用日志记录器）
            print(f"Error in get_rpt_commission_by_store: {str(e)}")
            # 可以根据需要重新抛出异常或返回默认值
            raise e


class CommissionService:
    @staticmethod
    async def create_commission(db: AsyncSession, fiscal_month: str, store_codes: list):
        created_commissions = []
        app_logger.info(f"Creating commission for fiscal month: {fiscal_month}")
        for store_code in store_codes:
            result = await db.execute(
                select(CommissionStoreModel)
                    .where(CommissionStoreModel.fiscal_month == fiscal_month)
                    .where(CommissionStoreModel.store_code == store_code)
            )
            existing_commission = result.scalar_one_or_none()

            if existing_commission:
                # 如果记录已存在，添加到结果列表
                created_commissions.append(existing_commission)
            else:
                # 如果记录不存在，则创建新记录
                commission = CommissionStoreModel(
                    fiscal_month=fiscal_month,
                    store_code=store_code,
                    fiscal_period=fiscal_month,
                    status=None,  # 示例默认值
                    created_at=datetime.now()
                )
                db.add(commission)
                created_commissions.append(commission)

        await db.commit()

        # 刷新所有创建的对象以获取数据库生成的值
        for commission in created_commissions:
            await db.refresh(commission)

        app_logger.info(f"Created {len(created_commissions)} commission records")
        return created_commissions

    @staticmethod
    async def get_store_performance(db: AsyncSession, store_code: str, fiscal_month: str):

        # 获取店铺目标数据
        target_result = await db.execute(
            select(
                TargetStoreMain.target_value,
                TargetStoreMain.sales_value
            )
                .where(TargetStoreMain.store_code == store_code)
                .where(TargetStoreMain.fiscal_month == fiscal_month)
        )
        target_data = target_result.fetchone()

        if not target_data:
            raise ValueError(f"No target data found for store {store_code} in fiscal month {fiscal_month}")

        target_value = target_data.target_value or 0
        sales_value = target_data.sales_value or 0

        # 计算达成率
        achievement_rate = 0.0
        if target_value > 0:
            achievement_rate = (sales_value / target_value) * 100

        return {
            "store_code": store_code,
            "fiscal_month": fiscal_month,
            "target_value": target_value,
            "sales_value": sales_value,
            "achievement_rate": round(achievement_rate, 2)
        }

    @staticmethod
    async def update_commission(db: AsyncSession, attendance_update,role_code: str) -> bool:
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
                    staff_record.updated_at = datetime.now()
                    updated_count += 1

            result_store = await db.execute(
                select(CommissionStoreModel)
                    .where(CommissionStoreModel.fiscal_month == fiscal_month)
                    .where(CommissionStoreModel.store_code == store_code)
            )

            existing_store = result_store.scalar_one_or_none()

            if existing_store:
                existing_store.status = attendance_update.staff_status
                existing_store.updated_at = datetime.now()

                if attendance_update.staff_status == "saved":
                    existing_store.saved_by = role_code
                    existing_store.saved_at = datetime.now()
                elif attendance_update.staff_status == "submitted":
                    existing_store.submit_by = role_code
                    existing_store.submit_at = datetime.now()

            # 提交更改
            await db.commit()
            # 返回更新结果
            return True

        except Exception as e:
            # 发生异常时回滚事务
            app_logger.error(f"Error in update_commission: {str(e)}")
            await db.rollback()
            raise e

    @staticmethod
    async def batch_approved_commission_by_store_codes(db: AsyncSession, request: BatchApprovedCommission,
                                                       role_code: str = 'system') -> bool:
        try:
            # 查询匹配的commission记录
            fiscal_month = request.fiscal_month
            status = request.status
            store_codes = request.store_codes

            result = await db.execute(
                select(CommissionStoreModel)
                    .where(CommissionStoreModel.fiscal_month == fiscal_month)
                    .where(CommissionStoreModel.store_code.in_(store_codes))
            )

            commissions = result.scalars().all()

            for commission in commissions:
                commission.status = status
                commission.updated_at = datetime.now()
                if request.status == "approved":
                    commission.approved_by = role_code
                    commission.approved_at = datetime.now()
                elif request.status == "rejected":
                    commission.rejected_by = role_code
                    commission.rejected_at = datetime.now()
                    commission.reject_remarks = request.remarks
                elif request.status == "saved":
                    commission.saved_by = role_code
                    commission.saved_at = datetime.now()
                elif request.status == "submitted":
                    commission.submit_by = role_code
                    commission.submit_at = datetime.now()
            await db.commit()

            return True

        except Exception as e:
            await db.rollback()
            raise e

    @staticmethod
    async def withdrawn_commission(fiscal_month: str, store_code: str, db: AsyncSession):
        result = await db.execute(
            select(CommissionStoreModel)
                .where(CommissionStoreModel.fiscal_month == fiscal_month)
                .where(CommissionStoreModel.store_code == store_code)
                .where(CommissionStoreModel.status == 'submitted')
        )
        existing_commission = result.scalar_one_or_none()
        if existing_commission:
            existing_commission.status = 'saved'
            existing_commission.updated_at = datetime.now()
            await db.commit()
            return True
        return False

    @staticmethod
    async def get_all_commissions_by_key(role_code: str, fiscal_month: str, key_word: str, status: str,
                                         db: AsyncSession):
        try:
            store_permission_query = build_store_permission_query(role_code)
            store_alias = store_permission_query.subquery()

            # 添加关键词过滤条件
            query = (
                select(
                    CommissionStoreModel.store_code,
                    store_alias.c.store_name,
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
                          isouter=True)
                    .join(CommissionRuleDetailModel,
                          CommissionStaffModel.rule_detail_code == CommissionRuleDetailModel.rule_detail_code,
                          isouter=True)
                    .join(CommissionRuleModel,
                          CommissionRuleModel.rule_code == CommissionRuleDetailModel.rule_code,
                          isouter=True)
                    .join(store_alias,
                          CommissionStoreModel.store_code == store_alias.c.store_code)
                    .where(CommissionStoreModel.fiscal_month == fiscal_month)
            )

            # 如果提供了关键词，则添加过滤条件
            if key_word:
                query = query.where(
                    or_(
                        CommissionStoreModel.store_code.contains(key_word),
                        store_alias.c.store_name.contains(key_word)
                    )
                )
            if status != 'All':
                query = query.where(CommissionStoreModel.status == status)

            query = query.group_by(
                CommissionStoreModel.store_code,
                store_alias.c.store_name,
                CommissionStoreModel.fiscal_period,
                CommissionStoreModel.store_type,
                CommissionStoreModel.status,
                CommissionRuleModel.rule_class
            )

            result = await db.execute(query)
            commissions = result.fetchall()

            # 使用 defaultdict 简化数据处理逻辑
            from collections import defaultdict

            commission_dict = defaultdict(lambda: {
                'store_code': '',
                'store_name': '',
                'store_type': '',
                'fiscal_period': '',
                'status': '',
                'amount_individual': 0.0,
                'amount_team': 0.0,
                'amount_incentive': 0.0,
                'amount_adjustment': 0.0
            })

            fiscal_periods = set(commission.fiscal_period for commission in commissions if commission.fiscal_period)

            # 为每个 fiscal_period 获取日期范围
            date_ranges = {}
            for fp in fiscal_periods:
                if fp:
                    # 分割可能包含多个财月的 fiscal_period
                    months = [month.strip() for month in fp.split(',')]

                    # 查询日期范围
                    date_query = select(
                        func.min(DimensionDayWeek.actual_date).label('min_date'),
                        func.max(DimensionDayWeek.actual_date).label('max_date')
                    ).where(
                        DimensionDayWeek.fiscal_month.in_(months)
                    )

                    date_result = await db.execute(date_query)
                    date_row = date_result.fetchone()

                    if date_row and date_row.min_date and date_row.max_date:
                        date_ranges[
                            fp] = f"{date_row.min_date.strftime('%Y-%m-%d')} to {date_row.max_date.strftime('%Y-%m-%d')}"
                    else:
                        date_ranges[fp] = "N/A"

            for commission in commissions:
                store_code = commission.store_code

                # 初始化店铺信息
                if commission_dict[store_code]['store_code'] == '':
                    commission_dict[store_code].update({
                        'store_code': store_code or '',
                        'store_name': commission.store_name or '',
                        'store_type': commission.store_type or '',
                        'fiscal_period': date_ranges.get(commission.fiscal_period, ''),
                        'fiscal_period_value': commission.fiscal_period,
                        'status': commission.status or ''
                    })

                # 根据规则类别累加金额
                rule_class = commission.rule_class
                amount = commission.amount if commission.amount is not None else 0.0

                if rule_class == 'individual':
                    commission_dict[store_code]['amount_individual'] += amount
                elif rule_class == 'team':
                    commission_dict[store_code]['amount_team'] += amount
                elif rule_class == 'incentive':
                    commission_dict[store_code]['amount_incentive'] += amount
                elif rule_class == 'adjustment':
                    commission_dict[store_code]['amount_adjustment'] += amount

            # 转换为列表格式返回
            formatted_commissions = list(commission_dict.values())

            status_count_query = (
                select(
                    CommissionStoreModel.status,
                    func.count(CommissionStoreModel.store_code).label('count')
                )
                    .select_from(CommissionStoreModel)
                    .join(store_alias, CommissionStoreModel.store_code == store_alias.c.store_code)
                    .where(CommissionStoreModel.fiscal_month == fiscal_month)
                    .group_by(CommissionStoreModel.status)
            )
            status_count_result = await db.execute(status_count_query)
            status_counts = status_count_result.fetchall()

            # 转换为字典格式
            status_count_dict = {row.status: row.count for row in status_counts}

            field_translations = {
                "store_code": {"en": "Store Code", "zh": "店铺代码"},
                "store_name": {"en": "Store Name", "zh": "店铺名称"},
                "store_type": {"en": "Store Type", "zh": "店铺类型"},
                "fiscal_period": {"en": "Fiscal Period", "zh": "计算周期"},
                "status": {"en": "Status", "zh": "状态"},
                "amount_individual": {"en": "Individual Amount", "zh": "个人提成"},
                "amount_team": {"en": "Team Amount", "zh": "团队提成"},
                "amount_incentive": {"en": "Incentive Amount", "zh": "激励金额"},
                "amount_adjustment": {"en": "Adjustment Amount", "zh": "调整金额"}
            }

            main_result = await db.execute(
                select(CommissionMainModel.month_end)
                    .where(CommissionMainModel.fiscal_month == fiscal_month)
            )
            main_record = main_result.fetchone()
            month_end_value = main_record.month_end if main_record else 0

            return {"data": formatted_commissions,
                    "status_counts": status_count_dict,
                    "field_translations": field_translations,
                    "MonthEnd": month_end_value}

        except Exception as e:
            # 记录异常信息（在实际应用中应该使用日志记录器）
            app_logger.error(f"Error in get_all_commissions_by_key: {str(e)}")
            # 抛出异常以便上层处理
            raise e

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
        try:
            adjustment_commission = CommissionStaffModel(
                fiscal_month=adjustment.fiscal_month,
                staff_code=adjustment.staff_code,
                store_code=adjustment.store_code,
                amount=adjustment.amount,
                rule_detail_code="Z-01"  # 设置为调整类型
            )

            db.add(adjustment_commission)
            await db.commit()
            await db.refresh(adjustment_commission)

            return adjustment_commission
        except Exception as e:
            # 捕获异常并记录错误信息
            app_logger.error(f"Error in create_add_adjustment: {str(e)}")
            # 抛出异常以便上层处理
            raise e

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
            app_logger.info(f"开始为店铺 {store_code} 在财月 {fiscal_month} 计算佣金")

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
                app_logger.warning(f"店铺 {store_code} 在财月 {fiscal_month} 没有找到数据")
                raise ValueError(f"Store {store_code} not found or has no data for {fiscal_month}")

            store_type = store_data.store_type
            store_target_value = store_data.target_value or 0
            store_sales_value = store_data.sales_value or 0

            app_logger.debug(f"店铺 {store_code} 类型: {store_type}, 目标值: {store_target_value}, 销售额: {store_sales_value}")

            if not store_type:
                app_logger.warning(f"店铺 {store_code} 缺少店铺类型信息")
                raise ValueError(f"Store {store_code} has no store_type")

            # 计算店铺达成率
            if store_target_value > 0 and store_sales_value is not None:
                store_achievement_rate = (store_sales_value / store_target_value) * 100
            else:
                store_achievement_rate = 0

            app_logger.debug(f"店铺 {store_code} 达成率: {store_achievement_rate}%")

            # 2. 获取该店铺所有员工的考勤和岗位信息
            app_logger.debug(f"正在获取店铺 {store_code} 所有员工的考勤信息")
            staff_attendances_result = await db.execute(
                select(
                    StaffAttendanceModel.staff_code,
                    StaffAttendanceModel.position,
                    StaffAttendanceModel.actual_attendance,
                    StaffAttendanceModel.expected_attendance,
                    StaffAttendanceModel.salary_coefficient,
                    StaffAttendanceModel.target_value_ratio,
                    StaffAttendanceModel.sales_value,
                ).where(
                    StaffAttendanceModel.store_code == store_code,
                    StaffAttendanceModel.fiscal_month == fiscal_month
                )
            )
            staff_attendances = staff_attendances_result.all()
            app_logger.info(f"找到 {len(staff_attendances)} 名员工需要计算佣金")

            # 3. 删除该店铺该财月的所有现有佣金记录
            app_logger.debug(f"删除店铺 {store_code} 在财月 {fiscal_month} 的现有佣金记录")
            delete_result = await db.execute(
                delete(CommissionStaffModel)
                    .where(
                    CommissionStaffModel.fiscal_month == fiscal_month,
                    CommissionStaffModel.store_code == store_code
                )
            )
            app_logger.debug(f"删除了 {delete_result.rowcount} 条现有佣金记录")

            # 4. 如果没有员工数据，直接提交事务并返回
            if not staff_attendances:
                app_logger.info(f"店铺 {store_code} 没有员工数据，直接提交事务")
                await db.commit()
                return True

            # 5. 收集所有需要的规则代码（一个岗位可能对应多个规则）
            positions = list(set(staff.position for staff in staff_attendances))
            app_logger.debug(f"涉及的岗位类型: {positions}")

            position_actual_attendance = {}
            for staff in staff_attendances:
                position = staff.position
                actual_attendance = getattr(staff, 'actual_attendance', 0) or 0
                if position in position_actual_attendance:
                    position_actual_attendance[position] += actual_attendance
                else:
                    position_actual_attendance[position] = actual_attendance

            app_logger.debug(f"岗位对应的实际勤天数: {position_actual_attendance}")

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

            app_logger.debug(f"岗位到规则映射: {position_to_rules}")

            # 6. 获取所有需要用到的规则信息
            all_rule_codes = []
            for rule_codes in position_to_rules.values():
                all_rule_codes.extend(rule_codes)

            all_rule_codes = list(set(all_rule_codes))  # 去重

            if not all_rule_codes:
                app_logger.warning(f"未找到适用的规则代码")
                await db.commit()
                return True

            app_logger.debug(f"需要获取信息的规则代码: {all_rule_codes}")

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

            app_logger.debug(f"获取到的规则信息: {rules_info}")

            commission_records = []

            # 7. 为每个员工计算佣金（应用所有适用的规则）
            for staff in staff_attendances:
                app_logger.debug(f"正在为员工 {staff.staff_code} 计算佣金")

                # 检查员工目标值
                staff_target_value = store_target_value * (staff.target_value_ratio or 0)
                app_logger.debug(f"员工 {staff.staff_code} 目标值: {staff_target_value}")

                if staff_target_value is None or staff_target_value == 0:
                    app_logger.warning(f"员工 {staff.staff_code} 目标值为空或为0，跳过计算")
                    continue

                # 计算员工达成率
                staff_sales_value = staff.sales_value or 0
                if staff_target_value > 0 and staff_sales_value is not None:
                    staff_achievement_rate = (staff_sales_value / staff_target_value) * 100
                else:
                    staff_achievement_rate = 0

                app_logger.debug(f"员工 {staff.staff_code} 达成率: {staff_achievement_rate}%")

                # 获取适用于该岗位的所有规则代码
                rule_codes = position_to_rules.get(staff.position, [])
                app_logger.debug(f"员工 {staff.staff_code} 岗位 {staff.position} 适用的规则: {rule_codes}")

                if not rule_codes:
                    app_logger.warning(f"员工 {staff.staff_code} 没有适用的规则，跳过计算")
                    continue

                # 为每个适用的规则计算佣金
                for rule_code in rule_codes:
                    app_logger.debug(f"正在应用规则 {rule_code} 计算员工 {staff.staff_code} 的佣金")

                    # 获取规则信息
                    rule_info = rules_info.get(rule_code)
                    if not rule_info:
                        app_logger.warning(f"未找到规则 {rule_code} 的详细信息，跳过")
                        continue

                    # 根据 rule_basis 选择合适的达成率和销售额
                    if rule_info.rule_basis == 'individual':
                        target_achievement_rate = staff_achievement_rate
                        sales_value = staff_sales_value
                    elif rule_info.rule_basis == 'store':
                        target_achievement_rate = store_achievement_rate
                        sales_value = store_sales_value
                    else:
                        app_logger.warning(f"规则 {rule_code} 的 rule_basis 值无效: {rule_info.rule_basis}")
                        continue

                    app_logger.debug(f"使用达成率: {target_achievement_rate}%, 销售额: {sales_value}")

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
                        app_logger.warning(f"未找到规则 {rule_code} 匹配达成率 {target_achievement_rate}% 的详情")
                        continue

                    app_logger.debug(f"匹配的规则详情: {matching_detail}")

                    # 计算佣金
                    commission_amount = 0.0
                    rule_detail_value = matching_detail.value or 0

                    if rule_info.rule_type == 'commission':
                        if sales_value is not None:
                            commission_amount = sales_value * (rule_detail_value / 100)
                            app_logger.debug(f"佣金计算: {sales_value} * ({rule_detail_value}/100) = {commission_amount}")
                    elif rule_info.rule_type == 'incentive':
                        commission_amount = rule_detail_value
                        app_logger.debug(f"激励金额: {commission_amount}")

                    # 考虑出勤率
                    if rule_info.consider_attendance:
                        expected_attendance = staff.expected_attendance or 0
                        actual_attendance = staff.actual_attendance or 0

                        if expected_attendance > 0:
                            if rule_info.rule_basis == 'store':
                                total_attendance = position_actual_attendance[staff.position]
                                if total_attendance > 0:
                                    attendance_factor = actual_attendance / total_attendance
                                    commission_amount = commission_amount * attendance_factor
                                app_logger.debug(
                                    f"考虑出勤率调整店提: {commission_amount} * ({actual_attendance}/{total_attendance})")
                            else:
                                attendance_factor = actual_attendance / expected_attendance
                                commission_amount = commission_amount * attendance_factor
                                app_logger.debug(
                                    f"考虑出勤率调整incentive: {commission_amount} * ({actual_attendance}/{expected_attendance})")
                        else:
                            commission_amount = 0
                            app_logger.warning(f"员工 {staff.staff_code} 应出勤为0，不应用出勤率调整")

                    # 最低保障金额
                    if rule_info.minimum_guarantee and commission_amount < rule_info.minimum_guarantee:
                        old_amount = commission_amount
                        commission_amount = rule_info.minimum_guarantee
                        app_logger.debug(f"应用最低保障金额: {old_amount} -> {commission_amount}")

                    # 只有佣金金额大于0时才保存
                    if commission_amount and commission_amount > 0:
                        commission_amount = round(commission_amount, 2)
                        app_logger.debug(f"为员工 {staff.staff_code} 创建佣金记录: {commission_amount}")
                        commission_record = CommissionStaffModel(
                            fiscal_month=fiscal_month,
                            staff_code=staff.staff_code,
                            store_code=store_code,
                            amount=commission_amount,
                            rule_detail_code=matching_detail.rule_detail_code
                        )
                        commission_records.append(commission_record)
                    else:
                        app_logger.debug(f"员工 {staff.staff_code} 计算的佣金金额为0或负数，跳过记录")

            # 8. 批量添加佣金记录
            if commission_records:
                app_logger.info(f"准备插入 {len(commission_records)} 条佣金记录")
                db.add_all(commission_records)
            else:
                app_logger.info("没有需要插入的佣金记录")

            # 9. 提交事务
            app_logger.info(f"提交店铺 {store_code} 的佣金计算结果")
            await db.commit()
            app_logger.info(f"成功完成店铺 {store_code} 在财月 {fiscal_month} 的佣金计算")
            return True

        except Exception as e:
            app_logger.error(f"计算店铺 {store_code} 在财月 {fiscal_month} 的佣金时发生错误: {e}", exc_info=True)
            await db.rollback()
            raise e

    @staticmethod
    async def update_store_type(db: AsyncSession, fiscal_month: str, store_code: str, store_type: str):
        result = await db.execute(select(CommissionStoreModel).where(
            CommissionStoreModel.store_code == store_code,
            CommissionStoreModel.fiscal_month == fiscal_month
        ))
        Commission_store = result.scalar_one_or_none()

        if Commission_store:
            Commission_store.store_type = store_type
            Commission_store.updated_at = datetime.utcnow()

        result = await db.execute(select(TargetStoreMain).where(
            TargetStoreMain.store_code == store_code,
            TargetStoreMain.fiscal_month == fiscal_month
        ))
        Target_store = result.scalar_one_or_none()

        if Target_store:
            Target_store.store_type = store_type
            Target_store.updated_at = datetime.utcnow()

        result = await db.execute(select(StoreModel).where(
            StoreModel.store_code == store_code
        ))
        store = result.scalar_one_or_none()

        if store:
            store.store_type = store_type

        await db.commit()
        return Commission_store

    @staticmethod
    async def update_fiscal_period(db: AsyncSession, fiscal_month: str, store_code: str, fiscal_periods: List[str]):

        fiscal_period_str = ','.join(fiscal_periods) if fiscal_periods else ''

        result = await db.execute(select(CommissionStoreModel).where(
            CommissionStoreModel.store_code == store_code,
            CommissionStoreModel.fiscal_month == fiscal_month
        ))
        Commission_store = result.scalar_one_or_none()
        if Commission_store:
            Commission_store.fiscal_period = fiscal_period_str
            Commission_store.updated_at = datetime.utcnow()
            await db.commit()
            return Commission_store

    @staticmethod
    async def add_month_end(db: AsyncSession, fiscal_month: str, role_code: str):
        try:
            # 查询是否存在相同fiscal_month的记录
            result = await db.execute(
                select(CommissionMainModel)
                    .where(CommissionMainModel.fiscal_month == fiscal_month)
            )
            existing_record = result.scalar_one_or_none()

            if existing_record:
                # 如果记录存在，更新month_end为1
                existing_record.month_end = 1
                existing_record.updated_at = datetime.now()
                month_end_record = existing_record
            else:
                # 如果记录不存在，创建新记录
                month_end_record = CommissionMainModel(
                    fiscal_month=fiscal_month,
                    month_end=1,
                    created_at=datetime.now()
                )
                db.add(month_end_record)

            await db.commit()
            await db.refresh(month_end_record)

            return month_end_record

        except Exception as e:
            app_logger.error(f"Error in add_month_end: {str(e)}")
            await db.rollback()
            raise e
