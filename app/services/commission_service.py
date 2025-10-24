from typing import List, Dict

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models.commission import CommissionStaffModel, CommissionStoreModel, CommissionRuleModel, \
    CommissionRuleAssignmentModel, CommissionRuleDetailModel, CommissionMainModel
from app.models.dimension import StoreModel, DimensionDayWeek, RoleOrgJoin
from app.models.staff import StaffAttendanceModel, StaffModel
from app.schemas.commission import CommissionCreate, CommissionUpdate, CommissionStaffCreate, BatchApprovedCommission
from app.models.target import TargetStoreMain

from datetime import datetime
from sqlalchemy import func, or_, exists, case
from sqlalchemy.orm import aliased
from sqlalchemy import delete
from app.utils.permissions import build_store_permission_query
from app.utils.logger import app_logger


class CommissionRPTService:

    @staticmethod
    async def get_rpt_commission_by_store(db: AsyncSession, fiscal_month: str, key_word: str, status: str,
                                          role_code: str):
        try:
            # 构建查询，包含所有需要的字段
            store_permission_query = build_store_permission_query(role_code)
            store_alias = store_permission_query.subquery()

            # 构建主查询
            query = (
                select(
                    StaffModel.staff_code.label('staff_code'),
                    func.concat(StaffModel.first_name, StaffModel.last_name).label('full_name'),
                    StaffModel.position_code.label('position_code'),
                    StaffAttendanceModel.position.label('position'),
                    StaffAttendanceModel.expected_attendance.label('expected_attendance'),
                    StaffAttendanceModel.target_value.label('target_value'),
                    StaffAttendanceModel.sales_value.label('sales_value'),
                    (StaffAttendanceModel.sales_value / StaffAttendanceModel.target_value * 100).label(
                        'achievement_rate'),
                    CommissionRuleDetailModel.value.label('individual_commission_percent'),
                    CommissionStaffModel.amount.label('amount'),
                    StaffAttendanceModel.actual_attendance.label('actual_attendance'),
                    CommissionRuleModel.rule_code.label('rule_code'),
                    CommissionStaffModel.total_days_store_work.label('total_days_store_work'),
                    CommissionStoreModel.store_code.label('store_code'),
                    CommissionStoreModel.fiscal_month.label('fiscal_month'),
                    TargetStoreMain.sales_value.label('store_sales_value'),
                    TargetStoreMain.target_value.label('store_target_value'),
                    (TargetStoreMain.sales_value / TargetStoreMain.target_value * 100).label('store_achievement_rate'),
                    store_alias.c.manage_region.label('manage_region'),
                    store_alias.c.manage_channel.label('manage_channel'),
                    store_alias.c.city.label('city'),
                    store_alias.c.city_tier.label('city_tier')
                )
                    .select_from(CommissionStoreModel)
                    .join(CommissionStaffModel,
                          (CommissionStoreModel.fiscal_month == CommissionStaffModel.fiscal_month) &
                          (CommissionStoreModel.store_code == CommissionStaffModel.store_code))
                    .join(CommissionRuleDetailModel,
                          CommissionStaffModel.rule_detail_code == CommissionRuleDetailModel.rule_detail_code)
                    .join(CommissionRuleModel,
                          CommissionRuleDetailModel.rule_code == CommissionRuleModel.rule_code)
                    .join(StaffAttendanceModel,
                          (CommissionStoreModel.fiscal_month == StaffAttendanceModel.fiscal_month) &
                          (CommissionStoreModel.store_code == StaffAttendanceModel.store_code))
                    .join(TargetStoreMain,
                          (TargetStoreMain.store_code == CommissionStoreModel.store_code) &
                          (TargetStoreMain.fiscal_month == CommissionStoreModel.fiscal_month))
                    .join(StaffModel,
                          CommissionStaffModel.staff_code == StaffModel.staff_code)
                    .join(store_alias,
                          CommissionStoreModel.store_code == store_alias.c.store_code)
                    .where(CommissionStoreModel.fiscal_month == fiscal_month)
            )

            # 如果提供了关键词，则添加过滤条件
            if key_word:
                query = query.where(
                    or_(
                        CommissionStoreModel.store_code.contains(key_word),
                        store_alias.c.store_name.contains(key_word),
                        StaffModel.staff_code.contains(key_word),
                        StaffModel.first_name.contains(key_word),
                        StaffModel.last_name.contains(key_word)
                    )
                )
            if status != 'All':
                query = query.where(CommissionStoreModel.status == status)

            result = await db.execute(query)
            rows = result.fetchall()

            # 获取区域、渠道和城市层级的聚合数据
            # 区域达成率
            region_achievement_query = (
                select(
                    store_alias.c.manage_region,
                    func.sum(TargetStoreMain.sales_value).label('region_sales'),
                    func.sum(TargetStoreMain.target_value).label('region_target')
                )
                    .select_from(CommissionStoreModel)
                    .join(TargetStoreMain,
                          (TargetStoreMain.store_code == CommissionStoreModel.store_code) &
                          (TargetStoreMain.fiscal_month == CommissionStoreModel.fiscal_month))
                    .join(store_alias,
                          CommissionStoreModel.store_code == store_alias.c.store_code)
                    .where(CommissionStoreModel.fiscal_month == fiscal_month)
                    .group_by(store_alias.c.manage_region)
            )

            region_result = await db.execute(region_achievement_query)
            region_achievements = {
                row.manage_region: (row.region_sales / row.region_target * 100)
                if row.region_target and row.region_target > 0 else 0
                for row in region_result.fetchall()
            }

            # 渠道达成率
            channel_achievement_query = (
                select(
                    store_alias.c.manage_channel,
                    func.sum(TargetStoreMain.sales_value).label('channel_sales'),
                    func.sum(TargetStoreMain.target_value).label('channel_target')
                )
                    .select_from(CommissionStoreModel)
                    .join(TargetStoreMain,
                          (TargetStoreMain.store_code == CommissionStoreModel.store_code) &
                          (TargetStoreMain.fiscal_month == CommissionStoreModel.fiscal_month))
                    .join(store_alias,
                          CommissionStoreModel.store_code == store_alias.c.store_code)
                    .where(CommissionStoreModel.fiscal_month == fiscal_month)
                    .group_by(store_alias.c.manage_channel)
            )

            channel_result = await db.execute(channel_achievement_query)
            channel_achievements = {
                row.manage_channel: (row.channel_sales / row.channel_target * 100)
                if row.channel_target and row.channel_target > 0 else 0
                for row in channel_result.fetchall()
            }

            # 格式化数据
            formatted_data = []
            for row in rows:
                # 计算每日目标值
                target_value = float(row.target_value) if row.target_value is not None else 0.0
                expected_attendance = float(row.expected_attendance) if row.expected_attendance is not None else 0.0
                daily_target = target_value / expected_attendance if expected_attendance > 0 else 0.0

                # 获取区域和渠道达成率
                region_achievement = region_achievements.get(row.manage_region, 0.0)
                channel_achievement = channel_achievements.get(row.manage_channel, 0.0)

                formatted_data.append({
                    "staff_no": row.staff_code or '',
                    "full_name": row.full_name or '',
                    "position_from_wd": row.position_code or '',
                    "position": row.position or '',
                    "expected_attendance": expected_attendance,
                    "monthly_target": target_value,
                    "daily_target": round(daily_target, 2),
                    "achievement_rate": round(float(row.achievement_rate) if row.achievement_rate is not None else 0.0,
                                              2),
                    "individual_commission_percent": float(
                        row.individual_commission_percent) if row.individual_commission_percent is not None else 0.0,
                    "amount": float(row.amount) if row.amount is not None else 0.0,
                    "actual_attendance": float(row.actual_attendance) if row.actual_attendance is not None else 0.0,
                    "rule_code": row.rule_code or '',
                    "total_days_store_work": float(
                        row.total_days_store_work) if row.total_days_store_work is not None else 0.0,
                    "store_code": row.store_code or '',
                    "fiscal_month": row.fiscal_month or '',
                    "store_sales_value": float(row.store_sales_value) if row.store_sales_value is not None else 0.0,
                    "store_target_value": float(row.store_target_value) if row.store_target_value is not None else 0.0,
                    "store_achievement_rate": round(
                        float(row.store_achievement_rate) if row.store_achievement_rate is not None else 0.0, 2),
                    "manage_region": row.manage_region or '',
                    "manage_channel": row.manage_channel or '',
                    "city": row.city or '',
                    "city_tier": row.city_tier or '',
                    "region_achievement_rate": round(region_achievement, 2),
                    "channel_achievement_rate": round(channel_achievement, 2)
                })

            # 字段翻译
            field_translations = {
                "staff_no": {"en": "Staff No.", "zh": "员工编号"},
                "full_name": {"en": "Supervisor/Sales Associates", "zh": "主管/销售助理"},
                "position_from_wd": {"en": "Position (From WD)", "zh": "职位(来自WD)"},
                "position": {"en": "Possition", "zh": "岗位"},
                "expected_attendance": {"en": "Actual Working Days", "zh": "实际工作天数"},
                "monthly_target": {"en": "Monthly Target", "zh": "月度目标"},
                "daily_target": {"en": "Daily Target Sales", "zh": "每日目标销售额"},
                "achievement_rate": {"en": "vs ind target (列名更新：Individual Achievement%)", "zh": "个人达成率"},
                "individual_commission_percent": {"en": "Individual Commission %", "zh": "个人佣金比例"},
                "amount": {"en": "Individual Commission Pool Commission", "zh": "个人佣金池佣金"},
                "actual_attendance": {"en": "Actual Working Days.1", "zh": "实际工作天数.1"},
                "rule_code": {"en": "Individual Type", "zh": "规则类型"},
                "total_days_store_work": {"en": "Total Days of Store's Acutal Working Days", "zh": "折算团队奖金的全店总天数"},
                "store_code": {"en": "Store Code", "zh": "店铺代码"},
                "fiscal_month": {"en": "Month", "zh": "月份"},
                "store_sales_value": {"en": "Store Sales", "zh": "店铺销售额"},
                "store_achievement_rate": {"en": "Store Achieved Rate", "zh": "店铺达成率"},
                "manage_region": {"en": "Region", "zh": "区域"},
                "region_achievement_rate": {"en": "Regional Achieved Rate", "zh": "区域达成率"},
                "manage_channel": {"en": "Channel", "zh": "渠道"},
                "channel_achievement_rate": {"en": "Channel Achieved Rate", "zh": "渠道达成率"},
                "city": {"en": "City", "zh": "城市"},
                "city_tier": {"en": "City Tier", "zh": "城市等级"}
            }

            return {
                "data": formatted_data,
                "field_translations": field_translations
            }

        except Exception as e:
            app_logger.error(f"Error in get_rpt_commission_by_store: {str(e)}")
            raise e

    @staticmethod
    async def get_rpt_sales_by_achievement(db: AsyncSession, fiscal_month: str, key_word: str,status: str, role_code: str):
        try:
            # 构建权限查询
            store_permission_query = build_store_permission_query(role_code)
            store_alias = store_permission_query.subquery()

            # 构建主查询
            query = (
                select(
                    CommissionStoreModel.fiscal_month.label('fiscal_month'),
                    CommissionStoreModel.store_code.label('store_code'),
                    store_alias.c.manage_region.label('manage_region'),
                    store_alias.c.store_name.label('store_name'),
                    StaffAttendanceModel.staff_code.label('staff_code'),
                    func.concat(StaffModel.first_name, StaffModel.last_name).label('full_name'),
                    StaffAttendanceModel.target_value.label('target_value'),
                    StaffAttendanceModel.sales_value.label('sales_value'),
                    (StaffAttendanceModel.sales_value / StaffAttendanceModel.target_value * 100).label(
                        'achievement_rate'),
                    StaffModel.position_code.label('position_code'),
                    StaffAttendanceModel.position.label('position')
                )
                    .select_from(CommissionStoreModel)
                    .join(StaffAttendanceModel,
                          (CommissionStoreModel.store_code == StaffAttendanceModel.store_code) &
                          (CommissionStoreModel.fiscal_month == StaffAttendanceModel.fiscal_month))
                    .join(StaffModel,
                          StaffAttendanceModel.staff_code == StaffModel.staff_code)
                    .join(store_alias,
                          CommissionStoreModel.store_code == store_alias.c.store_code)
                    .where(CommissionStoreModel.fiscal_month == fiscal_month)
                    .order_by(CommissionStoreModel.store_code)
            )

            # 如果提供了关键词，则添加过滤条件
            if key_word:
                query = query.where(
                    or_(
                        CommissionStoreModel.store_code.contains(key_word),
                        store_alias.c.store_name.contains(key_word),
                        StaffAttendanceModel.staff_code.contains(key_word),
                        StaffModel.first_name.contains(key_word),
                        StaffModel.last_name.contains(key_word)
                    )
                )

            if status != 'All':
                query = query.where(CommissionStoreModel.status == status)

            result = await db.execute(query)
            rows = result.fetchall()

            # 格式化数据
            formatted_data = []
            for row in rows:
                # 计算达成率，避免除零错误
                target_value = float(row.target_value) if row.target_value is not None else 0.0
                sales_value = float(row.sales_value) if row.sales_value is not None else 0.0
                achievement_rate = 0.0
                if target_value > 0:
                    achievement_rate = (sales_value / target_value) * 100

                formatted_data.append({
                    "fiscal_month": row.fiscal_month or '',
                    "store_code": row.store_code or '',
                    "manage_region": row.manage_region or '',
                    "store_name": row.store_name or '',
                    "staff_code": row.staff_code or '',
                    "full_name": row.full_name or '',
                    "target_value": target_value,
                    "sales_value": sales_value,
                    "achievement_rate": round(achievement_rate, 2),
                    "position_code": row.position_code or '',
                    "position": row.position or ''
                })

            # 字段翻译
            field_translations = {
                "fiscal_month": {"en": "Month", "zh": "月份"},
                "store_code": {"en": "Store ID", "zh": "店铺ID"},
                "manage_region": {"en": "Region", "zh": "区域"},
                "store_name": {"en": "Store", "zh": "店铺"},
                "staff_code": {"en": "Staff ID", "zh": "员工ID"},
                "full_name": {"en": "Full Name", "zh": "姓名"},
                "target_value": {"en": "Monthly Target", "zh": "月销售指标"},
                "sales_value": {"en": "Monthly Sales", "zh": "月销售"},
                "achievement_rate": {"en": "% TGT Ach", "zh": "月达成"},
                "position_code": {"en": "Position", "zh": "职位"},
                "position": {"en": "Position Type", "zh": "职位类型"}
            }

            return {
                "data": formatted_data,
                "field_translations": field_translations
            }

        except Exception as e:
            app_logger.error(f"Error in get_rpt_sales_by_achievement: {str(e)}")
            raise e

    @staticmethod
    async def get_rpt_commission_payout(db: AsyncSession, fiscal_month: str, key_word: str, status: str,role_code: str):
        try:
            # 构建权限查询
            store_permission_query = build_store_permission_query(role_code)
            store_alias = store_permission_query.subquery()

            # 构建主查询
            query = (
                select(
                    store_alias.c.store_name.label('store_name'),
                    CommissionStoreModel.store_code.label('store_code'),
                    CommissionStaffModel.staff_code.label('staff_code'),
                    func.concat(StaffModel.first_name, StaffModel.last_name).label('full_name'),
                    StaffModel.position_code.label('position_code'),
                    func.sum(
                        case(
                            (CommissionRuleModel.rule_class != 'incentive', CommissionStaffModel.amount),
                            else_=0
                        )
                    ).label('commission_only'),
                    func.sum(
                        case(
                            (CommissionRuleModel.rule_class == 'incentive', CommissionStaffModel.amount),
                            else_=0
                        )
                    ).label('incentive'),
                    func.sum(CommissionStaffModel.amount).label('total_commission')
                )
                    .select_from(CommissionStoreModel)
                    .join(CommissionStaffModel,
                          (CommissionStoreModel.store_code == CommissionStaffModel.store_code) &
                          (CommissionStoreModel.fiscal_month == CommissionStaffModel.fiscal_month))
                    .join(CommissionRuleDetailModel,
                          CommissionStaffModel.rule_detail_code == CommissionRuleDetailModel.rule_detail_code)
                    .join(CommissionRuleModel,
                          CommissionRuleDetailModel.rule_code == CommissionRuleModel.rule_code)
                    .join(StaffModel,
                          CommissionStaffModel.staff_code == StaffModel.staff_code)
                    .join(store_alias,
                          CommissionStoreModel.store_code == store_alias.c.store_code)
                    .where(CommissionStoreModel.fiscal_month == fiscal_month)
                    .group_by(
                    store_alias.c.store_name,
                    CommissionStoreModel.store_code,
                    CommissionStaffModel.staff_code,
                    StaffModel.first_name,
                    StaffModel.last_name,
                    StaffModel.position_code
                )
                    .order_by(CommissionStoreModel.store_code)
            )

            # 如果提供了关键词，则添加过滤条件
            if key_word:
                query = query.where(
                    or_(
                        CommissionStoreModel.store_code.contains(key_word),
                        store_alias.c.store_name.contains(key_word),
                        CommissionStaffModel.staff_code.contains(key_word),
                        StaffModel.first_name.contains(key_word),
                        StaffModel.last_name.contains(key_word)
                    )
                )
            if status != 'All':
                query = query.where(CommissionStoreModel.status == status)

            result = await db.execute(query)
            rows = result.fetchall()

            # 格式化数据
            formatted_data = []
            for row in rows:
                commission_only = float(row.commission_only) if row.commission_only is not None else 0.0
                incentive = float(row.incentive) if row.incentive is not None else 0.0
                total_commission = float(row.total_commission) if row.total_commission is not None else 0.0

                formatted_data.append({
                    "store_name": row.store_name or '',
                    "store_code": row.store_code or '',
                    "staff_code": row.staff_code or '',
                    "full_name": row.full_name or '',
                    "position_code": row.position_code or '',
                    "commission_only": commission_only,
                    "incentive": incentive,
                    "total_commission": total_commission
                })

            # 字段翻译
            field_translations = {
                "store_name": {"en": "Store", "zh": "店铺"},
                "store_code": {"en": "Store ID", "zh": "店铺ID"},
                "staff_code": {"en": "Staff ID", "zh": "员工ID"},
                "full_name": {"en": "Full Name", "zh": "姓名"},
                "position_code": {"en": "Position", "zh": "职位"},
                "commission_only": {"en": "Commission only", "zh": "纯佣金"},
                "incentive": {"en": "Incentive", "zh": "激励金额"},
                "total_commission": {"en": "Total Commission", "zh": "总佣金"}
            }

            return {
                "data": formatted_data,
                "field_translations": field_translations
            }

        except Exception as e:
            app_logger.error(f"Error in get_rpt_commission_payout: {str(e)}")
            raise e


class CommissionUtil:
    @staticmethod
    async def get_month_end_value(db: AsyncSession, fiscal_month: str) -> int:
        """
        获取指定财月的月结状态

        Args:
            db: 数据库会话
            fiscal_month: 财月

        Returns:
            int: 月结状态值 (0: 未月结, 1: 已月结)
        """
        try:
            result = await db.execute(
                select(CommissionMainModel.month_end)
                    .where(CommissionMainModel.fiscal_month == fiscal_month)
            )
            record = result.fetchone()
            return record.month_end if record and record.month_end else 0
        except Exception as e:
            app_logger.error(f"Error getting month end value for {fiscal_month}: {str(e)}")
            return 0


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
    async def update_commission(db: AsyncSession, attendance_update, role_code: str) -> bool:
        try:
            app_logger.info(f"Starting update_commission for store {attendance_update.store_code}, "
                            f"fiscal_month {attendance_update.fiscal_month}")
            app_logger.debug(f"Attendance update data: {attendance_update}")
            app_logger.debug(f"Role code: {role_code}")

            # 获取传入的参数
            store_code = attendance_update.store_code
            fiscal_month = attendance_update.fiscal_month
            staff_attendances = attendance_update.staff_actual_attendance

            updated_count = 0

            # 初始化 merged_codes 为默认空列表
            merged_codes = []
            app_logger.debug(f"Querying CommissionStoreModel for store {store_code}, fiscal_month {fiscal_month}")

            result = await db.execute(
                select(CommissionStoreModel.merged_store_codes)
                    .where(CommissionStoreModel.store_code == store_code)
                    .where(CommissionStoreModel.fiscal_month == fiscal_month)
            )
            commission_store = result.fetchone()
            app_logger.debug(f"CommissionStore query result: {commission_store}")

            # 安全地处理 merged_store_codes
            if commission_store and commission_store.merged_store_codes:
                merged_code = commission_store.merged_store_codes.split(',')
                merged_codes = [code.strip() for code in merged_code]
                app_logger.debug(f"Merged codes found: {merged_codes}")
            else:
                app_logger.debug("No merged codes found")

            # 遍历所有需要更新的员工
            app_logger.info(f"Processing {len(staff_attendances)} staff attendances")
            for idx, staff_attendance in enumerate(staff_attendances):
                staff_code = staff_attendance.staff_code
                actual_attendance = staff_attendance.actual_attendance
                app_logger.debug(f"Processing staff [{idx + 1}/{len(staff_attendances)}]: {staff_code}, "
                                 f"actual_attendance: {actual_attendance}")

                # 查询现有的员工考勤记录
                app_logger.debug(f"Querying StaffAttendanceModel for staff {staff_code}, store {store_code}")
                result = await db.execute(
                    select(StaffAttendanceModel)
                        .where(StaffAttendanceModel.staff_code == staff_code)
                        .where(StaffAttendanceModel.store_code == store_code)
                        .where(StaffAttendanceModel.fiscal_month == fiscal_month)
                )

                staff_record = result.scalar_one_or_none()
                app_logger.debug(f"Staff record found: {staff_record is not None}")

                # 如果找到记录，则更新实际出勤字段
                if staff_record:
                    app_logger.debug(
                        f"Updating staff {staff_code} attendance from {staff_record.actual_attendance} to {actual_attendance}")
                    staff_record.actual_attendance = actual_attendance
                    staff_record.updated_at = datetime.now()
                    updated_count += 1
                    app_logger.debug(f"Updated staff {staff_code} successfully")
                else:
                    app_logger.debug(f"No direct staff record found for {staff_code} in {store_code}")
                    # 只有当 merged_codes 存在且不为空时才执行
                    if merged_codes:
                        app_logger.debug(f"Checking merged stores: {merged_codes}")
                        for merged_store_code in merged_codes:
                            if merged_store_code != store_code:
                                app_logger.debug(f"Checking merged store: {merged_store_code}")
                                result = await db.execute(
                                    select(StaffAttendanceModel)
                                        .where(StaffAttendanceModel.staff_code == staff_code)
                                        .where(StaffAttendanceModel.store_code == merged_store_code)
                                        .where(StaffAttendanceModel.fiscal_month == fiscal_month)
                                )
                                merged_staff_record = result.scalar_one_or_none()
                                app_logger.debug(
                                    f"Merged staff record found in {merged_store_code}: {merged_staff_record is not None}")

                                if merged_staff_record:
                                    app_logger.debug(
                                        f"Updating merged staff {staff_code} attendance from {merged_staff_record.actual_attendance} to {actual_attendance}")
                                    merged_staff_record.actual_attendance = actual_attendance
                                    merged_staff_record.updated_at = datetime.now()
                                    updated_count += 1
                                    app_logger.debug(
                                        f"Updated merged staff {staff_code} in {merged_store_code} successfully")
                                    break  # 找到并更新后退出循环
                            else:
                                app_logger.debug(f"Skipping same store code: {merged_store_code}")
                    else:
                        app_logger.debug("No merged codes to check")

            app_logger.info(f"Total updated staff records: {updated_count}")

            app_logger.debug(f"Querying CommissionStoreModel for status update")
            result_store = await db.execute(
                select(CommissionStoreModel)
                    .where(CommissionStoreModel.fiscal_month == fiscal_month)
                    .where(CommissionStoreModel.store_code == store_code)
            )

            existing_store = result_store.scalar_one_or_none()
            app_logger.debug(f"Existing store record for status update: {existing_store is not None}")

            if existing_store:
                old_status = existing_store.status
                new_status = attendance_update.staff_status
                app_logger.debug(f"Updating store status from '{old_status}' to '{new_status}'")
                existing_store.status = new_status
                existing_store.updated_at = datetime.now()

                if attendance_update.staff_status == "saved":
                    existing_store.saved_by = role_code
                    existing_store.saved_at = datetime.now()
                    app_logger.debug(f"Set saved_by to {role_code}")
                elif attendance_update.staff_status == "submitted":
                    existing_store.submit_by = role_code
                    existing_store.submit_at = datetime.now()
                    app_logger.debug(f"Set submit_by to {role_code}")
                app_logger.debug(f"Store status updated successfully")
            else:
                app_logger.warning(
                    f"No CommissionStoreModel record found for store {store_code}, fiscal_month {fiscal_month}")

            # 提交更改
            app_logger.info("Committing changes to database")
            await db.commit()
            app_logger.info("Successfully committed changes")
            # 返回更新结果
            return True

        except Exception as e:
            # 发生异常时回滚事务
            app_logger.error(f"Error in update_commission: {str(e)}", exc_info=True)
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
                    .where(CommissionStoreModel.fiscal_month == fiscal_month,
                           CommissionStoreModel.merged_flag == 0)
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
                'amount_operational': 0.0,
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
                elif rule_class == 'operational':
                    commission_dict[store_code]['amount_operational'] += amount

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
                "amount_team": {"en": "Team Amount", "zh": "团队分摊"},
                "amount_operational": {"en": "Operational Bonus", "zh": "运营奖金"},
                "amount_incentive": {"en": "Incentive Amount", "zh": "激励奖金"},
                "amount_adjustment": {"en": "Adjustment Amount", "zh": "调整奖金"}
            }

            month_end_value = await CommissionUtil.get_month_end_value(db, fiscal_month)

            return {"data": formatted_commissions,
                    "status_counts": status_count_dict,
                    "field_translations": field_translations,
                    "MonthEnd": month_end_value,
                    "fiscal_month": fiscal_month}

        except Exception as e:
            # 记录异常信息（在实际应用中应该使用日志记录器）
            app_logger.error(f"Error in get_all_commissions_by_key: {str(e)}")
            # 抛出异常以便上层处理
            raise e

    @staticmethod
    async def get_commission_by_store_code(db: AsyncSession, store_code: str, fiscal_month: str):
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
    def calculate_discount_factor(achievement_rate: float) -> float:
        """
        根据达成率计算折扣因子

        Args:
            achievement_rate: 达成率

        Returns:
            float: 折扣因子
        """
        if achievement_rate < 80:
            return 0.75
        elif achievement_rate < 100:
            return 0.85
        return 1.0

    @staticmethod
    async def process_position_attendance_stats(staff_attendances: list) -> Dict[str, Dict[str, float]]:
        """
        处理各岗位的出勤统计数据

        Args:
            staff_attendances: 员工出勤数据列表

        Returns:
            Dict: 岗位出勤统计数据
        """
        app_logger.debug(f"开始处理岗位出勤统计数据，员工数量: {len(staff_attendances)}")
        position_stats = {}

        for idx, staff in enumerate(staff_attendances):
            position = staff['position']
            actual_attendance = staff.get('actual_attendance', 0)
            target_value = staff['target_value']
            sales_value = staff['sales_value'] or 0

            app_logger.debug(
                f"处理员工[{idx + 1}/{len(staff_attendances)}] - 岗位: {position}, 实际出勤: {actual_attendance}, 目标值: {target_value}, 销售额: {sales_value}")

            # 计算员工达成率
            achievement_rate = 0
            if sales_value is not None and target_value > 0:
                achievement_rate = (sales_value / target_value) * 100
            app_logger.debug(f"员工达成率: {achievement_rate}%")

            # 计算折扣因子
            discount_factor = CommissionService.calculate_discount_factor(achievement_rate)
            app_logger.debug(f"折扣因子: {discount_factor}")

            # 累计各岗位实际出勤(考虑折扣)
            if position not in position_stats:
                position_stats[position] = {
                    'total_attendance': 0,
                    'staff_count': 0
                }
                app_logger.debug(f"新增岗位统计: {position}")

            position_stats[position]['total_attendance'] += actual_attendance * discount_factor
            position_stats[position]['staff_count'] += 1

            app_logger.debug(
                f"岗位 {position} 累计统计 - 总出勤: {position_stats[position]['total_attendance']}, 员工数: {position_stats[position]['staff_count']}")

        app_logger.debug(f"岗位出勤统计处理完成，共处理 {len(position_stats)} 个岗位: {list(position_stats.keys())}")
        return position_stats

    @staticmethod
    def apply_attendance_adjustment(commission_amount: float, staff: dict,
                                    rule_info, position_stats: dict) -> float:
        """
        应用出勤率调整

        Args:
            commission_amount: 原始佣金金额
            staff: 员工数据
            rule_info: 规则信息
            position_stats: 岗位统计信息

        Returns:
            float: 调整后的佣金金额
        """
        app_logger.debug(f"开始应用出勤调整，员工: {staff.get('staff_code', 'Unknown')}, 原始佣金金额: {commission_amount}")

        expected_attendance = staff['expected_attendance'] or 0
        actual_attendance = staff['actual_attendance'] or 0

        app_logger.debug(
            f"员工 {staff.get('staff_code', 'Unknown')} 出勤信息 - 应出勤: {expected_attendance}, 实际出勤: {actual_attendance}")

        if expected_attendance <= 0:
            app_logger.debug(f"员工 {staff.get('staff_code', 'Unknown')} 应出勤为0或负数，不发放佣金")
            return 0  # 应出勤为0，不发放佣金

        if not rule_info.consider_attendance or actual_attendance == 0:
            app_logger.debug(f"规则 {getattr(rule_info, 'rule_code', 'Unknown')} 不考虑出勤率，返回原始佣金金额: {commission_amount}")
            return commission_amount  # 不考虑出勤率

        position = staff['position']
        position_stat = position_stats.get(position, {})

        app_logger.debug(
            f"员工 {staff.get('staff_code', 'Unknown')} 岗位: {position}, 岗位统计信息: {position_stat},consider_attendance: {rule_info.consider_attendance}")

        if rule_info.consider_attendance == 1:
            # 团队分摊模式
            app_logger.debug(f"规则 {getattr(rule_info, 'rule_code', 'Unknown')} 使用团队分摊模式")
            total_attendance = position_stat.get('total_attendance', 0)
            if total_attendance > 0:
                # 重新计算当前员工的折扣因子
                target_value = staff['target_value']
                sales_value = staff['sales_value'] or 0
                achievement_rate = 0
                if sales_value is not None and target_value > 0:
                    achievement_rate = (sales_value / target_value) * 100
                discount_factor = CommissionService.calculate_discount_factor(achievement_rate)

                app_logger.debug(
                    f"员工 {staff.get('staff_code', 'Unknown')} 达成率: {achievement_rate}%, 折扣因子: {discount_factor}")

                attendance_factor = (actual_attendance * discount_factor) / total_attendance
                adjusted_amount = commission_amount * attendance_factor
                app_logger.debug(
                    f"团队分摊模式调整 - 总岗位出勤: {total_attendance}, 出勤因子: {attendance_factor}, 调整后金额: {adjusted_amount}")
                return adjusted_amount
            else:
                app_logger.debug(f"岗位 {position} 总出勤为0，无法进行团队分摊计算")
        elif rule_info.consider_attendance == 2:
            # 个人出勤模式
            app_logger.debug(f"规则 {getattr(rule_info, 'rule_code', 'Unknown')} 使用个人出勤模式")
            attendance_factor = actual_attendance / expected_attendance
            adjusted_amount = commission_amount * attendance_factor
            app_logger.debug(f"个人出勤模式调整 - 出勤因子: {attendance_factor}, 调整后金额: {adjusted_amount}")
            return adjusted_amount

        app_logger.debug(f"未满足任何调整条件，返回原始佣金金额: {commission_amount}")
        return commission_amount

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

            commission_store_result = await db.execute(
                select(CommissionStoreModel.merged_store_codes, CommissionStoreModel.merged_flag,
                       CommissionStoreModel.store_type, CommissionStoreModel.fiscal_period)
                    .where(
                    CommissionStoreModel.store_code == store_code,
                    CommissionStoreModel.fiscal_month == fiscal_month
                )
            )
            commission_store_record = commission_store_result.fetchone()

            if commission_store_record and commission_store_record.merged_store_codes:
                merged_code = commission_store_record.merged_store_codes.split(',')
                merged_codes = [code.strip() for code in merged_code]
            else:
                merged_codes = [store_code]

            if fiscal_month != commission_store_record.fiscal_period:
                month_code = commission_store_record.fiscal_period.split(',')
                month_codes = [code.strip() for code in month_code]
            else:
                month_codes = [fiscal_month]

            app_logger.info(f"开始为店铺 {merged_codes} 在财月 {month_codes} 计算佣金")
            # 1. 获取店铺类型和数据
            store_result = await db.execute(
                select(
                    TargetStoreMain.store_type,
                    TargetStoreMain.target_value,
                    TargetStoreMain.sales_value,
                ).where(
                    TargetStoreMain.store_code.in_(merged_codes),
                    TargetStoreMain.fiscal_month.in_(month_codes)
                )
            )
            store_data_list = store_result.fetchall()

            if not store_data_list:
                app_logger.warning(f"门店列表 {merged_codes} 在财月 {fiscal_month} 没有找到数据")
                raise ValueError(f"Stores {merged_codes} not found or have no data for {fiscal_month}")

            # 3. 计算合并后的总目标值和销售额
            store_target_value = 0
            store_sales_value = 0

            for store_data in store_data_list:
                store_target_value += store_data.target_value or 0
                store_sales_value += store_data.sales_value or 0

            store_type = commission_store_record.store_type

            app_logger.debug(
                f"店铺 {store_code} -> {merged_codes} 类型: {store_type}, 目标值: {store_target_value}, 销售额: {store_sales_value}")

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
                    StaffAttendanceModel.target_value,
                    StaffAttendanceModel.sales_value,
                    StaffAttendanceModel.fiscal_month
                ).where(
                    StaffAttendanceModel.store_code.in_(merged_codes),
                    StaffAttendanceModel.fiscal_month.in_(month_codes)
                )
            )
            staff_attendances = staff_attendances_result.all()
            app_logger.info(f"找到 {len(staff_attendances)} 名员工需要计算佣金")

            # 3. 删除该店铺该财月的所有现有佣金记录
            app_logger.debug(f"删除店铺 {store_code} 在财月 {month_codes} 的现有佣金记录")
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
                    CommissionRuleModel.consider_attendance,
                    CommissionRuleModel.minimum_guarantee_on_attendance
                ).where(
                    CommissionRuleModel.rule_code.in_(all_rule_codes)
                )
            )
            rules_info = {
                row.rule_code: row
                for row in rules_result.fetchall()
            }

            app_logger.debug(f"获取到的规则信息: {rules_info}")

            # 预处理员工数据，合并同一员工在不同月份的数据
            processed_staff_attendances = {}
            for staff in staff_attendances:
                staff_code = staff.staff_code

                if staff_code not in processed_staff_attendances:
                    # 初始化员工数据，使用第一个遇到的记录作为基础
                    processed_staff_attendances[staff_code] = {
                        'staff_code': staff.staff_code,
                        'position': staff.position,
                        'actual_attendance': staff.actual_attendance or 0,
                        'expected_attendance': staff.expected_attendance or 0,
                        'salary_coefficient': staff.salary_coefficient,
                        'target_value_ratio': staff.target_value_ratio,
                        'target_value': staff.target_value or 0,
                        'sales_value': staff.sales_value or 0,
                        'fiscal_months': [staff.fiscal_month]
                    }
                else:
                    # 累加数值型字段
                    existing_staff = processed_staff_attendances[staff_code]
                    existing_staff['actual_attendance'] += staff.actual_attendance or 0
                    existing_staff['expected_attendance'] += staff.expected_attendance or 0
                    existing_staff['sales_value'] += staff.sales_value or 0
                    existing_staff['target_value'] += staff.target_value or 0
                    existing_staff['fiscal_months'].append(staff.fiscal_month)

            # 转换为列表格式
            staff_attendances = list(processed_staff_attendances.values())
            app_logger.info(f"预处理后有 {len(staff_attendances)} 名唯一员工需要计算佣金")

            # 处理岗位出勤统计数据
            position_stats = await CommissionService.process_position_attendance_stats(staff_attendances)
            app_logger.debug(f"岗位出勤统计数据: {position_stats}")

            commission_records = []

            # 7. 为每个员工计算佣金（应用所有适用的规则）
            for staff in staff_attendances:
                app_logger.debug(f"正在为员工 {staff['staff_code']} 计算佣金")

                # 检查员工目标值
                staff_target_value = staff['target_value']
                app_logger.debug(f"员工 {staff['staff_code']} 目标值: {staff_target_value}")

                # 计算员工达成率
                staff_sales_value = staff['sales_value'] or 0
                if staff_sales_value is not None and staff_target_value > 0:
                    staff_achievement_rate = (staff_sales_value / staff_target_value) * 100
                else:
                    staff_achievement_rate = 0

                app_logger.debug(f"员工 {staff['staff_code']} 达成率: {staff_achievement_rate}%")

                # 获取适用于该岗位的所有规则代码
                rule_codes = position_to_rules.get(staff['position'], [])
                app_logger.debug(f"员工 {staff['staff_code']} 岗位 {staff['position']} 适用的规则: {rule_codes}")

                if not rule_codes:
                    app_logger.warning(f"员工 {staff['staff_code']} 没有适用的规则，跳过计算")
                    continue

                # 为每个适用的规则计算佣金
                for rule_code in rule_codes:
                    app_logger.debug(f"正在应用规则 {rule_code} 计算员工 {staff['staff_code']} 的佣金")

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
                    if rule_info.consider_attendance and rule_info.consider_attendance > 0:
                        commission_amount = CommissionService.apply_attendance_adjustment(
                            commission_amount, staff, rule_info, position_stats
                        )
                        app_logger.debug(f"应用出勤调整后金额: {commission_amount}")
                    else:
                        app_logger.debug(f"未考虑出勤率")

                    # 最低保障金额
                    if rule_info.minimum_guarantee and commission_amount < rule_info.minimum_guarantee:
                        old_amount = commission_amount
                        commission_amount = rule_info.minimum_guarantee
                        app_logger.debug(f"应用保底金额: 原始金额 {old_amount} < 保底金额 {rule_info.minimum_guarantee}, 调整为保底金额")

                        expected_attendance = staff['expected_attendance'] or 0
                        actual_attendance = staff['actual_attendance'] or 0

                        if rule_info.minimum_guarantee_on_attendance == 1 and expected_attendance > 0:
                            attendance_factor = actual_attendance / expected_attendance
                            commission_amount = commission_amount * attendance_factor
                            app_logger.debug(
                                f"保底金额考虑出勤比例: 保底金额 {rule_info.minimum_guarantee} * 出勤率 {attendance_factor} = 调整后金额 {commission_amount}")
                        elif rule_info.minimum_guarantee_on_attendance > 1 and expected_attendance > 0:
                            attendance_percentage = (actual_attendance / expected_attendance) * 100
                            if attendance_percentage < rule_info.minimum_guarantee_on_attendance:
                                commission_amount = 0
                                app_logger.debug(
                                    f"出勤率 {attendance_percentage}% 低于要求的 {rule_info.minimum_guarantee_on_attendance}%, 保底金额取消")
                            else:
                                app_logger.debug(
                                    f"出勤率 {attendance_percentage}% 满足要求的 {rule_info.minimum_guarantee_on_attendance}%, 保底金额保持 {commission_amount}")
                        else:
                            app_logger.debug(f"保底金额不考虑出勤或应出勤为0, 最终金额 {commission_amount}")

                    # 只有佣金金额大于0时才保存

                    if commission_amount and commission_amount > 0:
                        position_stat = position_stats.get(staff['position'], {})
                        app_logger.debug(f"为员工 {staff['staff_code']} 创建佣金记录: {commission_amount}")
                        commission_amount = round(commission_amount, -1)
                        commission_record = CommissionStaffModel(
                            fiscal_month=fiscal_month,
                            staff_code=staff['staff_code'],
                            store_code=store_code,
                            amount=commission_amount,
                            rule_detail_code=matching_detail.rule_detail_code,
                            total_days_store_work=position_stat.get('total_attendance', 0)
                        )
                        commission_records.append(commission_record)
                    else:
                        app_logger.debug(f"员工 {staff['staff_code']} 计算的佣金金额为0或负数，跳过记录")

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
