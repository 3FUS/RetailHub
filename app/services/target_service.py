from collections import defaultdict

from sqlalchemy import String, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.models.commission import CommissionStoreModel
from app.models.target import TargetStoreMain, TargetStoreWeek, TargetStoreDaily
from app.models.staff import StaffAttendanceModel, StaffModel
from app.schemas.target import TargetStoreUpdate, \
    TargetStoreWeekCreate, \
    TargetStoreDailyCreate, StaffAttendanceCreate, BatchApprovedTarget
from app.models.dimension import DimensionDayWeek, StoreModel

from datetime import datetime

# 在 TargetRPTService 类中修改 get_rpt_target_by_store 方法
from app.utils.permissions import build_store_permission_query
from app.utils.logger import app_logger


class TargetRPTService:
    @staticmethod
    async def get_rpt_target_by_store(db: AsyncSession, fiscal_month: str, key_word: str):
        """
        获取门店目标报表数据

        Args:
            db: 数据库会话
            fiscal_month: 财月
            key_word: 查询关键字（门店代码或名称）

        Returns:
            dict: 报表数据
        """

        try:
            # 执行SQL查询逻辑
            query = select(
                TargetStoreDaily.target_date.label('date'),
                TargetStoreMain.fiscal_month,
                (DimensionDayWeek.finance_year + DimensionDayWeek.week_number.cast(String)).label('fiscal_week'),
                TargetStoreMain.store_code,
                StoreModel.store_name,
                (TargetStoreMain.target_value * TargetStoreDaily.monthly_percentage / 100).label('target_date_value')
            ).select_from(
                TargetStoreMain.__table__.join(
                    TargetStoreDaily.__table__,
                    (TargetStoreMain.store_code == TargetStoreDaily.store_code) &
                    (TargetStoreMain.fiscal_month == TargetStoreDaily.fiscal_month)
                ).join(
                    StoreModel.__table__,
                    StoreModel.store_code == TargetStoreMain.store_code
                ).join(
                    DimensionDayWeek.__table__,
                    DimensionDayWeek.actual_date == TargetStoreDaily.target_date
                )
            ).where(
                TargetStoreMain.fiscal_month == fiscal_month
            ).order_by(TargetStoreDaily.target_date, TargetStoreMain.store_code)

            # 如果有关键字过滤条件
            if key_word:
                query = query.where(
                    TargetStoreMain.store_code.contains(key_word) |
                    StoreModel.store_name.contains(key_word)
                )

            result = await db.execute(query)

            target_data = result.all()

            # 构建结果数据
            formatted_data = []
            for row in target_data:
                formatted_data.append({
                    "date": row.date.strftime('%Y%m%d') if row.date else None,
                    "fiscal_month": row.fiscal_month,
                    "fiscal_week": row.fiscal_week,
                    "store_code": row.store_code,
                    "store_name": row.store_name,
                    "target_date_value": float(row.target_date_value) if row.target_date_value is not None else 0.0
                })

            field_translations = {
                "date": {"en": "Date (Number)", "zh": "日期"},
                "fiscal_month": {"en": "Fiscal week (ID)", "zh": "财周"},
                "fiscal_week": {"en": "Fiscal Month (ID)", "zh": "财月"},
                "store_code": {"en": "Location Code", "zh": "店铺代码"},
                "store_name": {"en": "Location short Name", "zh": "店铺名称"},
                "target_date_value": {"en": "Commission Target Local", "zh": "日期目标值"}
            }

            return {
                "data": formatted_data,
                "field_translations": field_translations
            }
        except Exception as e:
            # 记录并返回错误信息
            error_msg = f"Error in get_rpt_target_by_store: {str(e)}"
            # print(error_msg)  # 在实际应用中应该使用日志记录
            app_logger.error(error_msg)
            # 添加字段名称的中英文翻译

            return {
                "data": [],
                "field_translations": [],
                "error": error_msg
            }

    @staticmethod
    async def get_rpt_target_by_staff(db: AsyncSession, fiscal_month: str, key_word: str = None):
        """
        获取员工目标报表数据

        Args:
            db: 数据库会话
            fiscal_month: 财月
            key_word: 查询关键字（门店代码或名称）

        Returns:
            dict: 报表数据
        """
        try:
            # 执行SQL查询逻辑
            query = select(
                TargetStoreMain.fiscal_month,
                TargetStoreMain.store_code,
                StoreModel.store_name,
                StaffAttendanceModel.staff_code,
                (TargetStoreMain.target_value * StaffAttendanceModel.target_value_ratio).label('target_value')
            ).select_from(
                TargetStoreMain.__table__.join(
                    StaffAttendanceModel.__table__,
                    (TargetStoreMain.store_code == StaffAttendanceModel.store_code) &
                    (TargetStoreMain.fiscal_month == StaffAttendanceModel.fiscal_month)
                ).join(
                    StoreModel.__table__,
                    StoreModel.store_code == TargetStoreMain.store_code
                )
            ).where(
                TargetStoreMain.fiscal_month == fiscal_month
            ).order_by(TargetStoreMain.store_code, StaffAttendanceModel.staff_code)

            # 如果有关键字过滤条件
            if key_word:
                query = query.where(
                    TargetStoreMain.store_code.contains(key_word) |
                    StoreModel.store_name.contains(key_word)
                )

            result = await db.execute(query)
            target_data = result.all()

            # 构建结果数据
            formatted_data = []
            for row in target_data:
                formatted_data.append({
                    "fiscal_month": row.fiscal_month,
                    "store_code": row.store_code,
                    "store_name": row.store_name,
                    "staff_code": row.staff_code,
                    "target_value": round(float(row.target_value), 2) if row.target_value is not None else 0.0
                })

            field_translations = {
                "fiscal_month": {"en": "Fiscal Month (ID)", "zh": "财月"},
                "store_code": {"en": "Location Code", "zh": "店铺代码"},
                "store_name": {"en": "Location Short Name", "zh": "店铺名称"},
                "staff_code": {"en": "Associate Number", "zh": "员工代码"},
                "target_value": {"en": "Commission Target Local", "zh": "目标值"}
            }

            return {
                "data": formatted_data,
                "field_translations": field_translations
            }
        except Exception as e:
            # 记录并返回错误信息
            error_msg = f"Error in get_rpt_target_by_staff: {str(e)}"
            # print(error_msg)  # 在实际应用中应该使用日志记录
            app_logger.error(error_msg)
            return {
                "data": [],
                "field_translations": [],
                "error": error_msg
            }


class TargetStoreService:
    # @staticmethod
    # async def create_target_store(db: AsyncSession, target_data: TargetStoreCreate):
    #     target_store = TargetStoreMain(**target_data.dict())
    #     db.add(target_store)
    #     await db.commit()
    #     await db.refresh(target_store)
    #     return target_store

    # 在 TargetStoreService 类中添加以下方法
    @staticmethod
    async def batch_update_target_value(db: AsyncSession, target_updates: list):
        """
        批量更新 TargetStoreMain 的 target_value

        Args:
            db: 数据库会话
            target_updates: 包含 store_code, fiscal_month, target_value 的字典列表
        """
        updated_targets = []

        for update_data in target_updates:
            store_code = update_data.get('store_code')
            fiscal_month = update_data.get('fiscal_month')
            target_value = update_data.get('target_value')

            # 查询现有记录
            result = await db.execute(select(TargetStoreMain).where(
                TargetStoreMain.store_code == store_code,
                TargetStoreMain.fiscal_month == fiscal_month
            ))
            target_store = result.scalar_one_or_none()

            if target_store:
                # 更新现有记录
                target_store.target_value = target_value
                target_store.updated_at = datetime.utcnow()
                updated_targets.append(target_store)
            else:
                # 创建新记录
                store_type = None
                result_store = await db.execute(
                    select(StoreModel.store_type)
                        .where(StoreModel.store_code == store_code)
                )
                store_data = result_store.fetchone()
                if store_data:
                    store_type = store_data.store_type

                new_target = TargetStoreMain(
                    store_code=store_code,
                    fiscal_month=fiscal_month,
                    target_value=target_value,
                    store_type=store_type
                )
                db.add(new_target)
                updated_targets.append(new_target)

        await db.commit()

        # 刷新所有对象以获取数据库生成的值
        for target in updated_targets:
            await db.refresh(target)

        return updated_targets

    @staticmethod
    async def update_target_store(db: AsyncSession, store_code: str, fiscal_month: str,
                                  target_data: TargetStoreUpdate):
        result = await db.execute(select(TargetStoreMain).where(
            TargetStoreMain.store_code == store_code,
            TargetStoreMain.fiscal_month == fiscal_month))
        target_store = result.scalar_one_or_none()

        if not target_store:

            store_type = None
            result_store = await db.execute(
                select(StoreModel.store_code, StoreModel.store_name, StoreModel.store_type)
                    .where(StoreModel.store_code == store_code)
            )
            store_data = result_store.fetchone()
            if store_data:
                store_type = store_data.store_type

            target_store_data = target_data.dict()
            target_store_data['store_code'] = store_code
            target_store_data['fiscal_month'] = fiscal_month
            target_store_data['store_type'] = store_type
            target_store = TargetStoreMain(**target_store_data)
            db.add(target_store)
            await db.commit()
            await db.refresh(target_store)
            return target_store

        for key, value in target_data.dict(exclude_unset=True).items():
            setattr(target_store, key, value)

        target_store.updated_at = datetime.utcnow()
        await db.commit()
        await db.refresh(target_store)
        return target_store

    @staticmethod
    async def get_all_target_stores_by_key(role_code: str, fiscal_month: str, key_word: str, db: AsyncSession):

        store_permission_query = build_store_permission_query(role_code)
        store_alias = store_permission_query.subquery()

        query = select(
            store_alias.c.store_code,
            store_alias.c.store_name,
            store_alias.c.store_type,
            TargetStoreMain.target_value,
            TargetStoreMain.store_status,
            TargetStoreMain.staff_status
        ).select_from(
            store_alias.join(
                TargetStoreMain.__table__,
                (store_alias.c.store_code == TargetStoreMain.store_code) &
                (TargetStoreMain.fiscal_month == fiscal_month),
                isouter=True
            )
        )

        if key_word:
            query = query.where(
                store_alias.c.store_code.contains(key_word) |
                store_alias.c.store_name.contains(key_word)
            )

        result = await db.execute(query)
        target_stores = result.all()

        formatted_data = [
            {
                "store_code": row.store_code,
                "store_name": row.store_name,
                "store_type": row.store_type,
                "target_value": row.target_value if row.target_value is not None else None,
                "store_status": row.store_status,
                "staff_status": row.staff_status
            }
            for row in target_stores
        ]

        # 添加字段名称的中英文翻译
        field_translations = {
            "store_code": {"en": "Store Code", "zh": "店铺代码"},
            "store_name": {"en": "Store Name", "zh": "店铺名称"},
            "store_type": {"en": "Store Type", "zh": "店铺类型"},
            "target_value": {"en": "Target Value", "zh": "目标值"},
            "store_status": {"en": "Store Status", "zh": "店铺目标"},
            "staff_status": {"en": "Staff Status", "zh": "员工目标"}
        }

        return {
            "data": formatted_data,
            "field_translations": field_translations
        }

    @staticmethod
    async def batch_approved_target_by_store_codes(db: AsyncSession, request: BatchApprovedTarget) -> bool:
        try:
            # 查询匹配的commission记录
            fiscal_month = request.fiscal_month
            staff_status = request.staff_status
            store_status = request.store_status
            store_codes = request.store_codes

            result = await db.execute(
                select(TargetStoreMain)
                    .where(TargetStoreMain.fiscal_month == fiscal_month)
                    .where(TargetStoreMain.store_code.in_(store_codes))
            )

            targets = result.scalars().all()

            for t in targets:
                if staff_status is not None:
                    t.staff_status = staff_status
                if store_status is not None:
                    t.store_status = store_status
                t.updated_at = datetime.now()
            await db.commit()

            return True

        except Exception as e:
            app_logger.error(f"batch_approved_target_by_store_codes {e}")
            await db.rollback()
            raise e

    @staticmethod
    async def withdrawn_target(db: AsyncSession, request: BatchApprovedTarget) -> bool:

        fiscal_month = request.fiscal_month
        store_code = request.store_code
        staff_status = request.staff_status
        store_status = request.store_status

        if staff_status is not None:
            result = await db.execute(
                select(TargetStoreMain)
                    .where(TargetStoreMain.fiscal_month == fiscal_month)
                    .where(TargetStoreMain.store_code == store_code)
                    .where(TargetStoreMain.staff_status == 'submitted')
            )

        if store_status is not None:
            result = await db.execute(
                select(TargetStoreMain)
                    .where(TargetStoreMain.fiscal_month == fiscal_month)
                    .where(TargetStoreMain.store_code == store_code)
                    .where(TargetStoreMain.store_status == 'submitted')
            )

        existing_target = result.scalar_one_or_none()
        if existing_target:
            if staff_status is not None:
                existing_target.staff_status = 'saved'
            if store_status is not None:
                existing_target.store_status = 'saved'
            existing_target.updated_at = datetime.now()
            await db.commit()
            return True
        return False


class TargetStoreWeekService:
    @staticmethod
    async def get_target_store_week(db: AsyncSession, store_code: str, fiscal_month: str):

        if fiscal_month:
            try:
                year, month = fiscal_month.split('-')
                last_year = int(year) - 1
                fiscal_month_ly = f"{last_year}-{month}"
            except (ValueError, IndexError):
                fiscal_month_ly = None

        result_current = await db.execute(
            select(
                TargetStoreWeek.week_number,
                TargetStoreWeek.percentage,
                TargetStoreWeek.target_value
            ).where(
                TargetStoreWeek.store_code == store_code,
                TargetStoreWeek.fiscal_month == fiscal_month
            )
        )
        current_weeks = result_current.all()

        result_last = await db.execute(
            select(
                TargetStoreWeek.week_number,
                TargetStoreWeek.percentage,
                TargetStoreWeek.target_value
            ).where(
                TargetStoreWeek.store_code == store_code,
                TargetStoreWeek.fiscal_month == fiscal_month_ly
            )
        )
        last_weeks = result_last.all()
        current_list = [
            {
                "week_number": row.week_number if row.week_number is not None else None,
                "percentage": float(row.percentage) if row.percentage is not None else None,
                "target_value": float(row.target_value) if row.target_value is not None else None
            }
            for row in current_weeks
        ]

        last_list = [
            {
                "week_number": row.week_number if row.week_number is not None else None,
                "percentage": float(row.percentage) if row.percentage is not None else None,
                "target_value": float(row.target_value) if row.target_value is not None else None
            }
            for row in last_weeks
        ]
        return {
            "current_year": current_list,
            "last_year": last_list
        }

    @staticmethod
    async def create_target_store_week(db: AsyncSession, target_data: TargetStoreWeekCreate):
        created_targets = []

        # 遍历所有传入的数据
        for week_data in target_data.weeks:
            # 检查记录是否已存在
            result = await db.execute(select(TargetStoreWeek).where(
                TargetStoreWeek.store_code == target_data.store_code,
                TargetStoreWeek.fiscal_month == target_data.fiscal_month,
                TargetStoreWeek.week_number == week_data.week_number
            ))
            existing_target = result.scalar_one_or_none()

            if existing_target:
                # 如果存在，更新记录
                for key, value in week_data.dict().items():
                    if key not in ['store_code', 'fiscal_month', 'week_number']:  # 不更新主键
                        setattr(existing_target, key, value)
                existing_target.updated_at = datetime.utcnow()
                created_targets.append(existing_target)
            else:
                # 如果不存在，创建新记录
                target_store_week = TargetStoreWeek(
                    store_code=target_data.store_code,
                    fiscal_month=target_data.fiscal_month,
                    week_number=week_data.week_number,
                    percentage=week_data.percentage,
                    creator_code=target_data.creator_code
                )
                db.add(target_store_week)
                created_targets.append(target_store_week)

        await db.commit()

        # 刷新所有对象以获取数据库生成的值
        for target in created_targets:
            await db.refresh(target)

        return created_targets


class TargetStoreDailyService:
    @staticmethod
    async def get_target_store_daily(db: AsyncSession, store_code: str, fiscal_month: str):
        if fiscal_month:
            try:
                year, month = fiscal_month.split('-')
                last_year = int(year) - 1
                fiscal_month_ly = f"{last_year}-{month}"
            except (ValueError, IndexError):
                fiscal_month_ly = None
        target_last_year = TargetStoreDaily.__table__.alias('target_last_year')
        result = await db.execute(
            select(
                DimensionDayWeek.day_number,
                DimensionDayWeek.week_number,
                DimensionDayWeek.actual_date,
                TargetStoreDaily.percentage,
                TargetStoreDaily.target_value,
                target_last_year.c.percentage.label('percentage_ly'),
                target_last_year.c.target_value.label('target_value_ly')
            )
                .select_from(
                DimensionDayWeek.__table__.join(
                    TargetStoreDaily.__table__,
                    (DimensionDayWeek.fiscal_month == TargetStoreDaily.fiscal_month) &
                    (DimensionDayWeek.actual_date == TargetStoreDaily.target_date) &
                    (TargetStoreDaily.store_code == store_code),
                    isouter=True
                ).join(
                    target_last_year,
                    (DimensionDayWeek.actual_date_ly == target_last_year.c.target_date) &
                    (target_last_year.c.store_code == store_code) &
                    (target_last_year.c.fiscal_month == fiscal_month_ly),
                    isouter=True
                )
            )
                .where(
                DimensionDayWeek.fiscal_month == fiscal_month
            )
        )
        target_daily = result.all()

        return [
            {
                "week_number": row.week_number if row.week_number is not None else None,
                "actual_date": row.actual_date.strftime('%Y-%m-%d') if row.actual_date else None,
                "percentage": float(row.percentage) if row.percentage is not None else None,
                "target_value": float(row.target_value) if row.target_value is not None else None,
                "percentage_ly": float(row.percentage_ly) if row.percentage_ly is not None else None,
                "target_value_ly": float(row.target_value_ly) if row.target_value_ly is not None else None
            }
            for row in target_daily
        ]

    @staticmethod
    async def update_target_monthly_percentage(db: AsyncSession, store_code: str, fiscal_month: str):
        """
        更新门店日目标数据的 monthly_percentage 字段

        Args:
            db: 数据库会话
            store_code: 门店代码
            fiscal_month: 财务月份

        Returns:
            list: 更新的记录列表
        """
        # 获取该门店该财月的所有日目标数据
        result = await db.execute(select(TargetStoreDaily).where(
            TargetStoreDaily.store_code == store_code,
            TargetStoreDaily.fiscal_month == fiscal_month
        ))
        target_store_dailies = result.scalars().all()

        if not target_store_dailies:
            return []

        updated_targets = []

        for target_store_daily in target_store_dailies:
            target_date = target_store_daily.target_date
            daily_percentage = target_store_daily.percentage

            # 获取日期对应的周数
            day_week_result = await db.execute(
                select(DimensionDayWeek.week_number)
                    .where(
                    DimensionDayWeek.fiscal_month == fiscal_month,
                    DimensionDayWeek.actual_date == target_date
                )
            )
            day_week_data = day_week_result.fetchone()

            monthly_percentage = 0
            if day_week_data:
                week_number = day_week_data.week_number

                # 获取该周的百分比
                week_result = await db.execute(
                    select(TargetStoreWeek.percentage)
                        .where(
                        TargetStoreWeek.store_code == store_code,
                        TargetStoreWeek.fiscal_month == fiscal_month,
                        TargetStoreWeek.week_number == week_number
                    )
                )
                week_data = week_result.fetchone()

                if week_data:
                    weekly_percentage = week_data.percentage
                    # 计算每月百分比 = 每日百分比 * 每周百分比 / 100
                    monthly_percentage = daily_percentage * weekly_percentage / 100 if daily_percentage else 0

            # 更新 monthly_percentage 字段
            target_store_daily.monthly_percentage = monthly_percentage
            target_store_daily.updated_at = datetime.utcnow()
            updated_targets.append(target_store_daily)

        await db.commit()

        # 刷新所有对象以获取数据库生成的值
        for target in updated_targets:
            await db.refresh(target)

        return updated_targets

    @staticmethod
    async def create_target_store_daily(db: AsyncSession,
                                        target_data: TargetStoreDailyCreate):

        created_targets = []
        for day_data in target_data.days:

            result = await db.execute(select(TargetStoreDaily).where(
                TargetStoreDaily.store_code == target_data.store_code,
                TargetStoreDaily.fiscal_month == target_data.fiscal_month,
                TargetStoreDaily.target_date == day_data.target_date
            ))
            existing_target = result.scalar_one_or_none()
            if existing_target:
                # 如果存在，更新记录
                for key, value in day_data.dict().items():
                    if key not in ['store_code', 'fiscal_month', 'target_date']:  # 不更新主键
                        setattr(existing_target, key, value)
                existing_target.updated_at = datetime.utcnow()
                created_targets.append(existing_target)
            else:
                target_store_daily = TargetStoreDaily(
                    store_code=target_data.store_code,
                    fiscal_month=target_data.fiscal_month,
                    target_date=day_data.target_date,
                    percentage=day_data.percentage,
                    creator_code=target_data.creator_code
                )
                db.add(target_store_daily)
                created_targets.append(target_store_daily)

        await db.commit()

        target_store_update = TargetStoreUpdate(
            store_code=target_data.store_code,
            fiscal_month=target_data.fiscal_month,
            store_status=target_data.store_status,
            creator_code=target_data.creator_code
        )
        await TargetStoreService.update_target_store(db, target_data.store_code, target_data.fiscal_month,
                                                     target_store_update)
        # 刷新所有创建的对象以获取数据库生成的值
        for target in created_targets:
            await db.refresh(target)

        return created_targets

    @staticmethod
    async def update_target_store_daily(db: AsyncSession, store_code: str, fiscal_month: str, target_date: datetime,
                                        target_data: TargetStoreDailyCreate):
        result = await db.execute(select(TargetStoreDaily).where(
            TargetStoreDaily.store_code == store_code,
            TargetStoreDaily.fiscal_month == fiscal_month,
            TargetStoreDaily.target_date == target_date))
        target_store_daily = result.scalar_one_or_none()

        if not target_store_daily:
            raise ValueError("Target store daily not found")

        for key, value in target_data.dict(exclude_unset=True).items():
            setattr(target_store_daily, key, value)

        target_store_daily.updated_at = datetime.utcnow()
        await db.commit()
        await db.refresh(target_store_daily)
        return target_store_daily


class TargetStaffService:
    @staticmethod
    async def get_staff_attendance(db: AsyncSession, fiscal_month: str, store_code: str):
        try:
            result_store = await db.execute(
                select(
                    TargetStoreMain.target_value,
                    TargetStoreMain.sales_value,
                    TargetStoreMain.store_status,
                    TargetStoreMain.staff_status,
                    CommissionStoreModel.status.label('commission_status')  # 获取佣金状态
                )
                    .select_from(
                    TargetStoreMain.__table__.join(
                        CommissionStoreModel.__table__,
                        (TargetStoreMain.store_code == CommissionStoreModel.store_code) &
                        (TargetStoreMain.fiscal_month == CommissionStoreModel.fiscal_month),
                        isouter=True  # left join
                    )
                )
                    .where(
                    TargetStoreMain.fiscal_month == fiscal_month,
                    TargetStoreMain.store_code == store_code
                )
            )
            store_target_record = result_store.fetchone()

            store_target_value = float(
                store_target_record.target_value) if store_target_record and store_target_record.target_value is not None else 0.0

            store_sales_value = float(
                store_target_record.sales_value) if store_target_record and store_target_record.sales_value is not None else 0.0

            staff_status = store_target_record.staff_status
            store_status = store_target_record.store_status
            commission_status = store_target_record.commission_status

            # 获取财月的日期范围
            date_range_result = await db.execute(
                select(
                    func.min(DimensionDayWeek.actual_date).label('min_date'),
                    func.max(DimensionDayWeek.actual_date).label('max_date')
                )
                    .where(DimensionDayWeek.fiscal_month == fiscal_month)
            )
            date_range = date_range_result.fetchone()

            fiscal_period = ""
            if date_range and date_range.min_date and date_range.max_date:
                fiscal_period = f"{date_range.min_date.strftime('%Y-%m-%d')} to {date_range.max_date.strftime('%Y-%m-%d')}"

            result = await db.execute(
                select(
                    StaffModel.avatar,
                    StaffModel.staff_code,
                    StaffModel.first_name,
                    StaffModel.position.label('staff_position'),  # 从StaffModel获取position
                    StaffModel.salary_coefficient.label('staff_salary_coefficient'),  # 从StaffModel获取salary_coefficient
                    StaffAttendanceModel.expected_attendance,
                    StaffAttendanceModel.actual_attendance,
                    StaffAttendanceModel.position.label('attendance_position'),  # 从StaffAttendanceModel获取position
                    StaffAttendanceModel.salary_coefficient.label('attendance_salary_coefficient'),
                    # 从StaffAttendanceModel获取salary_coefficient
                    StaffAttendanceModel.target_value_ratio,
                    StaffAttendanceModel.sales_value,
                    StaffAttendanceModel.deletable
                )
                    .select_from(
                    StaffModel.__table__.join(
                        StaffAttendanceModel.__table__,
                        (StaffModel.staff_code == StaffAttendanceModel.staff_code) &
                        (StaffAttendanceModel.fiscal_month == fiscal_month),
                        isouter=True
                    )
                )
                    .where(
                    StaffModel.store_code == store_code,
                    StaffModel.avatar.isnot(None)
                )
            )
            staff_attendance_data = result.all()

            # total_target_value = sum(
            #     float(row.target_value) if row.target_value is not None else 0 for row in staff_attendance_data)

            staff_attendance_list = []
            for row in staff_attendance_data:

                position = row.attendance_position if row.attendance_position is not None else row.staff_position
                salary_coefficient = (
                    row.attendance_salary_coefficient
                    if row.attendance_salary_coefficient is not None
                    else row.staff_salary_coefficient
                )

                target_value = 0.0
                if store_target_value > 0 and row.target_value_ratio is not None:
                    target_value = round(store_target_value * row.target_value_ratio, 2)  # 保留两位小数

                achievement_rate = None
                if (row.sales_value is not None and
                        target_value is not None and
                        target_value > 0):
                    achievement_rate = f"{row.sales_value / target_value :.2%}"

                staff_attendance_list.append({
                    "avatar": row.avatar,
                    "staff_code": row.staff_code,
                    "first_name": row.first_name,
                    "expected_attendance": float(
                        row.expected_attendance) if row.expected_attendance is not None else None,
                    "actual_attendance": float(row.actual_attendance) if row.actual_attendance is not None else None,
                    "position": position,
                    "salary_coefficient": float(salary_coefficient) if salary_coefficient is not None else None,
                    "target_value": target_value,
                    "sales_value": float(row.sales_value) if row.sales_value is not None else None,
                    "achievement_rate": achievement_rate,
                    "target_value_ratio": f"{row.target_value_ratio:.2%}" if row.target_value_ratio is not None else None,
                    "deletable": row.deletable
                })

            return {
                "data": staff_attendance_list,
                "header_info": {
                    "store_target_value": store_target_value,
                    "store_sales_value": store_sales_value,
                    "fiscal_period": fiscal_period,  # 新增的财月日期范围
                    "staff_status": staff_status,
                    "store_status": store_status,
                    "commission_status": commission_status
                }
            }

        except Exception as e:
            app_logger.error(
                f"Error in get_staff_attendance: fiscal_month={fiscal_month}, store_code={store_code}, error={str(e)}")
            # 可以选择抛出异常或返回空列表
            # return []  # 或者
            raise e

    @staticmethod
    async def create_staff_attendance(db: AsyncSession, target_data: StaffAttendanceCreate):

        created_staff_targets = []

        total_weight = 0
        for staff_data in target_data.staffs:
            expected_attendance = staff_data.expected_attendance or 0
            salary_coefficient = staff_data.salary_coefficient or 0
            weight = expected_attendance * salary_coefficient
            total_weight += weight

        for staff_data in target_data.staffs:

            result = await db.execute(select(StaffAttendanceModel).where(
                StaffAttendanceModel.staff_code == staff_data.staff_code,
                StaffAttendanceModel.store_code == target_data.store_code,
                StaffAttendanceModel.fiscal_month == target_data.fiscal_month
            ))
            existing_target = result.scalar_one_or_none()

            expected_attendance = staff_data.expected_attendance or 0
            salary_coefficient = staff_data.salary_coefficient or 0
            staff_weight = expected_attendance * salary_coefficient
            target_value_ratio = (staff_weight / total_weight) if total_weight > 0 else 0

            if existing_target:
                # 如果存在，更新记录
                for key, value in staff_data.dict().items():
                    if key not in ['store_code', 'fiscal_month', 'staff_code']:  # 不更新主键
                        setattr(existing_target, key, value)
                existing_target.target_value_ratio = target_value_ratio
                existing_target.updated_at = datetime.utcnow()
                created_staff_targets.append(existing_target)
            else:
                target_staff_attendance = StaffAttendanceModel(
                    staff_code=staff_data.staff_code,
                    store_code=target_data.store_code,
                    fiscal_month=target_data.fiscal_month,
                    expected_attendance=staff_data.expected_attendance,
                    position=staff_data.position,
                    salary_coefficient=staff_data.salary_coefficient,
                    target_value_ratio=target_value_ratio,
                    creator_code=target_data.creator_code
                )
                db.add(target_staff_attendance)
                created_staff_targets.append(target_staff_attendance)

        await db.commit()

        target_store_update = TargetStoreUpdate(
            store_code=target_data.store_code,
            fiscal_month=target_data.fiscal_month,
            staff_status=target_data.staff_status,
            creator_code=target_data.creator_code
        )
        await TargetStoreService.update_target_store(db, target_data.store_code, target_data.fiscal_month,
                                                     target_store_update)
        # 刷新所有创建的对象以获取数据库生成的值
        for target in created_staff_targets:
            await db.refresh(target)

        return created_staff_targets

    @staticmethod
    async def delete_staff_attendance(db: AsyncSession, fiscal_month: str, store_code: str, staff_code: str):
        result = await db.execute(select(StaffAttendanceModel).where(
            StaffAttendanceModel.fiscal_month == fiscal_month,
            StaffAttendanceModel.store_code == store_code,
            StaffAttendanceModel.staff_code == staff_code
        ))
        target_staff = result.scalars().all()
        for staff in target_staff:
            await db.delete(staff)
        await db.commit()
        return target_staff

    @staticmethod
    async def update_staff(db: AsyncSession, target_data: StaffAttendanceCreate):
        """
        根据target_data中的staff信息更新StaffModel的position和salary_coefficient

        Args:
            db: 数据库会话
            target_data: 包含员工信息的StaffAttendanceCreate对象

        Returns:
            list: 更新的员工列表
        """
        try:
            updated_staff_list = []

            for staff_data in target_data.staffs:
                # 查询StaffModel中对应的员工记录
                result = await db.execute(select(StaffModel).where(
                    StaffModel.staff_code == staff_data.staff_code,
                    StaffModel.store_code == target_data.store_code
                ))
                staff_model = result.scalar_one_or_none()

                if staff_model:
                    # 更新StaffModel中的position和salary_coefficient
                    if hasattr(staff_data, 'position') and staff_data.position is not None:
                        staff_model.position = staff_data.position
                    if hasattr(staff_data, 'salary_coefficient') and staff_data.salary_coefficient is not None:
                        staff_model.salary_coefficient = staff_data.salary_coefficient
                    updated_staff_list.append(staff_model)

            await db.commit()

            # 刷新所有更新的对象以获取数据库生成的值
            for staff in updated_staff_list:
                await db.refresh(staff)

            return updated_staff_list

        except Exception as e:
            app_logger.error(f"Error in update_staff: {str(e)}")
            await db.rollback()
            raise e
