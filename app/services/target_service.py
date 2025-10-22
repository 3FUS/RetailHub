from collections import defaultdict
from sqlalchemy import String, func, null, cast, Integer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.models.commission import CommissionStoreModel, CommissionMainModel
from app.models.target import TargetStoreMain, TargetStoreWeek, TargetStoreDaily
from app.models.staff import StaffAttendanceModel, StaffModel
from app.schemas.target import TargetStoreUpdate, \
    TargetStoreWeekCreate, \
    TargetStoreDailyCreate, StaffAttendanceCreate, BatchApprovedTarget
from app.models.dimension import DimensionDayWeek, StoreModel

from datetime import datetime

from app.services.commission_service import CommissionUtil
from app.utils.permissions import build_store_permission_query
from app.utils.logger import app_logger


class TargetRPTService:

    @staticmethod
    async def get_rpt_target_by_store(db: AsyncSession, fiscal_month: str, key_word: str, role_code: str):
        """
        获取门店目标报表数据
        """
        # 添加方法入口日志
        app_logger.info(
            f"Starting get_rpt_target_by_store with fiscal_month={fiscal_month}, key_word={key_word}, role_code={role_code}")

        try:
            store_permission_query = build_store_permission_query(role_code)
            store_alias = store_permission_query.subquery()

            # 记录权限查询结果
            app_logger.debug(f"Built store permission query for role_code={role_code}")

            # 执行SQL查询逻辑
            query = select(
                TargetStoreDaily.target_date.label('date'),
                func.dayname(TargetStoreDaily.target_date).label('day_of_week'),
                TargetStoreMain.store_code,
                store_alias.c.Location_ID,
                store_alias.c.store_name,
                (DimensionDayWeek.finance_year.cast(String) + DimensionDayWeek.week_number.cast(String)).label(
                    'fiscal_week'),
                TargetStoreWeek.percentage.label('week_percentage'),
                (TargetStoreWeek.percentage / 100 * TargetStoreMain.target_value).label('week_value'),
                TargetStoreDaily.percentage.label('day_percentage'),
                (TargetStoreMain.target_value * (TargetStoreWeek.percentage / 100) * (
                        TargetStoreDaily.percentage / 100)).label('day_value')
            ).select_from(
                TargetStoreMain.__table__.join(
                    TargetStoreDaily.__table__,
                    (TargetStoreMain.store_code == TargetStoreDaily.store_code) &
                    (TargetStoreMain.fiscal_month == TargetStoreDaily.fiscal_month)
                ).join(
                    TargetStoreWeek.__table__,
                    (TargetStoreDaily.store_code == TargetStoreWeek.store_code) &
                    (TargetStoreDaily.fiscal_month == TargetStoreWeek.fiscal_month) &
                    (TargetStoreDaily.week_number == TargetStoreWeek.week_number)
                ).join(
                    store_alias,
                    store_alias.c.store_code == TargetStoreMain.store_code
                ).join(
                    DimensionDayWeek.__table__,
                    DimensionDayWeek.actual_date == TargetStoreDaily.target_date
                )
            ).where(
                TargetStoreMain.fiscal_month == fiscal_month
            ).order_by(TargetStoreMain.store_code, TargetStoreDaily.target_date)

            # 如果有关键字过滤条件
            if key_word:
                query = query.where(
                    TargetStoreMain.store_code.contains(key_word) |
                    store_alias.c.store_name.contains(key_word)
                )
                app_logger.debug(f"Applied keyword filter: {key_word}")

            app_logger.debug(f"Executing query for fiscal_month={fiscal_month}")
            result = await db.execute(query)

            target_data = result.all()
            app_logger.debug(f"Query returned {len(target_data)} rows")

            # 构建结果数据
            formatted_data = []
            for row in target_data:
                formatted_data.append({
                    "date": row.date.strftime('%Y%m%d') if row.date else None,
                    "store_code": row.store_code,
                    "Location_ID": row.Location_ID,
                    "store_name": row.store_name,
                    "week_number": row.fiscal_week,
                    "week": row.day_of_week,
                    "week_percentage": f"{row.week_percentage}%" if row.week_percentage is not None else 0.0,
                    "week_value": float(row.week_value) if row.week_value is not None else 0.0,
                    "day_percentage": f"{row.day_percentage}%" if row.day_percentage is not None else 0.0,
                    "day_value": float(row.day_value) if row.day_value is not None else 0.0
                })

            app_logger.debug(f"Formatted {len(formatted_data)} rows of data")

            field_translations = {
                "date": {"en": "Date (Number)", "zh": "日期"},
                "store_code": {"en": "Location Code", "zh": "店铺代码"},
                "Location_ID": {"en": "Location ID", "zh": "店铺ID"},
                "store_name": {"en": "Location Name", "zh": "店铺名称"},
                "week_number": {"en": "Week Number", "zh": "周数"},
                "week": {"en": "Week", "zh": "周"},
                "week_percentage": {"en": "Week Percentage", "zh": "周百分比"},
                "week_value": {"en": "Week Value", "zh": "周目标值"},
                "day_percentage": {"en": "Day Percentage", "zh": "日百分比"},
                "day_value": {"en": "Day Value", "zh": "日目标值"}
            }

            app_logger.info(f"Successfully completed get_rpt_target_by_store with {len(formatted_data)} records")

            return {
                "data": formatted_data,
                "field_translations": field_translations
            }
        except Exception as e:
            # 记录并返回错误信息
            error_msg = f"Error in get_rpt_target_by_store: {str(e)}"
            app_logger.error(error_msg)
            return {
                "data": [],
                "field_translations": [],
                "error": error_msg
            }

    @staticmethod
    async def get_rpt_target_percentage_version(db: AsyncSession, fiscal_month: str, key_word: str, role_code: str):

        try:
            store_permission_query = build_store_permission_query(role_code)
            store_alias = store_permission_query.subquery()

            # 执行SQL查询逻辑
            query = select(
                TargetStoreDaily.target_date.label('date'),
                TargetStoreMain.store_code.label('store_code'),
                store_alias.c.store_name.label('store_name'),
                (DimensionDayWeek.finance_year + DimensionDayWeek.week_number.cast(String)).label('fiscal_week'),
                DimensionDayWeek.week_number.label('week_number'),
                TargetStoreWeek.percentage.label('week_percentage'),
                TargetStoreWeek.target_value.label('week_target_value'),
                TargetStoreDaily.percentage.label('day_percentage'),
                (TargetStoreMain.target_value * TargetStoreDaily.monthly_percentage / 100).label('day_target_value')
            ).select_from(
                TargetStoreMain.__table__.join(
                    TargetStoreDaily.__table__,
                    (TargetStoreMain.store_code == TargetStoreDaily.store_code) &
                    (TargetStoreMain.fiscal_month == TargetStoreDaily.fiscal_month)
                ).join(
                    store_alias,
                    store_alias.c.store_code == TargetStoreMain.store_code
                ).join(
                    DimensionDayWeek.__table__,
                    DimensionDayWeek.actual_date == TargetStoreDaily.target_date
                ).join(
                    TargetStoreWeek.__table__,
                    (TargetStoreWeek.store_code == TargetStoreMain.store_code) &
                    (TargetStoreWeek.fiscal_month == TargetStoreMain.fiscal_month) &
                    (TargetStoreWeek.week_number == DimensionDayWeek.week_number)
                )
            ).where(
                TargetStoreMain.fiscal_month == fiscal_month
            ).order_by(TargetStoreMain.store_code, TargetStoreDaily.target_date)

            # 如果有关键字过滤条件
            if key_word:
                query = query.where(
                    TargetStoreMain.store_code.contains(key_word) |
                    store_alias.c.store_name.contains(key_word)
                )

            result = await db.execute(query)
            target_data = result.all()

            # 构建结果数据
            formatted_data = []
            for row in target_data:
                formatted_data.append({
                    "date": row.date.strftime('%Y%m%d') if row.date else None,
                    "location_code": row.store_code,
                    "location_id": row.store_code,
                    "location_name": row.store_name,
                    "fiscal_week": row.fiscal_week,
                    "week": row.week_number,
                    "week_percentage": float(row.week_percentage) if row.week_percentage is not None else 0.0,
                    "week_value": float(row.week_target_value) if row.week_target_value is not None else 0.0,
                    "day_percentage": float(row.day_percentage) if row.day_percentage is not None else 0.0,
                    "day_value": float(row.day_target_value) if row.day_target_value is not None else 0.0
                })

            field_translations = {
                "date": {"en": "Date", "zh": "日期"},
                "location_code": {"en": "Location Code", "zh": "店铺代码"},
                "location_id": {"en": "Location ID", "zh": "店铺ID"},
                "location_name": {"en": "Location Name", "zh": "店铺名称"},
                "fiscal_week": {"en": "Fiscal Week", "zh": "财周"},
                "week": {"en": "Week", "zh": "周"},
                "week_percentage": {"en": "Week Percentage", "zh": "周百分比"},
                "week_value": {"en": "Week Value", "zh": "周目标值"},
                "day_percentage": {"en": "Day Percentage", "zh": "日百分比"},
                "day_value": {"en": "Day Value", "zh": "日目标值"}
            }

            return {
                "data": formatted_data,
                "field_translations": field_translations
            }
        except Exception as e:
            error_msg = f"Error in get_rpt_target_percentage_version: {str(e)}"
            app_logger.error(error_msg)
            return {
                "data": [],
                "field_translations": {},
                "error": error_msg
            }

    @staticmethod
    async def get_rpt_target_bi_version(db: AsyncSession, fiscal_month: str, key_word: str, role_code: str):
        """
        获取门店目标报表数据 - BI版本
        格式: Date (Number)	Fiscal Week (ID)	Fiscal Month (ID)	Location Code	Location ID	Location Short Name	Commission Target Local

        Args:
            db: 数据库会话
            fiscal_month: 财月
            key_word: 查询关键字（门店代码或名称）
            role_code: 角色代码

        Returns:
            dict: 报表数据
        """
        try:
            store_permission_query = build_store_permission_query(role_code)
            store_alias = store_permission_query.subquery()

            query = select(
                TargetStoreDaily.target_date.label('date'),
                (DimensionDayWeek.finance_year.cast(String) + DimensionDayWeek.week_number.cast(String)).label(
                    'fiscal_week'),
                TargetStoreMain.fiscal_month.label('fiscal_month'),
                TargetStoreMain.store_code.label('store_code'),
                store_alias.c.Location_ID,
                store_alias.c.store_name.label('store_name'),
                (TargetStoreMain.target_value * (TargetStoreWeek.percentage / 100) * (
                        TargetStoreDaily.percentage / 100)).label('commission_target_local')
            ).select_from(
                TargetStoreMain.__table__.join(
                    TargetStoreDaily.__table__,
                    (TargetStoreMain.store_code == TargetStoreDaily.store_code) &
                    (TargetStoreMain.fiscal_month == TargetStoreDaily.fiscal_month)
                ).join(
                    TargetStoreWeek.__table__,
                    (TargetStoreDaily.store_code == TargetStoreWeek.store_code) &
                    (TargetStoreDaily.fiscal_month == TargetStoreWeek.fiscal_month) &
                    (TargetStoreDaily.week_number == TargetStoreWeek.week_number)
                ).join(
                    store_alias,
                    store_alias.c.store_code == TargetStoreMain.store_code
                ).join(
                    DimensionDayWeek.__table__,
                    DimensionDayWeek.actual_date == TargetStoreDaily.target_date
                )
            ).where(
                TargetStoreMain.fiscal_month == fiscal_month
            ).order_by(TargetStoreMain.store_code,TargetStoreDaily.target_date)

            # 如果有关键字过滤条件
            if key_word:
                query = query.where(
                    TargetStoreMain.store_code.contains(key_word) |
                    store_alias.c.store_name.contains(key_word)
                )

            result = await db.execute(query)
            target_data = result.all()

            # 构建结果数据
            formatted_data = []
            for row in target_data:
                formatted_data.append({
                    "date_number": row.date.strftime('%Y%m%d') if row.date else None,
                    "fiscal_week_id": row.fiscal_week,
                    "fiscal_month_id": row.fiscal_month,
                    "location_code": row.store_code,
                    "location_id": row.Location_ID,
                    "location_short_name": row.store_name,
                    "commission_target_local": float(
                        row.commission_target_local) if row.commission_target_local is not None else 0.0
                })

            field_translations = {
                "date_number": {"en": "Date (Number)", "zh": "日期"},
                "fiscal_week_id": {"en": "Fiscal Week (ID)", "zh": "财周"},
                "fiscal_month_id": {"en": "Fiscal Month (ID)", "zh": "财月"},
                "location_code": {"en": "Location Code", "zh": "店铺代码"},
                "location_id": {"en": "Location ID", "zh": "店铺ID"},
                "location_short_name": {"en": "Location Short Name", "zh": "店铺名称"},
                "commission_target_local": {"en": "Commission Target Local", "zh": "佣金目标"}
            }

            return {
                "data": formatted_data,
                "field_translations": field_translations
            }
        except Exception as e:
            error_msg = f"Error in get_rpt_target_bi_version: {str(e)}"
            app_logger.error(error_msg)
            return {
                "data": [],
                "field_translations": {},
                "error": error_msg
            }

    @staticmethod
    async def get_rpt_target_date_horizontal_version(db: AsyncSession, fiscal_month: str, key_word: str,
                                                     role_code: str):
        """
        获取门店目标报表数据 - 日期横向版本
        格式: Store	20250803	20250804	20250805	20250806	20250807	20250808	20250809	20250810

        Args:
            db: 数据库会话
            fiscal_month: 财月
            key_word: 查询关键字（门店代码或名称）
            role_code: 角色代码

        Returns:
            dict: 报表数据
        """
        try:
            store_permission_query = build_store_permission_query(role_code)
            store_alias = store_permission_query.subquery()

            # 执行SQL查询逻辑
            query = select(
                TargetStoreDaily.target_date.label('date'),
                TargetStoreMain.store_code.label('store_code'),
                store_alias.c.store_name.label('store_name'),
                (TargetStoreMain.target_value * TargetStoreDaily.monthly_percentage / 100).label('target_date_value')
            ).select_from(
                TargetStoreMain.__table__.join(
                    TargetStoreDaily.__table__,
                    (TargetStoreMain.store_code == TargetStoreDaily.store_code) &
                    (TargetStoreMain.fiscal_month == TargetStoreDaily.fiscal_month)
                ).join(
                    store_alias,
                    store_alias.c.store_code == TargetStoreMain.store_code
                ).join(
                    DimensionDayWeek.__table__,
                    DimensionDayWeek.actual_date == TargetStoreDaily.target_date
                )
            ).where(
                TargetStoreMain.fiscal_month == fiscal_month
            ).order_by(TargetStoreMain.store_code, TargetStoreDaily.target_date)

            # 如果有关键字过滤条件
            if key_word:
                query = query.where(
                    TargetStoreMain.store_code.contains(key_word) |
                    store_alias.c.store_name.contains(key_word)
                )

            result = await db.execute(query)
            target_data = result.all()

            # 按日期分组数据并构建横向表结构（参考 get_budget_data 的实现方式）
            date_groups = defaultdict(dict)
            store_codes = set()
            # store_names = {}

            for row in target_data:
                date_str = row.date.strftime('%Y%m%d') if row.date else None
                store_code = row.store_code
                # store_name = row.store_name
                target_value = float(row.target_date_value) if row.target_date_value is not None else 0.0

                if date_str:
                    date_groups[date_str][store_code] = target_value
                    store_codes.add(store_code)
                    # if store_code not in store_names:
                    #     store_names[store_code] = store_name

            # 构建结果数据 - 转换为横向格式
            sorted_store_codes = sorted(list(store_codes))
            formatted_data = []

            # 为每个门店构建一行数据
            for store_code in sorted_store_codes:
                # store_name = store_names.get(store_code, "")
                row_data = {"store": f"{store_code}"}

                # 为每个日期添加目标值
                for date_str in sorted(date_groups.keys()):
                    row_data[date_str] = date_groups[date_str].get(store_code, 0.0)

                formatted_data.append(row_data)

            # 构建字段翻译
            field_translations = {
                "store": {"en": "Store", "zh": "店铺"}
            }
            for date_str in sorted(date_groups.keys()):
                field_translations[date_str] = {"en": date_str, "zh": date_str}

            return {
                "data": formatted_data,
                "field_translations": field_translations,
                "dates": sorted(list(date_groups.keys()))  # 返回排序后的日期列表
            }
        except Exception as e:
            error_msg = f"Error in get_rpt_target_date_horizontal_version: {str(e)}"
            app_logger.error(error_msg)
            return {
                "data": [],
                "field_translations": {},
                "error": error_msg
            }

    @staticmethod
    async def get_rpt_target_by_staff(db: AsyncSession, fiscal_month: str, key_word: str, role_code: str):
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
            store_permission_query = build_store_permission_query(role_code)
            store_alias = store_permission_query.subquery()
            # 执行SQL查询逻辑
            query = select(
                TargetStoreMain.fiscal_month,
                TargetStoreMain.store_code,
                store_alias.c.Location_ID,
                store_alias.c.store_name,
                StaffAttendanceModel.staff_code,
                StaffAttendanceModel.target_value
            ).select_from(
                TargetStoreMain.__table__.join(
                    StaffAttendanceModel.__table__,
                    (TargetStoreMain.store_code == StaffAttendanceModel.store_code) &
                    (TargetStoreMain.fiscal_month == StaffAttendanceModel.fiscal_month)
                ).join(
                    store_alias,
                    store_alias.c.store_code == TargetStoreMain.store_code
                )
            ).where(
                TargetStoreMain.fiscal_month == fiscal_month
            ).order_by(TargetStoreMain.store_code, StaffAttendanceModel.staff_code)

            # 如果有关键字过滤条件
            if key_word:
                query = query.where(
                    TargetStoreMain.store_code.contains(key_word) |
                    store_alias.c.store_name.contains(key_word)
                )

            result = await db.execute(query)
            target_data = result.all()

            # 构建结果数据
            formatted_data = []
            for row in target_data:
                formatted_data.append({
                    "fiscal_month": row.fiscal_month,
                    "store_code": row.store_code,
                    "Location_ID": row.Location_ID,
                    "store_name": row.store_name,
                    "staff_code": row.staff_code,
                    "target_value": row.target_value
                })

            field_translations = {
                "fiscal_month": {"en": "Fiscal Month (ID)", "zh": "财月"},
                "store_code": {"en": "Location Code", "zh": "店铺代码"},
                "Location_ID": {"en": "Location ID", "zh": "店铺ID"},
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
    async def batch_update_target_value(db: AsyncSession, target_updates: list
                                        ):
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
                                  target_data: TargetStoreUpdate, role_code: str = 'system'):
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

        now = datetime.now()
        target_dict = target_data.dict(exclude_unset=True)

        # 处理 staff_status 相关字段
        if 'staff_status' in target_dict:
            staff_status = target_dict['staff_status']
            if staff_status == "saved":
                target_store.staff_saved_by = role_code
                target_store.staff_saved_at = now
            elif staff_status == "submitted":
                target_store.staff_submit_by = role_code
                target_store.staff_submit_at = now

        # 处理 store_status 相关字段
        if 'store_status' in target_dict:
            store_status = target_dict['store_status']
            if store_status == "saved":
                target_store.store_saved_by = role_code
                target_store.store_saved_at = now
            elif store_status == "submitted":
                target_store.store_submit_by = role_code
                target_store.store_submit_at = now

        # 更新其他字段
        for key, value in target_dict.items():
            setattr(target_store, key, value)

        target_store.updated_at = now

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
            store_alias.c.inactive_flag,
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

        should_values = True
        date_range_result = await db.execute(
            select(
                func.min(DimensionDayWeek.actual_date).label('min_date'),
                func.max(DimensionDayWeek.actual_date).label('max_date')
            )
                .where(DimensionDayWeek.fiscal_month == fiscal_month)
        )
        date_range = date_range_result.fetchone()
        min_date = None
        if date_range and date_range.min_date:
            min_date = date_range.min_date

        app_logger.debug(f"min_date values: {min_date}")
        if min_date and min_date.date() > datetime.now().date():
            should_values = False
            app_logger.debug("Minimum date is in the future, hiding target values")

        formatted_data = [
            {
                "store_code": row.store_code,
                "store_name": row.store_name,
                "store_type": row.store_type,
                "target_value": row.target_value if row.target_value is not None and should_values else None,
                "store_status": row.store_status,
                "staff_status": row.staff_status
            }
            for row in target_stores
            if (getattr(row, 'inactive_flag', None) == 0 or row.target_value is not None)
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

        month_end_value = await CommissionUtil.get_month_end_value(db, fiscal_month)

        return {
            "data": formatted_data,
            "field_translations": field_translations,
            "MonthEnd": month_end_value,
            "fiscal_month": fiscal_month
        }

    @staticmethod
    async def batch_approved_target_by_store_codes(db: AsyncSession, request: BatchApprovedTarget,
                                                   role_code: str = 'system') -> bool:
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
                    if staff_status == "approved":
                        t.staff_approved_by = role_code
                        t.staff_approved_at = datetime.now()
                    elif staff_status == "rejected":
                        t.staff_rejected_by = role_code
                        t.staff_rejected_at = datetime.now()
                        t.staff_reject_remarks = request.remarks
                if store_status is not None:
                    t.store_status = store_status

                    if store_status == "approved":
                        t.store_approved_by = role_code
                        t.store_approved_at = datetime.now()
                    elif store_status == "rejected":
                        t.store_rejected_by = role_code
                        t.store_rejected_at = datetime.now()
                        t.store_reject_remarks = request.remarks
                    elif store_status == "submitted":
                        t.store_submit_by = role_code
                        t.store_submit_at = datetime.now()

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

        result_current = await db.execute(
            select(
                TargetStoreWeek.week_number,
                TargetStoreWeek.percentage,
                TargetStoreWeek.target_value,
                TargetStoreWeek.sales_value_ly,
                TargetStoreWeek.sales_value_ly_percentage

            ).where(
                TargetStoreWeek.store_code == store_code,
                TargetStoreWeek.fiscal_month == fiscal_month
            )
        )
        current_weeks = result_current.all()

        current_list = [
            {
                "week_number": row.week_number if row.week_number is not None else None,
                "percentage": row.percentage if row.percentage is not None else None,
                "target_value": row.target_value if row.target_value is not None else None,
                "sales_value_ly": row.sales_value_ly if row.sales_value_ly is not None else None,
                "sales_value_ly_percentage": row.sales_value_ly_percentage if row.sales_value_ly_percentage is not None else None
            }
            for row in current_weeks
        ]

        return {
            "current_year": current_list,
            "last_year": []
        }

    @staticmethod
    async def create_target_store_week(db: AsyncSession, target_data: TargetStoreWeekCreate):

        result_main = await db.execute(select(TargetStoreMain.target_value).where(
            TargetStoreMain.store_code == target_data.store_code,
            TargetStoreMain.fiscal_month == target_data.fiscal_month
        ))
        target_store_main = result_main.fetchone()

        store_target_value = target_store_main.target_value if target_store_main and target_store_main.target_value else 0

        created_targets = []

        # 遍历所有传入的数据
        total_target_value = 0
        weeks_data = target_data.weeks

        for i, week_data in enumerate(weeks_data):
            # 检查记录是否已存在
            result = await db.execute(select(TargetStoreWeek).where(
                TargetStoreWeek.store_code == target_data.store_code,
                TargetStoreWeek.fiscal_month == target_data.fiscal_month,
                TargetStoreWeek.week_number == week_data.week_number
            ))
            existing_target = result.scalar_one_or_none()

            calculated_target_value = None
            if store_target_value > 0 and week_data.percentage is not None:
                calculated_target_value = round(store_target_value * week_data.percentage / 100)
                total_target_value += calculated_target_value
                if i == len(weeks_data) - 1:
                    # 最后一条记录使用剩余值
                    remaining_value = store_target_value - total_target_value
                    calculated_target_value = calculated_target_value + remaining_value  # 确保不为负数

            if existing_target:
                # 如果存在，更新记录
                for key, value in week_data.dict().items():
                    if key not in ['store_code', 'fiscal_month', 'week_number']:  # 不更新主键
                        setattr(existing_target, key, value)
                existing_target.percentage = week_data.percentage
                existing_target.target_value = calculated_target_value
                existing_target.updated_at = datetime.now()
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
                if calculated_target_value is not None:
                    target_store_week.target_value = calculated_target_value
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

        result_main = await db.execute(
            select(
                TargetStoreMain.store_status,
                TargetStoreMain.store_saved_by,
                TargetStoreMain.store_saved_at,
                TargetStoreMain.store_submit_by,
                TargetStoreMain.store_submit_at,
                TargetStoreMain.store_approved_by,
                TargetStoreMain.store_approved_at,
                TargetStoreMain.store_reject_remarks,
                TargetStoreMain.store_rejected_at,
                TargetStoreMain.store_rejected_by,
                TargetStoreMain.target_value
            )
                .where(
                TargetStoreMain.store_code == store_code,
                TargetStoreMain.fiscal_month == fiscal_month
            )
        )
        target_main_data = result_main.fetchone()

        result = await db.execute(
            select(
                DimensionDayWeek.day_number,
                DimensionDayWeek.week_number,
                DimensionDayWeek.actual_date,
                TargetStoreDaily.percentage,
                TargetStoreDaily.target_value,
                TargetStoreDaily.sales_value_ly,
                TargetStoreDaily.sales_value_ly_percentage

            )
                .select_from(
                DimensionDayWeek.__table__.join(
                    TargetStoreDaily.__table__,
                    (DimensionDayWeek.fiscal_month == TargetStoreDaily.fiscal_month) &
                    (DimensionDayWeek.actual_date == TargetStoreDaily.target_date) &
                    (TargetStoreDaily.store_code == store_code),
                    isouter=True
                )
            )
                .where(
                DimensionDayWeek.fiscal_month == fiscal_month
            )
        )
        target_daily = result.all()

        should_values = True
        date_range_result = await db.execute(
            select(
                func.min(DimensionDayWeek.actual_date).label('min_date'),
                func.max(DimensionDayWeek.actual_date).label('max_date')
            )
                .where(DimensionDayWeek.fiscal_month == fiscal_month)
        )
        date_range = date_range_result.fetchone()
        min_date = None
        fiscal_period = None
        if date_range and date_range.min_date:
            min_date = date_range.min_date
            fiscal_period = f"{date_range.min_date.strftime('%Y-%m-%d')} to {date_range.max_date.strftime('%Y-%m-%d')}"

        app_logger.debug(f"min_date values: {min_date}")
        if min_date and min_date.date() > datetime.now().date():
            should_values = False
            app_logger.debug("Minimum date is in the future, hiding target values")

        data = [
            {
                "week_number": row.week_number if row.week_number is not None else None,
                "actual_date": row.actual_date.strftime('%Y-%m-%d') if row.actual_date else None,
                "percentage": row.percentage if row.percentage is not None else None,
                "target_value": row.target_value if row.target_value is not None else None,
                "sales_value_ly_percentage": row.sales_value_ly_percentage if row.sales_value_ly_percentage is not None else None,
                "sales_value_ly": row.sales_value_ly if row.sales_value_ly is not None else None
            }
            for row in target_daily
        ]

        header_info = {
            "store_status": target_main_data.store_status if target_main_data else None,
            "fiscal_period": fiscal_period,
            "target_value":
                target_main_data.target_value if target_main_data and target_main_data.target_value is not None and should_values else None,
            "store_status_details": {
                "saved_by": target_main_data.store_saved_by if target_main_data else None,
                "saved_at": target_main_data.store_saved_at if target_main_data else None,
                "submit_by": target_main_data.store_submit_by if target_main_data else None,
                "submit_at": target_main_data.store_submit_at if target_main_data else None,
                "approved_by": target_main_data.store_approved_by if target_main_data else None,
                "approved_at": target_main_data.store_approved_at if target_main_data else None,
                "reject_remarks": target_main_data.store_reject_remarks if target_main_data else None,
                "rejected_at": target_main_data.store_rejected_at if target_main_data else None,
                "rejected_by": target_main_data.store_rejected_by if target_main_data else None}

        }

        month_end_value = await CommissionUtil.get_month_end_value(db, fiscal_month)

        return {"data": data, "header_info": header_info, "MonthEnd": month_end_value}

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
                                        target_data: TargetStoreDailyCreate, role_code: str = 'system'):

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
                                                     target_store_update, role_code)
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


# 在 target_service.py 中添加以下工具类
class StaffTargetCalculator:
    @staticmethod
    def calculate_staff_targets(store_target_value: float, ratios: list) -> list:
        """
        根据门店目标值和员工比例计算每个员工的目标值

        Args:
            store_target_value: 门店目标值
            ratios: 员工比例列表

        Returns:
            list: 员工目标值列表
        """
        if store_target_value <= 0 or not ratios:
            return [0] * len(ratios) if ratios else []

        # 计算每个员工的初始目标值（整数）
        staff_target_values = []
        for ratio in ratios:
            staff_target_value = round(store_target_value * ratio, 0) if ratio else 0.0
            staff_target_values.append(int(staff_target_value))

        # 计算总和与门店目标值的差异
        total_staff_target = sum(staff_target_values)
        difference = int(store_target_value) - total_staff_target

        # 找到目标值最大的员工索引，将差异加到该员工身上
        if difference != 0 and staff_target_values:
            max_target_index = staff_target_values.index(max(staff_target_values))
            staff_target_values[max_target_index] += difference

        return staff_target_values

    @staticmethod
    def calculate_staff_target_from_ratio(store_target_value: float, ratio: float) -> int:
        """
        根据门店目标值和单个员工比例计算员工目标值

        Args:
            store_target_value: 门店目标值
            ratio: 员工比例

        Returns:
            int: 员工目标值
        """
        if store_target_value <= 0 or ratio <= 0:
            return 0

        return int(round(store_target_value * ratio, 0))


class TargetStaffService:
    @staticmethod
    async def get_staff_attendance(db: AsyncSession, fiscal_month: str, store_code: str, module: str = "target"):
        try:
            app_logger.info(f"Starting get_staff_attendance for fiscal_month={fiscal_month}, store_code={store_code}")

            # Step 1: 获取门店目标与佣金相关信息
            store_target_record = await TargetStaffService._fetch_store_target_data(db, fiscal_month, store_code)

            # Step 2: 提取门店相关基础信息
            store_target_value, store_sales_value, staff_status, store_status, commission_status, \
            commission_status_details, store_status_details, staff_status_details = \
                TargetStaffService._extract_store_info(store_target_record)

            app_logger.debug(
                f"get_staff_attendance Store target value: {store_target_value}, sales value: {store_sales_value}")

            merged_codes = [store_code]
            merged_months = [fiscal_month]
            if store_target_record and module == "commission":
                if store_target_record.merged_store_codes:
                    merged_code = store_target_record.merged_store_codes.split(',')
                    merged_codes = [code.strip() for code in merged_code]

                if store_target_record.fiscal_period and store_target_record.fiscal_period != fiscal_month:
                    merged_month = store_target_record.fiscal_period.split(',')
                    merged_months = [code.strip() for code in merged_month]

                result_merged = await db.execute(
                    select(
                        func.sum(TargetStoreMain.target_value).label('total_target_value'),
                        func.sum(TargetStoreMain.sales_value).label('total_sales_value')
                    )
                        .where(
                        TargetStoreMain.fiscal_month.in_(merged_months),
                        TargetStoreMain.store_code.in_(merged_codes)
                    )
                )
                merged_data = result_merged.fetchone()

                store_target_value = float(
                    merged_data.total_target_value) if merged_data.total_target_value is not None else 0.0
                store_sales_value = float(
                    merged_data.total_sales_value) if merged_data.total_sales_value is not None else 0.0

            fiscal_period, min_date = await TargetStaffService._fetch_fiscal_period(db,
                                                                                    merged_months if module == "commission" else [
                                                                                        fiscal_month])

            should_values = True
            app_logger.debug(f"min_date values: {min_date}")
            if min_date and min_date.date() > datetime.now().date():
                should_values = False
                app_logger.debug("Minimum date is in the future, hiding target values")

            app_logger.debug("Checking if staff attendance data exists")
            attendance_check_result = await db.execute(
                select(func.count()).select_from(StaffAttendanceModel)
                    .where(
                    StaffAttendanceModel.fiscal_month.in_(merged_months),
                    StaffAttendanceModel.store_code.in_(merged_codes)
                )
            )
            attendance_exists = attendance_check_result.scalar() > 0
            app_logger.debug(f"Staff attendance data exists: {attendance_exists}")

            if attendance_exists:
                # 有数据存在，使用inner join
                app_logger.debug(f'Querying staff attendance with inner join {merged_codes}, {merged_months}')
                result = await db.execute(
                    select(
                        StaffModel.avatar,
                        StaffModel.staff_code,
                        StaffModel.first_name,
                        StaffModel.state,
                        StaffModel.position.label('staff_position'),
                        StaffModel.salary_coefficient.label('staff_salary_coefficient'),
                        StaffAttendanceModel.expected_attendance,
                        StaffAttendanceModel.actual_attendance,
                        StaffAttendanceModel.position.label('attendance_position'),
                        StaffAttendanceModel.salary_coefficient.label('attendance_salary_coefficient'),
                        StaffAttendanceModel.target_value_ratio,
                        StaffAttendanceModel.target_value,
                        StaffAttendanceModel.sales_value,
                        StaffAttendanceModel.deletable,
                        StaffAttendanceModel.fiscal_month
                    )
                        .select_from(
                        StaffModel.__table__.join(
                            StaffAttendanceModel.__table__,
                            (StaffModel.staff_code == StaffAttendanceModel.staff_code)
                        )
                    )
                        .where(
                        StaffAttendanceModel.store_code.in_(merged_codes),
                        StaffAttendanceModel.fiscal_month.in_(merged_months)
                    )
                )
            else:
                # 没有数据存在，直接查询StaffModel单表
                app_logger.debug("Querying staff model data only")
                result = await db.execute(
                    select(
                        StaffModel.avatar,
                        StaffModel.staff_code,
                        StaffModel.first_name,
                        StaffModel.state,
                        StaffModel.position.label('staff_position'),
                        StaffModel.salary_coefficient.label('staff_salary_coefficient'),
                        null().label('expected_attendance'),
                        null().label('actual_attendance'),
                        null().label('attendance_position'),
                        null().label('attendance_salary_coefficient'),
                        null().label('target_value_ratio'),
                        null().label('target_value'),
                        null().label('sales_value'),
                        cast(0, type_=Integer).label('deletable'),
                        cast(fiscal_month, type_=String).label('fiscal_month')
                    )
                        .where(
                        StaffModel.store_code.in_(merged_codes),
                        StaffModel.state == 'A'
                    )
                )
            staff_attendance_data = result.all()
            app_logger.debug(f"Retrieved {len(staff_attendance_data)} staff records")

            # total_target_value = sum(
            #     float(row.target_value) if row.target_value is not None else 0 for row in staff_attendance_data)

            staff_attendance_dict = {}
            for row in staff_attendance_data:
                staff_code = row.staff_code

                position = row.attendance_position if row.attendance_position is not None else row.staff_position
                salary_coefficient = (
                    row.attendance_salary_coefficient
                    if row.attendance_salary_coefficient is not None
                    else row.staff_salary_coefficient
                )

                if staff_code not in staff_attendance_dict:
                    staff_attendance_dict[staff_code] = {
                        "avatar": row.avatar,
                        "staff_code": staff_code,
                        "first_name": row.first_name,
                        "expected_attendance": float(
                            row.expected_attendance) if row.expected_attendance is not None else 0.0,
                        "actual_attendance": float(
                            row.actual_attendance) if row.actual_attendance is not None else 0.0,
                        "target_value": float(
                            row.target_value) if row.target_value is not None and should_values else 0.0,
                        "sales_value": float(row.sales_value) if row.sales_value is not None else 0.0,
                        "target_value_ratio": row.target_value_ratio,
                        "deletable": row.deletable
                    }
                else:
                    staff_attendance_dict[staff_code]["expected_attendance"] += float(
                        row.expected_attendance) if row.expected_attendance is not None else 0.0

                    staff_attendance_dict[staff_code]["actual_attendance"] += float(
                        row.actual_attendance) if row.actual_attendance is not None else 0.0

                    staff_attendance_dict[staff_code]["sales_value"] += float(
                        row.sales_value) if row.sales_value is not None else 0.0

                    staff_attendance_dict[staff_code]["target_value"] += float(
                        row.target_value) if row.target_value is not None else 0.0

                if row.fiscal_month == fiscal_month:
                    # staff_attendance_dict[staff_code]["actual_attendance"] = float(
                    #     row.actual_attendance) if row.actual_attendance is not None else None
                    staff_attendance_dict[staff_code]["position"] = position
                    staff_attendance_dict[staff_code]["salary_coefficient"] = float(
                        salary_coefficient) if salary_coefficient is not None else None

            # 计算每个员工的目标值和达成率
            for staff_code, staff_info in staff_attendance_dict.items():

                # 使用汇总后的数据计算达成率
                achievement_rate = None
                if (staff_info["sales_value"] is not None and
                        staff_info['target_value'] is not None and
                        staff_info['target_value'] > 0):
                    achievement_rate = f"{staff_info['sales_value'] / staff_info['target_value']:.2%}"

                staff_info["achievement_rate"] = achievement_rate
                staff_info["target_value_ratio"] = f"{staff_info['target_value_ratio']:.4%}" if staff_info[
                                                                                                    "target_value_ratio"] is not None else None

            # 转换为列表格式
            staff_attendance_list = list(staff_attendance_dict.values())

            # 获取月结状态
            month_end_value = await CommissionUtil.get_month_end_value(db, fiscal_month)

            result_data = {
                "data": staff_attendance_list,
                "header_info": {
                    "store_target_value": store_target_value if should_values else 0,
                    "store_sales_value": store_sales_value,
                    "fiscal_period": fiscal_period,  # 新增的财月日期范围
                    "min_date": min_date,
                    "staff_status": staff_status,
                    "staff_status_details": staff_status_details,
                    "store_status": store_status,
                    "store_status_details": store_status_details,
                    "commission_status": commission_status,
                    "commission_status_details": commission_status_details
                },
                "MonthEnd": month_end_value
            }

            app_logger.info(
                f"Successfully completed get_staff_attendance for fiscal_month={fiscal_month}, store_code={store_code}")
            return result_data

        except Exception as e:
            app_logger.error(
                f"Error in get_staff_attendance: fiscal_month={fiscal_month}, store_code={store_code}, error={str(e)}")
            # 可以选择抛出异常或返回空列表
            # return []  # 或者
            raise e

    @staticmethod
    async def _fetch_store_target_data(db: AsyncSession, fiscal_month: str, store_code: str):
        """获取门店的目标和佣金相关信息"""
        result_store = await db.execute(
            select(
                TargetStoreMain.target_value,
                TargetStoreMain.sales_value,
                TargetStoreMain.store_status,
                TargetStoreMain.staff_status,
                TargetStoreMain.store_saved_by,
                TargetStoreMain.store_saved_at,
                TargetStoreMain.store_submit_by,
                TargetStoreMain.store_submit_at,
                TargetStoreMain.store_approved_by,
                TargetStoreMain.store_approved_at,
                TargetStoreMain.store_rejected_by,
                TargetStoreMain.store_rejected_at,
                TargetStoreMain.store_reject_remarks,
                TargetStoreMain.staff_saved_by,
                TargetStoreMain.staff_saved_at,
                TargetStoreMain.staff_submit_by,
                TargetStoreMain.staff_submit_at,
                TargetStoreMain.staff_approved_by,
                TargetStoreMain.staff_approved_at,
                TargetStoreMain.staff_rejected_by,
                TargetStoreMain.staff_rejected_at,
                TargetStoreMain.staff_reject_remarks,
                CommissionStoreModel.status.label('commission_status'),
                CommissionStoreModel.saved_by,
                CommissionStoreModel.saved_at,
                CommissionStoreModel.submit_by,
                CommissionStoreModel.submit_at,
                CommissionStoreModel.approved_by,
                CommissionStoreModel.approved_at,
                CommissionStoreModel.rejected_by,
                CommissionStoreModel.rejected_at,
                CommissionStoreModel.reject_remarks,
                CommissionStoreModel.merged_store_codes,
                CommissionStoreModel.merged_flag,
                CommissionStoreModel.fiscal_period
            )
                .select_from(
                TargetStoreMain.__table__.join(
                    CommissionStoreModel.__table__,
                    (TargetStoreMain.store_code == CommissionStoreModel.store_code) &
                    (TargetStoreMain.fiscal_month == CommissionStoreModel.fiscal_month),
                    isouter=True
                )
            )
                .where(
                TargetStoreMain.fiscal_month == fiscal_month,
                TargetStoreMain.store_code == store_code
            )
        )
        return result_store.fetchone()

    @staticmethod
    def _extract_store_info(store_target_record):
        """从门店记录中提取关键信息"""
        if store_target_record:
            store_target_value = float(store_target_record.target_value) if store_target_record.target_value else 0.0
            store_sales_value = float(store_target_record.sales_value) if store_target_record.sales_value else 0.0
            staff_status = store_target_record.staff_status
            store_status = store_target_record.store_status
            commission_status = store_target_record.commission_status

            commission_status_details = {
                'saved_by': store_target_record.saved_by,
                'saved_at': store_target_record.saved_at,
                'submit_by': store_target_record.submit_by,
                'submit_at': store_target_record.submit_at,
                'approved_by': store_target_record.approved_by,
                'approved_at': store_target_record.approved_at,
                'rejected_by': store_target_record.rejected_by,
                'rejected_at': store_target_record.rejected_at,
                'reject_remarks': store_target_record.reject_remarks
            }
            store_status_details = {
                'saved_by': store_target_record.store_saved_by,
                'saved_at': store_target_record.store_saved_at,
                'submit_by': store_target_record.store_submit_by,
                'submit_at': store_target_record.store_submit_at,
                'approved_by': store_target_record.store_approved_by,
                'approved_at': store_target_record.store_approved_at,
                'rejected_by': store_target_record.store_rejected_by,
                'rejected_at': store_target_record.store_rejected_at,
                'reject_remarks': store_target_record.store_reject_remarks
            }
            staff_status_details = {
                'saved_by': store_target_record.staff_saved_by,
                'saved_at': store_target_record.staff_saved_at,
                'submit_by': store_target_record.staff_submit_by,
                'submit_at': store_target_record.staff_submit_at,
                'approved_by': store_target_record.staff_approved_by,
                'approved_at': store_target_record.staff_approved_at,
                'rejected_by': store_target_record.staff_rejected_by,
                'rejected_at': store_target_record.staff_rejected_at,
                'reject_remarks': store_target_record.staff_reject_remarks
            }
        else:
            store_target_value = 0.0
            store_sales_value = 0.0
            staff_status = None
            store_status = None
            commission_status = None
            commission_status_details = {}
            store_status_details = {}
            staff_status_details = {}

        return (store_target_value, store_sales_value, staff_status, store_status,
                commission_status, commission_status_details, store_status_details, staff_status_details)

    @staticmethod
    async def _fetch_fiscal_period(db: AsyncSession, fiscal_month: list):
        """获取指定财月的时间区间"""
        date_range_result = await db.execute(
            select(
                func.min(DimensionDayWeek.actual_date).label('min_date'),
                func.max(DimensionDayWeek.actual_date).label('max_date')
            )
                .where(DimensionDayWeek.fiscal_month.in_(fiscal_month))
        )
        date_range = date_range_result.fetchone()
        if date_range and date_range.min_date and date_range.max_date:
            date_range_str = f"{date_range.min_date.strftime('%Y-%m-%d')} to {date_range.max_date.strftime('%Y-%m-%d')}"
            return date_range_str, date_range.min_date
        return "", None

    @staticmethod
    async def create_staff_attendance(db: AsyncSession, target_data: StaffAttendanceCreate, user_id: str = 'system'):
        """
        创建员工考勤和目标数据

        Args:
            db: 数据库会话
            target_data: 员工考勤创建数据
            user_id: 操作用户ID
        """
        app_logger.info(f"Starting create_staff_attendance for store_code={target_data.store_code}, "
                        f"fiscal_month={target_data.fiscal_month}, user_id={user_id}")
        app_logger.debug(f"Received target_data: {target_data}")

        created_staff_targets = []

        try:

            selling_1_staffs = [staff for staff in target_data.staffs if staff.position == 'Selling_1']
            app_logger.debug(f"Found {len(selling_1_staffs)}  Selling_1 staff members")

            # 处理 Selling_1 员工
            selling_1_total_sales = 0
            for staff_data in selling_1_staffs:
                app_logger.debug(f"Processing  Selling_1 staff: {staff_data.staff_code}")
                result = await db.execute(select(StaffAttendanceModel).where(
                    StaffAttendanceModel.staff_code == staff_data.staff_code,
                    StaffAttendanceModel.store_code == target_data.store_code,
                    StaffAttendanceModel.fiscal_month == target_data.fiscal_month
                ))
                existing_target = result.scalar_one_or_none()

                if existing_target and existing_target.sales_value is not None:
                    staff_target_value = round(existing_target.sales_value, 0)
                else:
                    staff_target_value = getattr(staff_data, 'sales_value', 0) or 0

                selling_1_total_sales += staff_target_value
                app_logger.debug(f"Staff {staff_data.staff_code} target value: {staff_target_value}, "
                                 f"running total: {selling_1_total_sales}")

                if existing_target:
                    # 更新记录
                    app_logger.debug(f"Updating existing record for staff: {staff_data.staff_code}")

                    for key, value in staff_data.dict().items():
                        if key not in ['store_code', 'fiscal_month', 'staff_code']:
                            setattr(existing_target, key, value)
                    existing_target.target_value = staff_target_value
                    existing_target.position = staff_data.position
                    existing_target.salary_coefficient = staff_data.salary_coefficient
                    existing_target.updated_at = datetime.now()
                    created_staff_targets.append(existing_target)
                else:
                    # 创建新记录
                    app_logger.debug(f"Creating new record for staff: {staff_data.staff_code}")
                    target_staff_attendance = StaffAttendanceModel(
                        staff_code=staff_data.staff_code,
                        store_code=target_data.store_code,
                        fiscal_month=target_data.fiscal_month,
                        expected_attendance=staff_data.expected_attendance,
                        position=staff_data.position,
                        salary_coefficient=staff_data.salary_coefficient,
                        target_value=staff_target_value,
                        creator_code=user_id
                    )
                    db.add(target_staff_attendance)
                    created_staff_targets.append(target_staff_attendance)

            staffs = [staff for staff in target_data.staffs if staff.position != 'Selling_1']
            app_logger.debug(f"Found {len(staffs)} Selling_1 staff members")

            # 获取门店目标值
            result_store = await db.execute(
                select(TargetStoreMain.target_value)
                    .where(
                    TargetStoreMain.store_code == target_data.store_code,
                    TargetStoreMain.fiscal_month == target_data.fiscal_month
                )
            )
            store_target_record = result_store.fetchone()
            store_target_value = float(
                store_target_record.target_value) if store_target_record and store_target_record.target_value else 0.0
            app_logger.debug(f"Store target value: {store_target_value}")

            # 计算分配给 Selling_1 员工的目标值
            store_target_value = store_target_value - selling_1_total_sales
            app_logger.debug(f"Adjusted store target value for Selling_1 staff: {store_target_value}")

            # 计算权重
            total_expected_attendance = sum(staff_data.expected_attendance or 0 for staff_data in staffs)
            total_salary_coefficient = sum(staff_data.salary_coefficient or 0 for staff_data in staffs)
            app_logger.debug(f"Total expected attendance: {total_expected_attendance}, "
                             f"total salary coefficient: {total_salary_coefficient}")

            weights = []
            for staff_data in staffs:
                expected_attendance = staff_data.expected_attendance or 0
                salary_coefficient = staff_data.salary_coefficient or 0

                if total_expected_attendance > 0 and total_salary_coefficient > 0:
                    expected_attendance_ratio = expected_attendance / total_expected_attendance
                    salary_coefficient_ratio = salary_coefficient / total_salary_coefficient
                    weight = expected_attendance_ratio * salary_coefficient_ratio
                else:
                    weight = 0

                weights.append(weight)
                app_logger.debug(f"Staff {staff_data.staff_code}: weight={weight}")

            # 计算总权重
            total_weight = sum(weights)
            app_logger.debug(f"Total weight: {total_weight}")

            if total_weight <= 0:
                app_logger.warning("Total weight is zero or negative, returning empty list")
                return []

            # 计算比例
            ratios = []
            if total_weight > 0:
                for i in range(len(weights)):
                    ratio = round(weights[i] / total_weight, 6)
                    ratios.append(ratio)

                app_logger.debug(f"Initial ratios before adjustment: {ratios}")

                if ratios:
                    ratio_sum = sum(ratios)
                    diff = 1.0 - ratio_sum
                    app_logger.debug(f"Ratio sum: {ratio_sum}, Difference to 1.0: {diff}")

                    if diff != 0:
                        # 找到比例最高的员工索引
                        max_ratio_index = ratios.index(max(ratios))
                        app_logger.debug(f"Max ratio index: {max_ratio_index}, Max ratio value: {max(ratios)}")

                        # 将差额加到比例最高的员工身上
                        ratios[max_ratio_index] = round(ratios[max_ratio_index] + diff, 6)
                        app_logger.debug(
                            f"Adjusted ratio at index {max_ratio_index} from {max(ratios) - diff} to {ratios[max_ratio_index]}")

                    app_logger.debug(f"Final ratios after adjustment: {ratios}, Sum: {sum(ratios)}")

            app_logger.debug(f"Calculated ratios: {ratios}")

            # 计算员工目标值
            staff_target_values = StaffTargetCalculator.calculate_staff_targets(store_target_value, ratios)
            app_logger.debug(f"Calculated staff target values: {staff_target_values}")

            # 为 Selling_1 员工创建或更新记录
            for i, staff_data in enumerate(staffs):
                app_logger.debug(f"Processing Selling_1 staff: {staff_data.staff_code}")
                result = await db.execute(select(StaffAttendanceModel).where(
                    StaffAttendanceModel.staff_code == staff_data.staff_code,
                    StaffAttendanceModel.store_code == target_data.store_code,
                    StaffAttendanceModel.fiscal_month == target_data.fiscal_month
                ))
                existing_target = result.scalar_one_or_none()

                target_value_ratio = ratios[i] if ratios else 0
                staff_target_value = staff_target_values[i]
                app_logger.debug(f"Staff {staff_data.staff_code}: ratio={target_value_ratio}, "
                                 f"target_value={staff_target_value}")

                if existing_target:
                    # 如果存在，更新记录
                    app_logger.debug(f"Updating existing record for Selling_1 staff: {staff_data.staff_code}")
                    for key, value in staff_data.dict().items():
                        if key not in ['store_code', 'fiscal_month', 'staff_code']:  # 不更新主键
                            setattr(existing_target, key, value)
                    existing_target.target_value_ratio = target_value_ratio
                    existing_target.target_value = staff_target_value
                    existing_target.updated_at = datetime.now()
                    created_staff_targets.append(existing_target)
                else:
                    app_logger.debug(f"Creating new record for Selling_1 staff: {staff_data.staff_code}")
                    target_staff_attendance = StaffAttendanceModel(
                        staff_code=staff_data.staff_code,
                        store_code=target_data.store_code,
                        fiscal_month=target_data.fiscal_month,
                        expected_attendance=staff_data.expected_attendance,
                        position=staff_data.position,
                        salary_coefficient=staff_data.salary_coefficient,
                        target_value_ratio=target_value_ratio,
                        target_value=staff_target_value,
                        creator_code=user_id
                    )
                    db.add(target_staff_attendance)
                    created_staff_targets.append(target_staff_attendance)

            # 提交事务
            await db.commit()
            app_logger.info(f"Committed {len(created_staff_targets)} staff attendance records")

            # 更新门店目标状态
            target_store_update = TargetStoreUpdate(
                store_code=target_data.store_code,
                fiscal_month=target_data.fiscal_month,
                staff_status=target_data.staff_status,
                creator_code=user_id
            )
            app_logger.debug(f"Updating target store with status: {target_data.staff_status}")
            await TargetStoreService.update_target_store(db, target_data.store_code, target_data.fiscal_month,
                                                         target_store_update, user_id)

            # 刷新所有创建的对象以获取数据库生成的值
            for target in created_staff_targets:
                await db.refresh(target)

            app_logger.info(f"Successfully completed create_staff_attendance for store_code={target_data.store_code}, "
                            f"fiscal_month={target_data.fiscal_month}")
            return created_staff_targets

        except Exception as e:
            app_logger.error(f"Error in create_staff_attendance for store_code={target_data.store_code}, "
                             f"fiscal_month={target_data.fiscal_month}, error={str(e)}")
            await db.rollback()
            raise e

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
