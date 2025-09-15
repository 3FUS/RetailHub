from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models.target import TargetStoreMain, TargetStoreWeek, TargetStoreDaily
from app.models.staff import StaffAttendanceModel, StaffModel
from app.schemas.target import TargetStoreCreate, TargetStoreUpdate, \
    TargetStoreWeekCreate, \
    TargetStoreDailyCreate, StaffAttendanceCreate
from app.models.dimension import DimensionDayWeek, StoreModel

from datetime import datetime


class TargetStoreService:
    @staticmethod
    async def create_target_store(db: AsyncSession, target_data: TargetStoreCreate):
        target_store = TargetStoreMain(**target_data.dict())
        db.add(target_store)
        await db.commit()
        await db.refresh(target_store)
        return target_store

    @staticmethod
    async def update_target_store(db: AsyncSession, store_code: str, fiscal_month: str,
                                  target_data: TargetStoreUpdate):
        result = await db.execute(select(TargetStoreMain).where(
            TargetStoreMain.store_code == store_code,
            TargetStoreMain.fiscal_month == fiscal_month))
        target_store = result.scalar_one_or_none()

        if not target_store:
            target_store_data = target_data.dict()
            target_store_data['store_code'] = store_code
            target_store_data['fiscal_month'] = fiscal_month
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
    async def get_all_target_stores_by_key(fiscal_month: str, key_word: str, db: AsyncSession):
        query = select(
            StoreModel.store_code,
            StoreModel.store_name,
            StoreModel.store_type,
            TargetStoreMain.target_value,
            TargetStoreMain.store_status,
            TargetStoreMain.staff_status
        ).select_from(
            StoreModel.__table__.join(
                TargetStoreMain.__table__,
                (StoreModel.store_code == TargetStoreMain.store_code) &
                (TargetStoreMain.fiscal_month == fiscal_month),
                isouter=True
            )
        ).where(
            StoreModel.manage_channel.in_(['ROCN', 'RFCN'])
        )

        # 当key_word不为None时，模糊过滤store_code或store_name
        if key_word:
            query = query.where(
                StoreModel.store_code.contains(key_word) |
                StoreModel.store_name.contains(key_word)
            )

        result = await db.execute(query)
        target_stores = result.all()

        # 将结果转换为字典列表
        return [
            {
                "store_code": row.store_code,
                "store_name": row.store_name,
                "store_type": row.store_type,
                "target_value": float(row.target_value) if row.target_value is not None else None,
                "store_status": row.store_status,
                "staff_status": row.staff_status
            }
            for row in target_stores
        ]


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
        result = await db.execute(
            select(
                StaffModel.avatar,
                StaffModel.staff_code,
                StaffModel.first_name,
                StaffAttendanceModel.expected_attendance,
                StaffAttendanceModel.actual_attendance,
                StaffAttendanceModel.position,
                StaffAttendanceModel.salary_coefficient,
                StaffAttendanceModel.target_value
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

        total_target_value = sum(
            float(row.target_value) if row.target_value is not None else 0 for row in staff_attendance_data)

        staff_attendance_list = []
        for row in staff_attendance_data:
            target_value = float(row.target_value) if row.target_value is not None else None
            target_value_ratio = None
            if target_value is not None and total_target_value > 0:
                ratio = target_value / total_target_value
                target_value_ratio = f"{ratio:.2%}"

            staff_attendance_list.append({
                "avatar": row.avatar,
                "staff_code": row.staff_code,
                "first_name": row.first_name,
                "expected_attendance": float(row.expected_attendance) if row.expected_attendance is not None else None,
                "actual_attendance": float(row.actual_attendance) if row.actual_attendance is not None else None,
                "position": row.position,
                "salary_coefficient": float(row.salary_coefficient) if row.salary_coefficient is not None else None,
                "target_value": target_value,
                "target_value_ratio": target_value_ratio  # 新增的占比字段
            })

        return staff_attendance_list

    @staticmethod
    async def create_staff_attendance(db: AsyncSession, target_data: StaffAttendanceCreate):

        created_staff_targets = []
        for staff_data in target_data.staffs:

            result = await db.execute(select(StaffAttendanceModel).where(
                StaffAttendanceModel.staff_code == staff_data.staff_code,
                StaffAttendanceModel.store_code == target_data.store_code,
                StaffAttendanceModel.fiscal_month == target_data.fiscal_month
            ))
            existing_target = result.scalar_one_or_none()
            if existing_target:
                # 如果存在，更新记录
                for key, value in staff_data.dict().items():
                    if key not in ['store_code', 'fiscal_month', 'staff_code']:  # 不更新主键
                        setattr(existing_target, key, value)
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
