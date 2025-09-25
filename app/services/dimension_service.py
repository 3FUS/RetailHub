from sqlalchemy.ext.asyncio import AsyncSession
from app.models.dimension import StoreTypeModel
from app.models.staff import PositionModel, StaffModel
from sqlalchemy.future import select


class DimensionService:
    @staticmethod
    async def get_store_type(db: AsyncSession):
        query = select(StoreTypeModel)
        result = await db.execute(query)
        store_types = result.scalars().all()
        # 转换为JSON格式
        return [{"store_type_code": st.store_type_code, "store_type_name": st.store_type_name} for st in store_types]

    @staticmethod
    async def get_position(db: AsyncSession):
        query = select(PositionModel.position, PositionModel.default_coefficient).where(PositionModel.is_active == 1)
        result = await db.execute(query)
        positions = result.fetchall()
        # 转换为JSON格式
        return [{"position": pos.position, "default_coefficient": pos.default_coefficient} for pos in positions]

    @staticmethod
    async def get_staff_name(db: AsyncSession, staff_code: str):
        query = select(StaffModel.first_name, StaffModel.last_name).where(StaffModel.staff_code == staff_code)
        result = await db.execute(query)
        staff = result.fetchone()
        if staff:
            return staff.first_name
        else:
            return None
