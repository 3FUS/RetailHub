from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from typing import List

from app.schemas.target import TargetStoreWeekCreate, TargetStoreDailyCreate, TargetStoreUpdate, StaffAttendanceCreate
from app.services.target_service import TargetStoreService, TargetStoreWeekService, TargetStoreDailyService, \
    TargetStaffService
from app.database import get_db

router = APIRouter()


#
@router.put("/update/{store_code}/{fiscal_month}")
async def update_target(store_code: str, fiscal_month: str, target: TargetStoreUpdate,
                        db: AsyncSession = Depends(get_db)):
    try:
        return await TargetStoreService.update_target(db, store_code, fiscal_month, target)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except SQLAlchemyError as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred while updating target"
        )
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while updating target: {str(e)}"
        )


@router.get("/list")
async def get_targets(fiscal_month: str, key_word: str = None, store_status: str = 'ALL', staff_status: str = 'ALL', db: AsyncSession = Depends(get_db)):
    try:
        data = await TargetStoreService.get_all_target_stores_by_key(fiscal_month, key_word, db)
        return {"code": 200, "data": data}

    except SQLAlchemyError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred while fetching targets"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching targets: {str(e)}"
        )


@router.post("/create_week")
async def create_week_target(TargetStoreWeek: TargetStoreWeekCreate, db: AsyncSession = Depends(get_db)):
    try:
        data= await TargetStoreWeekService.create_target_store_week(db, TargetStoreWeek)
        return {"code": 200, "data": data, "msg": "Success"}
    except SQLAlchemyError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred while creating week targets"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching targets: {str(e)}"
        )


@router.get("/get_week")
async def get_week_target(store_code: str, fiscal_month: str, db: AsyncSession = Depends(get_db)):
    try:
        data = await TargetStoreWeekService.get_target_store_week(db, store_code, fiscal_month)
        return {"code": 200, "data": data}
    except SQLAlchemyError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred while fetching week targets"
        )
    except Exception as e:
        raise HTTPException()


@router.post("/create_daily")
async def create_daily_target(TargetStoreDaily: TargetStoreDailyCreate, db: AsyncSession = Depends(get_db)):
    try:
        data = await TargetStoreDailyService.create_target_store_daily(db, TargetStoreDaily)
        return {"code": 200, "data": data, "msg": "Success"}
    except SQLAlchemyError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred while creating daily targets"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching targets: {str(e)}"
        )


@router.get("/get_daily")
async def get_daily_target(store_code: str, fiscal_month: str, db: AsyncSession = Depends(get_db)):
    try:
        target_data = await TargetStoreDailyService.get_target_store_daily(db, store_code, fiscal_month)
        return {"code": 200, "data": target_data}
    except SQLAlchemyError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred while fetching daily targets"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching targets: {str(e)}"
        )


@router.post("/create_staff_attendance")
async def create_staff_attendance(TargetStaffAttendance: StaffAttendanceCreate,
                                  db: AsyncSession = Depends(get_db)):
    try:
        data = await TargetStaffService.create_staff_attendance(db, TargetStaffAttendance)
        return {"code": 200, "data": data, "msg": "Success"}
    except SQLAlchemyError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred while creating staff attendance targets"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching targets: {str(e)}"
        )


@router.get("/get_staff_attendance")
async def get_staff_attendance(fiscal_month: str, store_code: str, db: AsyncSession = Depends(get_db)):
    try:
        data = await TargetStaffService.get_staff_attendance(db, fiscal_month, store_code)
        return {"code": 200, "data": data}
    except SQLAlchemyError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred while fetching staff attendance targets"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching targets: {str(e)}"
        )


@router.delete("/delete_staff_attendance")
async def delete_staff_attendance(fiscal_month: str, store_code: str, staff_code: str,
                                  db: AsyncSession = Depends(get_db)):
    try:
        data = await TargetStaffService.delete_staff_attendance(db, fiscal_month, store_code, staff_code)
        return {"code": 200, "data": data, "msg": "Success"}
    except SQLAlchemyError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred while deleting staff attendance targets"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching targets: {str(e)}"
        )
