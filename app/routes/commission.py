from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from app.schemas.commission import CommissionCreate, CommissionUpdate, CommissionStaffCreate
from app.schemas.target import StaffAttendanceUpdate
from app.services.commission_service import CommissionService
from app.database import get_db
from sqlalchemy.exc import SQLAlchemyError

router = APIRouter()


@router.post("/create")
async def create_commission(commission: CommissionCreate, db: AsyncSession = Depends(get_db)):
    return await CommissionService.create_commission(db, commission)


@router.put("/update")
async def update_commission(attendance_update: StaffAttendanceUpdate,
                            db: AsyncSession = Depends(get_db)):
    try:

        if await CommissionService.update_commission(db, attendance_update):
            data = await CommissionService.calculate_commissions_for_store(db, attendance_update.store_code, attendance_update.fiscal_month)
            return {"code": 200, "data": data, "msg": "Success"}
        else:
            return {"code": 500, "msg": "An error occurred while fetching targets"}
    except SQLAlchemyError as e:
        return {"code": 500, "msg": "An error occurred while fetching targets"}
    except Exception as e:
        return {"code": 500, "msg": "An error occurred while fetching targets"}


@router.get("/list")
async def get_commissions_by_key(fiscal_month: str, key_word: str = None, status: str = 'ALL',
                                 db: AsyncSession = Depends(get_db)):
    try:
        data = await CommissionService.get_all_commissions_by_key(fiscal_month, key_word, db)
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


@router.get("/staff_commission_detail")
async def get_commission_detail(fiscal_month: str, store_code: str, staff_code: str,
                                db: AsyncSession = Depends(get_db)):
    try:
        data = await CommissionService.get_commission_by_staff_code(db, staff_code, store_code, fiscal_month)
        return {"code": 200, "data": data}
    except SQLAlchemyError as e:
        # raise HTTPException(
        #     status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        #     detail="Database error occurred while fetching targets"
        # )
        return {"code": 500, "msg": "Database error occurred while fetching targets"}
    except Exception as e:
        # raise HTTPException(
        #     status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        #     detail=f"An error occurred while fetching targets: {str(e)}"
        # )
        return {"code": 500, "msg": "An error occurred while fetching targets"}


@router.post("/add_adjustment")
async def add_adjustment(add_adjustment: CommissionStaffCreate, db: AsyncSession = Depends(get_db)):
    try:
        data = await CommissionService.create_add_adjustment(db, add_adjustment)
        return {"code": 200, "data": data, "msg": "Success"}

    except SQLAlchemyError as e:
        return {"code": 500, "msg": "An error occurred while fetching targets"}
    except Exception as e:
        return {"code": 500, "msg": "An error occurred while fetching targets"}


@router.post("/audit/{id}")
async def audit_commission(id: int, db: AsyncSession = Depends(get_db)):
    return await CommissionService.audit_commission(db, id)


@router.post("/unaudit/{id}")
async def unaudit_commission(id: int, db: AsyncSession = Depends(get_db)):
    return await CommissionService.unaudit_commission(db, id)
