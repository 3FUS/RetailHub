from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from app.schemas.target import TargetStoreWeekCreate, TargetStoreDailyCreate, StaffAttendanceCreate, \
    BatchApprovedTarget, WithdrawnTarget
from app.services.target_service import TargetStoreService, TargetStoreWeekService, TargetStoreDailyService, \
    TargetStaffService
from app.services.commission_service import CommissionService
from app.database import get_db
from app.core.security import get_current_user

from app.utils.logger import app_logger

router = APIRouter()


@router.get("/list")
async def get_targets(fiscal_month: str, key_word: str = None,
                      store_status: str = 'ALL', staff_status: str = 'ALL',
                      db: AsyncSession = Depends(get_db),
                      current_user: dict = Depends(get_current_user)):
    try:
        role_code = current_user['user_code']
        approve = current_user['approve']
        data = await TargetStoreService.get_all_target_stores_by_key(role_code, fiscal_month, key_word, db,approve)
        return {"code": 200, "data": data['data'],
                "field_translations": data['field_translations'],
                "MonthEnd": data['MonthEnd'], "fiscal_month": fiscal_month}

    except SQLAlchemyError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred while fetching targets"
        )
    except Exception as e:
        app_logger.error(f"get_targets list Exception {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching targets: {str(e)}"
        )


@router.post("/create_week")
async def create_week_target(TargetStoreWeek: TargetStoreWeekCreate, db: AsyncSession = Depends(get_db),
                             current_user: dict = Depends(get_current_user)):
    try:
        data = await TargetStoreWeekService.create_target_store_week(db, TargetStoreWeek)
        return {"code": 200, "data": data, "msg": "Success"}
    except SQLAlchemyError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred while creating week targets"
        )
    except Exception as e:
        app_logger.error(f"create_week_target Exception {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching targets: {str(e)}"
        )


@router.get("/get_week")
async def get_week_target(store_code: str, fiscal_month: str, db: AsyncSession = Depends(get_db),
                          current_user: dict = Depends(get_current_user)):
    try:
        data = await TargetStoreWeekService.get_target_store_week(db, store_code, fiscal_month)
        return {"code": 200, "data": data}
    except SQLAlchemyError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred while fetching week targets"
        )
    except Exception as e:
        app_logger.error(f"get_week_target Exception {str(e)}")
        raise HTTPException()


@router.post("/create_daily")
async def create_daily_target(TargetStoreDaily: TargetStoreDailyCreate, db: AsyncSession = Depends(get_db),
                              current_user: dict = Depends(get_current_user)):
    try:
        role_code = current_user['user_code']
        data = await TargetStoreDailyService.create_target_store_daily(db, TargetStoreDaily, role_code)
        # if TargetStoreDaily.store_status == "submitted":
        #     await TargetStoreService.update_target_store_daily(db, TargetStoreDaily.store_code,
        #                                                        TargetStoreDaily.fiscal_month)

        return {"code": 200, "data": data, "msg": "Success"}
    except SQLAlchemyError as e:

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred while creating daily targets"
        )
    except Exception as e:
        app_logger.error(f"create_daily Exception {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching targets: {str(e)}"
        )


@router.get("/get_daily")
async def get_daily_target(store_code: str, fiscal_month: str, db: AsyncSession = Depends(get_db),
                           current_user: dict = Depends(get_current_user)):
    try:
        app_logger.info(current_user)
        approve = current_user['approve']
        target_data = await TargetStoreDailyService.get_target_store_daily(db, store_code, fiscal_month,approve)
        return {"code": 200, "data": target_data['data'], "header_info": target_data['header_info'],
                "MonthEnd": target_data['MonthEnd']}
    except SQLAlchemyError as e:
        app_logger.error(f"get_daily_target SQLAlchemyError {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred while fetching daily targets"
        )
    except Exception as e:
        app_logger.error(f"get_daily_target Exception {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching targets: {str(e)}"
        )


@router.post("/create_staff_attendance")
async def create_staff_attendance(TargetStaffAttendance: StaffAttendanceCreate,
                                  db: AsyncSession = Depends(get_db),
                                  current_user: dict = Depends(get_current_user)):
    try:
        role_code = current_user['user_code']
        data = await TargetStaffService.create_staff_attendance(db, TargetStaffAttendance, role_code)
        await TargetStaffService.update_staff(db, TargetStaffAttendance)
        return {"code": 200, "data": data, "msg": "Success"}
    except SQLAlchemyError as e:
        app_logger.error(f"create_staff_attendance SQLAlchemyError {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred while creating staff attendance targets"
        )
    except Exception as e:
        app_logger.error(f"create_staff_attendance Exception {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching targets: {str(e)}"
        )


@router.get("/get_staff_attendance")
async def get_staff_attendance_details(fiscal_month: str, store_code: str, db: AsyncSession = Depends(get_db),
                                       module: str = "target",
                                       current_user: dict = Depends(get_current_user)):
    try:
        approve = current_user['approve']
        data = await TargetStaffService.get_staff_attendance(db, fiscal_month, store_code, module,approve)
        return {"code": 200, "data": data['data'], "header_info": data['header_info'], "MonthEnd": data['MonthEnd']}
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
                                  db: AsyncSession = Depends(get_db), current_user: dict = Depends(get_current_user)):
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


@router.post("/batch_Approved")
async def batch_audit_target(request: BatchApprovedTarget, db: AsyncSession = Depends(get_db),
                             current_user: dict = Depends(get_current_user)):
    try:
        app_logger.info(f"batch_audit_target {request}")
        role_code = current_user['user_code']
        await CommissionService.create_commission(db, request.fiscal_month, request.store_codes)

        if request.store_status and request.store_status == "approved":
            for store in request.store_codes:
                await TargetStoreDailyService.update_target_monthly_percentage(db, store,
                                                                               request.fiscal_month)

        result = await TargetStoreService.batch_approved_target_by_store_codes(
            db, request, role_code
        )
        app_logger.info(f"batch_audit_target {result}")
        return {"code": 200, "data": result, "msg": f"Successfully target"}
    except ValueError as e:
        app_logger.error(f"batch_audit_target {e}")
        return {"code": 404, "msg": str(e)}
    except SQLAlchemyError as e:
        app_logger.error(f"batch_audit_target {e}")
        return {"code": 500, "msg": "Database error occurred while processing batch audit"}
    except Exception as e:
        app_logger.error(f"batch_audit_target {e}")
        return {"code": 500, "msg": f"An error occurred while processing batch audit: {str(e)}"}


@router.post("/withdrawn")
async def withdrawn_target(request: WithdrawnTarget, db: AsyncSession = Depends(get_db),
                           current_user: dict = Depends(get_current_user)):
    try:
        result = await TargetStoreService.withdrawn_target(
            db, request
        )
        return {"code": 200, "data": result, "msg": f"Successfully withdrawn taget"}
    except ValueError as e:
        return {"code": 404, "msg": str(e)}
    except SQLAlchemyError as e:
        return {"code": 500, "msg": "Database error occurred while processing batch audit"}
    except Exception as e:
        return {"code": 500, "msg": f"An error occurred while processing batch audit: {str(e)}"}
