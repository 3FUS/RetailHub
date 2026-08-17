from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.schemas.commission import CommissionStaffCreate, BatchApprovedCommission, FiscalPeriodUpdate, StoreTypeUpdate, \
    WithdrawnCommission, UpdateOpeningDay
from app.schemas.target import StaffAttendanceUpdate, StaffPlannedAttendanceUpdate
from app.services.commission_service import CommissionService, CommissionDataHubService
from app.database import get_db
from sqlalchemy.exc import SQLAlchemyError
from app.core.security import get_current_user

from app.utils.logger import app_logger

router = APIRouter()


# @router.post("/create")
# async def create_commission(commission: CommissionCreate, db: AsyncSession = Depends(get_db)):
#     return await CommissionService.create_commission(db, commission)


@router.get("/commission_tiers", tags=["DataHub"])
async def get_commission_tiers(fiscal_month: str, store_code: str, staff_code: str,
                               db: AsyncSession = Depends(get_db),
                               current_user: dict = Depends(get_current_user)):
    try:
        data = await CommissionDataHubService.get_commission_tiers_by_staff(db, fiscal_month, store_code, staff_code)
        return {"code": 200, "data": data}
    except SQLAlchemyError as e:
        app_logger.error(f"get_commission_tiers Database error: {str(e)}")
        return {"code": 500, "msg": "Database error occurred while fetching commission tiers"}
    except Exception as e:
        app_logger.error(f"get_commission_tiers An error occurred: {str(e)}")
        return {"code": 500, "msg": f"An error occurred while fetching commission tiers: {str(e)}"}


@router.put("/update")
async def update_commission(attendance_update: StaffAttendanceUpdate,
                            db: AsyncSession = Depends(get_db),
                            current_user: dict = Depends(get_current_user)):
    try:
        role_code = current_user['user_code']
        if await CommissionService.update_commission(db, attendance_update, role_code):
            data = await CommissionService.calculate_commissions_for_store(db, attendance_update.store_code,
                                                                           attendance_update.fiscal_month, role_code)
            return {"code": 200, "data": data, "msg": "Success"}
        else:
            app_logger.warning(f"An error occurred while fetching targets")
            return {"code": 500, "msg": "An error occurred while fetching targets"}
    except SQLAlchemyError as e:
        app_logger.error(f"An error occurred while fetching targets: {str(e)}")
        return {"code": 500, "msg": "An error occurred while fetching targets"}
    except Exception as e:
        app_logger.error(f"An error occurred while fetching targets: {str(e)}")
        return {"code": 500, "msg": "An error occurred while fetching targets"}


@router.get("/list")
async def get_commissions_by_key(fiscal_month: str, key_word: str = None, status: str = 'All',
                                 db: AsyncSession = Depends(get_db),
                                 current_user: dict = Depends(get_current_user)):
    try:
        role_code = current_user['user_code']
        data = await CommissionService.get_all_commissions_by_key(role_code, fiscal_month, key_word, status, db)
        app_logger.info(f"get_commissions_by_key: {role_code} {fiscal_month} {key_word}")
        return {"code": 200, "data": data['data'], "status_counts": data['status_counts'],
                "field_translations": data['field_translations'],
                "MonthEnd": data['MonthEnd'], "fiscal_month": fiscal_month}

    except SQLAlchemyError as e:
        app_logger.error(f"get_commissions_by_key An error occurred while fetching targets: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred while fetching targets"
        )
    except Exception as e:
        app_logger.error(f"get_commissions_by_key An error occurred while fetching targets: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching targets: {str(e)}"
        )


@router.get("/staff_commission_ranking", tags=["DataHub"])
async def get_staff_commission_ranking(fiscal_month: str, org1: str, org2: str, org3: str,
                                       org4: str,
                                       cursor: int = 0, limit: int = 30,
                                       rank_type: str = 'ALL',
                                       sort_by: str = 'commissions',
                                       is_prod: bool = False,
                                       db: AsyncSession = Depends(get_db),
                                       current_user: dict = Depends(get_current_user)):
    try:
        role_code = current_user['user_code']
        primary_group = current_user['primary_group']
        app_logger.info(f"primary_group: {primary_group}")
        data = await CommissionDataHubService.get_staff_commissions_list_by_role(
            db, role_code, fiscal_month, org1, org2, org3, org4,
            cursor, limit, rank_type, sort_by, primary_group
        )
        return {"code": 200, "data": data}
    except SQLAlchemyError as e:
        app_logger.error(f"get_staff_commission_ranking Database error: {str(e)}")
        return {"code": 500, "msg": "Database error occurred while fetching staff commission ranking"}
    except Exception as e:
        app_logger.error(f"get_staff_commission_ranking An error occurred: {str(e)}")
        return {"code": 500, "msg": f"An error occurred while fetching staff commission ranking: {str(e)}"}


@router.put("/update_planned_attendance", tags=["DataHub"])
async def update_planned_attendance(request: StaffPlannedAttendanceUpdate,
                                    db: AsyncSession = Depends(get_db),
                                    current_user: dict = Depends(get_current_user)):
    try:
        result = await CommissionDataHubService.update_planned_attendance(
            db, request.store_code, request.fiscal_month, request.staff_code,
            request.planned_attendance
        )
        if result:
            return {"code": 200, "msg": "Planned attendance updated successfully"}
        else:
            return {"code": 404, "msg": "Staff record not found"}
    except SQLAlchemyError as e:
        app_logger.error(f"update_planned_attendance Database error: {str(e)}")
        return {"code": 500, "msg": "Database error occurred"}
    except Exception as e:
        app_logger.error(f"update_planned_attendance error: {str(e)}")
        return {"code": 500, "msg": f"An error occurred: {str(e)}"}


@router.get("/store_performance")
async def get_store_performance(fiscal_month: str, store_code: str,
                                db: AsyncSession = Depends(get_db),
                                current_user: dict = Depends(get_current_user)):
    try:
        data = await CommissionService.get_store_performance(db, store_code, fiscal_month)
        return {"code": 200, "data": data}
    except SQLAlchemyError as e:
        app_logger.error(f"get_store_performance An error occurred while fetching targets: {str(e)}")
        # raise HTTPException(
        #     status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        #     detail="Database error occurred while fetching targets"
        # )
        return {"code": 500, "msg": "Database error occurred while fetching targets"}
    except Exception as e:
        app_logger.error(f"get_store_performance An error occurred while fetching targets: {str(e)}")
        # raise HTTPException(
        #     status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        #     detail=f"An error occurred while fetching targets: {str(e)}"
        # )
        return {"code": 500, "msg": "An error occurred while fetching targets"}


@router.get("/staff_commission_detail")
async def get_commission_detail(fiscal_month: str, store_code: str, staff_code: str,
                                db: AsyncSession = Depends(get_db),
                                current_user: dict = Depends(get_current_user)):
    try:
        data, expected_attendance, actual_attendance, position, staff_target_value, staff_sales_value, store_target, store_sales = await CommissionService.get_commission_by_staff_code(
            db, staff_code, store_code, fiscal_month)
        data_info = await CommissionService.get_commission_by_store_code(db, store_code, fiscal_month, staff_code)
        data_info['expected_attendance'] = expected_attendance
        data_info['actual_attendance'] = actual_attendance
        data_info['position'] = position

        return {"code": 200, "data": data, "data_info": data_info}
    except SQLAlchemyError as e:
        app_logger.error(f"get_commission_detail An error occurred while fetching targets: {str(e)}")
        return {"code": 500, "msg": "Database error occurred while fetching targets"}
    except Exception as e:
        app_logger.error(f"get_commission_detail An error occurred while fetching targets: {str(e)}")
        return {"code": 500, "msg": "An error occurred while fetching targets"}


@router.delete("/delete_adjustment")
async def delete_adjustment(fiscal_month: str, store_code: str, staff_code: str,
                            db: AsyncSession = Depends(get_db),
                            current_user: dict = Depends(get_current_user)):
    try:
        # 调用服务层删除调整记录
        result = await CommissionService.delete_adjustment(db, fiscal_month, store_code, staff_code)
        if result:
            return {"code": 200, "msg": "Adjustment deleted successfully"}
        else:
            return {"code": 404, "msg": "No adjustment found for the given parameters"}
    except SQLAlchemyError as e:
        app_logger.error(f"delete_adjustment Database error: {str(e)}")
        return {"code": 500, "msg": "Database error occurred while deleting adjustment"}
    except Exception as e:
        app_logger.error(f"delete_adjustment An error occurred: {str(e)}")
        return {"code": 500, "msg": f"An error occurred while deleting adjustment: {str(e)}"}


@router.post("/add_adjustment")
async def add_adjustment(add_adjustment: CommissionStaffCreate, db: AsyncSession = Depends(get_db)):
    try:
        data = await CommissionService.create_add_adjustment(db, add_adjustment)
        return {"code": 200, "data": data, "msg": "Success"}

    except SQLAlchemyError as e:
        app_logger.error(f"add_adjustment An error occurred while fetching targets: {str(e)}")
        return {"code": 500, "msg": f"add_adjustment An error occurred while fetching targets: {str(e)}"}
    except Exception as e:
        app_logger.error(f"add_adjustment An error occurred while fetching targets: {str(e)}")
        return {"code": 500, "msg": f"An error occurred while fetching targets {str(e)}"}


@router.post("/update_opening_day")
async def update_opening_day(request: UpdateOpeningDay, db: AsyncSession = Depends(get_db)):
    try:
        data = await CommissionService.update_opening_day(db, request.fiscal_month, request.store_code,
                                                          request.opening_days)
        return {"code": 200, "data": data, "msg": "Success"}

    except SQLAlchemyError as e:
        app_logger.error(f"update_opening_day An error occurred while fetching targets: {str(e)}")
        return {"code": 500, "msg": f"update_opening_day An error occurred while fetching targets"}


@router.post("/batch_Approved")
async def batch_audit_commission(request: BatchApprovedCommission, db: AsyncSession = Depends(get_db),
                                 current_user: dict = Depends(get_current_user)):
    try:
        role_code = current_user['user_code']
        result = await CommissionService.batch_approved_commission_by_store_codes(
            db, request, role_code
        )
        return {"code": 200, "data": result, "msg": f"Successfully {request.status} commissions"}
    except ValueError as e:
        return {"code": 404, "msg": str(e)}
    except SQLAlchemyError as e:
        return {"code": 500, "msg": "Database error occurred while processing batch audit"}
    except Exception as e:
        return {"code": 500, "msg": f"An error occurred while processing batch audit: {str(e)}"}


@router.post("/withdrawn")
async def withdrawn_commission(request: WithdrawnCommission, db: AsyncSession = Depends(get_db),
                               current_user: dict = Depends(get_current_user)):
    try:
        result = await CommissionService.withdrawn_commission(request.fiscal_month, request.store_code,
                                                              db
                                                              )
        return {"code": 200, "data": result, "msg": f"Successfully withdrawn commission"}
    except ValueError as e:
        return {"code": 404, "msg": str(e)}


@router.post("/update_store_type")
async def update_store_type(request: StoreTypeUpdate, db: AsyncSession = Depends(get_db),
                            current_user: dict = Depends(get_current_user)):
    try:
        result = await CommissionService.update_store_type(
            db, request.fiscal_month, request.store_code, request.store_type
        )
        return {"code": 200, "data": result, "msg": f"Successfully updated store type"}
    except ValueError as e:
        app_logger.error(f"update_store_type An error occurred while fetching targets: {str(e)}")
        return {"code": 404, "msg": str(e)}


@router.post("/update_fiscal_period")
async def up_fiscal_period(request: FiscalPeriodUpdate, db: AsyncSession = Depends(get_db),
                           current_user: dict = Depends(get_current_user)):
    try:
        result = await CommissionService.update_fiscal_period(
            db, request.fiscal_month, request.store_code, request.fiscal_period
        )
        return {"code": 200, "data": result, "msg": f"Successfully updated fiscal period"}
    except ValueError as e:
        app_logger.error(f"update_fiscal_period An error occurred while fetching targets: {str(e)}")
        return {"code": 404, "msg": str(e)}
    except Exception as e:
        app_logger.error(f"update_fiscal_period An error occurred while fetching targets: {str(e)}")
        return {"code": 500, "msg": "error update_fiscal_period"}


#
@router.post("/month_end")
async def add_month_end(fiscal_month: str, db: AsyncSession = Depends(get_db),
                        current_user: dict = Depends(get_current_user)):
    try:
        role_code = current_user['user_code']
        result = await CommissionService.add_month_end(db, fiscal_month, role_code)
        return {"code": 200, "data": result, "msg": "Month end record created/updated successfully"}
    except SQLAlchemyError as e:
        app_logger.error(f"add_month_end Database error: {str(e)}")
        return {"code": 500, "msg": "Database error occurred while processing month end"}
    except Exception as e:
        app_logger.error(f"add_month_end An error occurred: {str(e)}")
        return {"code": 500, "msg": f"An error occurred while processing month end: {str(e)}"}
