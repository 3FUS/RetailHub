from fastapi import APIRouter, Depends
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db

from app.services.dimension_service import DimensionService

router = APIRouter()


@router.get("/store_type")
async def get_store_type(db: AsyncSession = Depends(get_db)):
    try:
        data = await DimensionService.get_store_type(db)
        return {"code": 200, "data": data}
    except SQLAlchemyError as e:
        return {"message": str(e)}
    except Exception as e:
        return {"message": str(e)}


@router.get("/position")
async def get_position(db: AsyncSession = Depends(get_db)):
    try:
        data = await DimensionService.get_position(db)
        return {"code": 200, "data": data}
    except SQLAlchemyError as e:
        return {"message": str(e)}
    except Exception as e:
        return {"message": str(e)}


@router.get("/staff_name")
async def staff_name(staff_code: str, db: AsyncSession = Depends(get_db)):
    try:
        data = await DimensionService.get_staff_name(db, staff_code)
        return {"code": 200, "data": data}
    except SQLAlchemyError as e:
        return {"message": str(e)}
    except Exception as e:
        return {"message": str(e)}


@router.get("/document_status")
async def get_document_status(template_type: str = "target"):
    """
    获取单据状态列表

    Args:
        template_type: 模板类型 ("target" 或 "commission")

    Returns:
        包含中英文状态的列表
    """
    try:
        if template_type == "target":
            # 目标模板状态
            data = [
                {"en": "all", "zh": "全部"},
                {"en": "saved", "zh": "已保存"},
                {"en": "submitted", "zh": "已提交"},
                {"en": "approved", "zh": "已审核"},
                {"en": "rejected", "zh": "已拒绝"}
            ]
        elif template_type == "commission":
            # 奖金模板状态
            data = [
                {"en": "all", "zh": "全部"},
                {"en": "saved", "zh": "已保存"},
                {"en": "submitted", "zh": "已提交"},
                {"en": "approved", "zh": "已审核"},
                {"en": "rejected", "zh": "已拒绝"}
            ]
        else:
            # 默认返回目标模板状态
            data = [
                {"en": "all", "zh": "全部"},
                {"en": "saved", "zh": "已保存"},
                {"en": "submitted", "zh": "已提交"},
                {"en": "approved", "zh": "已审核"},
                {"en": "rejected", "zh": "已拒绝"}
            ]

        return {"code": 200, "data": data}
    except Exception as e:
        return {"message": str(e)}
