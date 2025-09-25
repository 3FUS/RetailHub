from fastapi import APIRouter, Depends, Query, Response
import pandas as pd
from io import BytesIO
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.services.target_service import TargetRPTService
from app.services.commission_service import CommissionRPTService
from app.services.budget_service import BudgetService

router = APIRouter()

# 报表类型枚举
REPORT_TYPES = ["target_by_store", "target_by_staff", "commission", "budget"]


@router.get("/data")
async def get_report_data(
        financial_month: str = Query(..., description="财月，格式如：2025-9"),
        report_type: str = Query("target", description=f"报表类型: {', '.join(REPORT_TYPES)}"),
        keyword: str = Query(None, description="门店代码或名称模糊查询"),
        format: str = Query("json", description="返回格式: json 或 excel"),
        # page: int = Query(1, description="页码，从1开始", ge=1),
        # page_size: int = Query(20, description="每页记录数", ge=1, le=1000),
        session: AsyncSession = Depends(get_db)
):
    """
    获取报表数据，支持target、commission和budget
    - financial_month: 财月（必需）
    - keyword: 门店代码或名称模糊查询（可选）
    """
    if report_type not in REPORT_TYPES:
        return {"code": 400, "msg": f"Invalid report_type. Must be one of: {', '.join(REPORT_TYPES)}"}

    # 根据报表类型获取数据
    report_data = {}

    if report_type == "target_by_store":
        report_data["target_by_store"] = await TargetRPTService.get_rpt_target_by_store(session, financial_month,
                                                                                        keyword)

    if report_type == "target_by_staff":
        report_data["target_by_staff"] = await TargetRPTService.get_rpt_target_by_staff(session, financial_month,
                                                                                        keyword)

    if report_type == "commission":
        report_data["commission"] = await CommissionRPTService.get_rpt_commission_by_store(session, financial_month,
                                                                                           keyword)

    if report_type == "budget":
        report_data["budget"] = await BudgetService.get_budget_data(session, financial_month, keyword)

    # 添加元数据
    report_data.update({
        "financial_month": financial_month,
        "report_type": report_type,
        "keyword": keyword,
        "generated_at": pd.Timestamp.now().isoformat()
    })

    # 根据格式返回数据
    if format.lower() == 'excel':
        return _export_to_excel(report_data, report_type)
    else:
        return {"code": 200, "data": report_data}


def _export_to_excel(report_data: dict, report_type: str):
    """
    将报告数据导出为 Excel 文件
    """
    output = BytesIO()

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        financial_month = report_data.get("financial_month", "unknown")
        keyword = report_data.get("keyword", "")

        # 根据报表类型导出不同sheet
        if report_type == "target" and "target" in report_data:
            target_data = report_data["target"]
            target_details = target_data.get("data", []) if isinstance(target_data, dict) else []

            if target_details:
                target_df = pd.DataFrame(target_details)
                target_df.to_excel(writer, sheet_name='Target', index=False)
            else:
                pd.DataFrame([target_data]).to_excel(writer, sheet_name='Target', index=False)

        if report_type == "commission" and "commission" in report_data:
            commission_data = report_data["commission"]
            commission_details = commission_data.get("details", []) if isinstance(commission_data, dict) else []

            if commission_details:
                commission_df = pd.DataFrame(commission_details)
                commission_df.to_excel(writer, sheet_name='Commission', index=False)
            else:
                pd.DataFrame([commission_data]).to_excel(writer, sheet_name='Commission', index=False)

        if report_type == "budget" and "budget" in report_data:
            budget_data = report_data["budget"]

            # 对于预算数据，使用不同的处理方式
            if isinstance(budget_data, dict) and "data" in budget_data:
                budget_details = budget_data.get("data", [])
                if budget_details:
                    budget_df = pd.DataFrame(budget_details)
                    budget_df.to_excel(writer, sheet_name='Budget', index=False)
                else:
                    pd.DataFrame([budget_data]).to_excel(writer, sheet_name='Budget', index=False)
            elif isinstance(budget_data, list):
                budget_df = pd.DataFrame(budget_data)
                budget_df.to_excel(writer, sheet_name='Budget', index=False)
            else:
                pd.DataFrame([budget_data]).to_excel(writer, sheet_name='Budget', index=False)

    output.seek(0)

    filename_keyword = f"_{keyword}" if keyword else ""
    headers = {
        'Content-Disposition': f'attachment; filename="report_{financial_month}{filename_keyword}_{report_type}.xlsx"',
        'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    }

    return Response(content=output.getvalue(), headers=headers)
