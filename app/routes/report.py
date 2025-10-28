from fastapi import APIRouter, Depends, Query, Response
import pandas as pd
from io import BytesIO
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.services.target_service import TargetRPTService
from app.services.commission_service import CommissionRPTService
from app.services.budget_service import BudgetService
from app.core.security import get_current_user
from app.utils.logger import app_logger

router = APIRouter()

# 报表类型枚举
REPORT_TYPES = ["target_by_store", "target_percentage_version", "target_bi_version", "target_date_horizontal_version",
                "target_by_staff", "commission", "budget", "sales_by_achievement", "commission_payout"]


@router.get("/data")
async def get_report_data(
        financial_month: str = Query(..., description="财月，格式如：2025-9"),
        status: str = Query('approved'),
        report_type: str = Query("target_by_store", description=f"报表类型: {', '.join(REPORT_TYPES)}"),
        keyword: str = Query(None, description="门店代码或名称模糊查询"),
        format: str = Query("json", description="返回格式: json 或 excel"),
        session: AsyncSession = Depends(get_db),
        current_user: dict = Depends(get_current_user)
):
    """
    获取报表数据，支持多种报表类型
    - financial_month: 财月（必需）
    - keyword: 门店代码或名称模糊查询（可选）
    """
    if report_type not in REPORT_TYPES:
        return {"code": 400, "msg": f"Invalid report_type. Must be one of: {', '.join(REPORT_TYPES)}"}

    # 根据报表类型获取数据
    report_data = {}
    role_code = current_user['user_code']

    try:
        if report_type == "target_by_store":
            report_data["target_by_store"] = await TargetRPTService.get_rpt_target_by_store(
                session, financial_month, keyword, status, role_code)

        elif report_type == "target_percentage_version":
            report_data["target_percentage_version"] = await TargetRPTService.get_rpt_target_by_store(
                session, financial_month, keyword, status, role_code)

        elif report_type == "target_bi_version":
            report_data["target_bi_version"] = await TargetRPTService.get_rpt_target_bi_version(
                session, financial_month, keyword, status, role_code)

        elif report_type == "target_date_horizontal_version":
            report_data[
                "target_date_horizontal_version"] = await TargetRPTService.get_rpt_target_date_horizontal_version(
                session, financial_month, keyword, status, role_code)

        elif report_type == "target_by_staff":
            report_data["target_by_staff"] = await TargetRPTService.get_rpt_target_by_staff(
                session, financial_month, keyword, status, role_code)

        elif report_type == "commission":
            report_data["commission"] = await CommissionRPTService.get_rpt_commission_by_store(
                session, financial_month, keyword, status, role_code)

        elif report_type == "sales_by_achievement":
            report_data["sales_by_achievement"] = await CommissionRPTService.get_rpt_sales_by_achievement(
                session, financial_month, keyword, status, role_code)

        elif report_type == "commission_payout":
            report_data["commission_payout"] = await CommissionRPTService.get_rpt_commission_payout(
                session, financial_month, keyword, status, role_code)

        elif report_type == "budget":
            report_data["budget"] = await BudgetService.get_budget_data(
                session, financial_month, keyword, status, role_code)

        # 添加元数据
        report_data.update({
            "financial_month": financial_month,
            "report_type": report_type,
            "keyword": keyword
        })

        # 根据格式返回数据
        if format.lower() == 'excel':
            return _export_to_excel(report_data, report_type,status)
        else:
            return {"code": 200, "data": report_data}

    except Exception as e:
        app_logger.error(f"Error generating report: {str(e)}")
        return {"code": 500, "msg": f"Error generating report: {str(e)}"}


def _export_to_excel(report_data: dict, report_type: str,status: str):
    """
    将报告数据导出为 Excel 文件，使用 field_translations 的英文字段名作为表头
    """
    output = BytesIO()

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        financial_month = report_data.get("financial_month", "unknown")
        keyword = report_data.get("keyword", "")

        # 通用的数据处理函数，支持使用 field_translations 设置表头
        def _write_data_to_sheet(data, sheet_name):

            if isinstance(data, dict) and "data" in data:
                details = data.get("data", [])
                if details:
                    df = pd.DataFrame(details)
                    # 如果有 field_translations，使用英文字段名作为表头
                    if "field_translations" in data and data["field_translations"]:
                        field_translations = data["field_translations"]
                        # 创建列名映射（从原始字段名到英文字段名）
                        column_mapping = {
                            old_name: translations["en"]
                            for old_name, translations in field_translations.items()
                            if "en" in translations
                        }
                        # 重命名列
                        df.rename(columns=column_mapping, inplace=True)
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                else:
                    df = pd.DataFrame([data])
                    # 如果有 field_translations，使用英文字段名作为表头
                    if "field_translations" in data and data["field_translations"]:
                        field_translations = data["field_translations"]
                        column_mapping = {
                            old_name: translations["en"]
                            for old_name, translations in field_translations.items()
                            if "en" in translations
                        }
                        df.rename(columns=column_mapping, inplace=True)
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            elif isinstance(data, list):
                df = pd.DataFrame(data)
                # 如果列表不为空且有字段翻译信息，尝试应用翻译
                if data and isinstance(data[0], dict):
                    # 这里无法直接获取 field_translations，需要在调用时确保传入正确的数据结构
                    pass
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            else:
                df = pd.DataFrame([data])
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        # 根据报表类型导出不同sheet
        if report_type in ["target_by_store", "target_percentage_version",
                           "target_bi_version", "target_date_horizontal_version"] and report_type in report_data:
            _write_data_to_sheet(report_data[report_type], "Sheet1")

        elif report_type == "target_by_staff" and "target_by_staff" in report_data:
            _write_data_to_sheet(report_data["target_by_staff"], "Sheet1")

        elif report_type == "commission_payout" and "commission_payout" in report_data:
            _write_data_to_sheet(report_data["commission_payout"], "Sheet1")

        elif report_type == "commission" and "commission" in report_data:
            _write_data_to_sheet(report_data["commission"], "Sheet1")

        elif report_type == "sales_by_achievement" and "sales_by_achievement" in report_data:
            _write_data_to_sheet(report_data["sales_by_achievement"], "Sheet1")

        elif report_type == "budget" and "budget" in report_data:
            _write_data_to_sheet(report_data["budget"], "Upload_Budget")

    output.seek(0)

    filename_keyword = f"_{keyword}" if keyword else ""
    headers = {
        'Content-Disposition': f'attachment; filename="report_{financial_month}{filename_keyword}_{report_type}.xlsx"',
        'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    }

    return Response(content=output.getvalue(), headers=headers)
