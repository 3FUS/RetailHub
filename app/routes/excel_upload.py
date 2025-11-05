# routes/excel_upload.py
from datetime import datetime
from typing import List, Dict, Any, Tuple

from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import delete, text, select
from sqlalchemy.ext.asyncio import AsyncSession
import pandas as pd
import io

from app.database import get_db
from app.models.dimension import DimensionDayWeek
from app.models.sales import ECSalesModel
from app.models.staff import StaffAttendanceModel
from app.models.target import TargetStoreMain
from app.services.budget_service import BudgetService
from app.services.target_service import TargetStoreService, StaffTargetCalculator
from app.utils.logger import app_logger

router = APIRouter(prefix="/excel", tags=["excel"])


class ExcelPreviewResponse(BaseModel):
    data_type: str
    preview_data: List[Dict[str, Any]]
    total_rows: int
    columns: List[str]
    errors: List[str]


class ImportResult(BaseModel):
    success: bool
    message: str
    data_type: str
    rows_processed: int
    rows_with_errors: int = 0
    errors: List[str] = []


def identify_excel_type(df: pd.DataFrame) -> str:
    """识别Excel文件类型"""
    headers = [str(col).lower().strip() for col in df.columns]

    target_indicators = ['门店ID', '财月', '目标金额']
    budget_indicators = ['门店ID', '财月', '预算金额']
    ec_sales_indicators = ['订单号', '数量', '订单总金额', '员工ID', '门店ID', 'Week']

    if all(indicator.lower() in headers for indicator in target_indicators):
        return 'target'
    elif all(indicator.lower() in headers for indicator in budget_indicators):
        return 'budget'
    elif all(indicator.lower() in headers for indicator in ec_sales_indicators):
        return 'ec_sales'

    return 'unknown'


def parse_datetime_field(value) -> datetime:
    """解析时间字段"""
    if not value:
        return None

    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            try:
                return pd.to_datetime(value)
            except:
                return None
    elif isinstance(value, (pd.Timestamp, datetime)):
        return value

    return None


class ExcelImportService:
    """Excel导入服务类"""

    @staticmethod
    async def import_target_data(df: pd.DataFrame, db: AsyncSession) -> ImportResult:
        """导入目标数据"""
        target_updates = []

        for _, row in df.iterrows():
            store_code = row.get('门店ID') or row.get('store_code') or row.get('门店代码')
            fiscal_month = row.get('财月') or row.get('fiscal_month')
            target_value = row.get('目标金额') or row.get('target_value')

            if store_code and fiscal_month:
                target_updates.append({
                    'store_code': str(store_code),
                    'fiscal_month': str(fiscal_month),
                    'target_value': float(target_value) if target_value else 0
                })

        updated_targets = await TargetStoreService.batch_update_target_value(db, target_updates)

        await ExcelImportService._recalculate_staff_targets(db, target_updates)

        return ImportResult(
            success=True,
            message="成功导入目标数据",
            data_type="target",
            rows_processed=len(updated_targets)
        )

    @staticmethod
    async def _recalculate_staff_targets(db: AsyncSession, target_updates: list):
        """
        当门店目标值更新后，重新计算相关员工的目标值
        """
        for update_data in target_updates:
            store_code = update_data.get('store_code')
            fiscal_month = update_data.get('fiscal_month')
            store_target_value = update_data.get('target_value')

            # 获取该门店该财月的所有员工考勤记录
            result = await db.execute(
                select(StaffAttendanceModel)
                    .where(
                    StaffAttendanceModel.store_code == store_code,
                    StaffAttendanceModel.fiscal_month == fiscal_month
                )
            )
            staff_attendances = result.scalars().all()

            if not staff_attendances:
                continue

            # 提取所有员工的比例
            ratios = [attendance.target_value_ratio for attendance in staff_attendances if
                      attendance.target_value_ratio is not None]

            # 如果没有比例数据，跳过计算
            if not ratios:
                continue

            # 使用工具类计算新的员工目标值
            new_staff_targets = StaffTargetCalculator.calculate_staff_targets(store_target_value, ratios)

            # 更新每个员工的目标值
            for i, attendance in enumerate(staff_attendances):
                if i < len(new_staff_targets):
                    attendance.target_value = new_staff_targets[i]
                    attendance.updated_at = datetime.now()

            await db.commit()

            # 刷新对象
            for attendance in staff_attendances:
                await db.refresh(attendance)

    @staticmethod
    async def import_budget_data(df: pd.DataFrame, db: AsyncSession) -> ImportResult:
        """导入预算数据"""
        budget_updates = []

        for _, row in df.iterrows():
            store_code = row.get('门店ID') or row.get('store_code') or row.get('门店代码')
            fiscal_month = row.get('财月') or row.get('fiscal_month')
            budget_value = row.get('预算金额') or row.get('budget_value')

            if store_code and fiscal_month:
                budget_updates.append({
                    'store_code': str(store_code),
                    'fiscal_month': str(fiscal_month),
                    'budget_value': float(budget_value) if budget_value else 0
                })

        updated_budgets = await BudgetService.batch_update_budget_value(db, budget_updates)

        return ImportResult(
            success=True,
            message="成功导入预算数据",
            data_type="budget",
            rows_processed=len(updated_budgets)
        )

    @staticmethod
    async def import_ec_sales_data(df: pd.DataFrame, db: AsyncSession) -> ImportResult:
        """导入电商销售数据"""
        processed_count = 0
        error_count = 0
        errors = []

        # 收集所有订单号用于批量删除
        order_ids = set()
        valid_rows = []

        # 第一步：验证数据并收集订单ID
        for index, row in df.iterrows():
            try:
                order_id = row.get('订单号') or row.get('order_id')
                if not order_id:
                    errors.append(f"第{index + 1}行: 订单号为空")
                    error_count += 1
                    continue

                order_ids.add(str(order_id))
                valid_rows.append((index, row))

            except Exception as e:
                errors.append(f"第{index + 1}行处理出错: {str(e)}")
                error_count += 1
                continue

        # 第二步：删除已存在的记录
        if order_ids:
            stmt = delete(ECSalesModel).where(
                ECSalesModel.order_id.in_(list(order_ids))
            )
            await db.execute(stmt)

        # 第三步：批量插入新数据
        records_to_add = []
        ec_sales_summary = {}
        for index, row in valid_rows:
            try:
                # 提取并处理字段数据
                order_id = str(row.get('订单号') or row.get('order_id'))
                is_return = bool(row.get('是否退货', row.get('is_return', False)))
                recipient_name = str(row.get('收件人姓名', row.get('recipient_name', '')))[:60] or None
                order_source = str(row.get('订单来源', row.get('order_source', '')))[:30] or None
                payment_method = str(row.get('支付方式', row.get('payment_method', '')))[:30] or None
                order_status = str(row.get('订单状态', row.get('order_status', '')))[:30] or None
                quantity = int(row.get('数量', row.get('quantity', 0)) or 0)
                product_code = str(row.get('商品编码', row.get('product_code', '')))[:30] or None
                product_sku = str(row.get('商品SKU编码', row.get('product_sku', '')))[:30] or ''
                product_name = str(row.get('商品名称', row.get('product_name', '')))[:80] or None
                product_size = str(row.get('商品尺码', row.get('product_size', '')))[:30] or None
                total_amount = float(row.get('订单总金额', row.get('total_amount', 0.0)) or 0.0)
                staff_code = str(row.get('员工ID', row.get('staff_code', '')))[:30] or ''
                store_code = str(row.get('门店ID', row.get('store_code', '')))[:30] or ''
                area = str(row.get('Area', row.get('area', '')))[:30] or None
                week = row.get('Week') or row.get('week')
                is_wechat = bool(row.get('是否企微', row.get('is_wechat', False)))

                # 处理时间字段
                payment_time = parse_datetime_field(row.get('付款时间') or row.get('payment_time'))
                # shipping_time = parse_datetime_field(row.get('发货时间') or row.get('退货时间') or row.get('shipping_time'))

                # 创建记录对象
                record = ECSalesModel(
                    order_id=order_id,
                    is_return=is_return,
                    recipient_name=recipient_name,
                    order_source=order_source,
                    payment_method=payment_method,
                    order_status=order_status,
                    quantity=quantity,
                    product_code=product_code,
                    product_sku=product_sku,
                    product_name=product_name,
                    product_size=product_size,
                    total_amount=total_amount,
                    staff_code=staff_code,
                    store_code=store_code,
                    area=area,
                    payment_time=payment_time,
                    # shipping_time=shipping_time,
                    week=week,
                    is_wechat=is_wechat
                )

                records_to_add.append(record)
                processed_count += 1

                # 如果有付款时间，则计算财月并汇总线上销售数据
                if payment_time and staff_code and store_code:
                    # 格式化付款时间为年月日格式
                    payment_date_str = payment_time.strftime('%Y-%m-%d')

                    # 根据付款日期查找财月
                    query = select(DimensionDayWeek.fiscal_month).where(
                        DimensionDayWeek.week_number == week,
                        DimensionDayWeek.finance_year == 2025
                    )
                    result = await db.execute(query)
                    fiscal_month_result = result.fetchone()

                    if fiscal_month_result:
                        fiscal_month = fiscal_month_result[0]

                        # 汇总线上销售数据
                        key = (staff_code, store_code, fiscal_month)
                        if key in ec_sales_summary:
                            ec_sales_summary[key] += total_amount
                        else:
                            ec_sales_summary[key] = total_amount

            except Exception as e:
                errors.append(f"第{index + 1}行处理出错: {str(e)}")
                error_count += 1
                continue
        try:
            # 批量添加记录
            if records_to_add:
                db.add_all(records_to_add)

            # 提交事务
            await db.commit()

            updated_attendances = 0

            store_sales_summary = {}

            fiscal_months = set([fiscal_month for (_, _, fiscal_month) in ec_sales_summary.keys()])
            if fiscal_months:
                app_logger.debug(f"重置以下财月的员工电商销售数据: {fiscal_months}")
                # 查询这些财月的所有员工考勤记录
                reset_stmt = select(StaffAttendanceModel).where(
                    StaffAttendanceModel.fiscal_month.in_(list(fiscal_months))
                )
                reset_result = await db.execute(reset_stmt)
                reset_records = reset_result.scalars().all()

                # 重置所有相关记录的电商销售数据
                for record in reset_records:
                    record.sales_value_ec = 0.0
                    # 销售总额设为线下销售额(如果没有则为0)
                    record.sales_value = record.sales_value_store or 0.0

                reset_store_stmt = select(TargetStoreMain).where(
                    TargetStoreMain.fiscal_month.in_(list(fiscal_months))
                )
                reset_store_result = await db.execute(reset_store_stmt)
                reset_store_records = reset_store_result.scalars().all()

                # 重置所有相关门店记录的电商销售数据
                for record in reset_store_records:
                    record.sales_value_ec = 0.0
                    # 销售总额设为线下销售额(如果没有则为0)
                    record.sales_value = record.sales_value_store or 0.0

                # 提交重置操作
                await db.commit()
                app_logger.debug(f"已重置 {len(reset_records)} 条员工考勤记录")

            for (staff_code, store_code, fiscal_month), sales_value_ec in ec_sales_summary.items():
                # 更新或创建员工考勤记录
                query = select(StaffAttendanceModel).where(
                    StaffAttendanceModel.staff_code == staff_code,
                    StaffAttendanceModel.store_code == store_code,
                    StaffAttendanceModel.fiscal_month == fiscal_month
                )
                result = await db.execute(query)
                attendance_record = result.scalar_one_or_none()

                if attendance_record:
                    # 如果记录存在，更新线上销售金额
                    attendance_record.sales_value_ec = sales_value_ec
                    attendance_record.sales_value = (attendance_record.sales_value_store or 0) + sales_value_ec
                    updated_attendances += 1
                else:
                    app_logger.warning(
                        f"未找到员工考勤记录，将创建新的记录: staff_code={staff_code}, store_code={store_code}, fiscal_month={fiscal_month}")

                store_key = (store_code, fiscal_month)
                if store_key in store_sales_summary:
                    store_sales_summary[store_key] += sales_value_ec
                else:
                    store_sales_summary[store_key] = sales_value_ec

            # 更新门店目标表中的电商销售数据
            for (store_code, fiscal_month), total_sales_value_ec in store_sales_summary.items():
                # 查询现有的门店目标记录
                query = select(TargetStoreMain).where(
                    TargetStoreMain.store_code == store_code,
                    TargetStoreMain.fiscal_month == fiscal_month
                )
                result = await db.execute(query)
                target_store_record = result.scalar_one_or_none()

                if target_store_record:
                    # 更新电商销售金额和总销售金额
                    target_store_record.sales_value_ec = total_sales_value_ec
                    # 总销售金额 = 线下销售金额 + 电商销售金额
                    # sales_value_store = target_store_record.sales_value_store or 0.0
                    target_store_record.sales_value = (
                                                                  target_store_record.sales_value_store or 0) + total_sales_value_ec
                else:
                    app_logger.warning(
                        f"未找到门店目标记录: store_code={store_code}, fiscal_month={fiscal_month}")

            # 提交员工考勤更新和门店目标更新
            if ec_sales_summary:
                await db.commit()

            return ImportResult(
                success=True,
                message="成功导入电商销售数据",
                data_type="ec_sales",
                rows_processed=processed_count,
                rows_with_errors=error_count,
                errors=errors[:10]  # 只返回前10个错误信息
            )
        except Exception as e:
            await db.rollback()
            errors.append(f"数据库提交失败: {str(e)}")
            return ImportResult(
                success=False,
                message=f"导入电商销售数据失败: {str(e)}",
                data_type="ec_sales",
                rows_processed=processed_count,
                rows_with_errors=error_count + 1,
                errors=errors[:10]
            )


@router.post("/preview", response_model=ExcelPreviewResponse)
async def preview_excel(file: UploadFile = File(...)):
    """预览Excel文件"""
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="只支持Excel文件格式")

    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))
        data_type = identify_excel_type(df)
        df = df.fillna('')

        for col in df.columns:
            if df[col].dtype == 'datetime64[ns]':
                df[col] = df[col].astype(str).replace('NaT', '')

        return ExcelPreviewResponse(
            data_type=data_type,
            preview_data=df.to_dict('records'),
            total_rows=len(df),
            columns=list(df.columns),
            errors=[]
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"处理文件时出错: {str(e)}")


@router.post("/import")
async def import_excel_data(
        file: UploadFile = File(...),
        db: AsyncSession = Depends(get_db)
):
    """导入Excel数据"""
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="只支持Excel文件格式")

    try:
        # 读取并处理Excel文件
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))
        data_type = identify_excel_type(df)

        # 根据数据类型调用相应的处理函数
        if data_type == 'target':
            result = await ExcelImportService.import_target_data(df, db)
        elif data_type == 'budget':
            result = await ExcelImportService.import_budget_data(df, db)
        elif data_type == 'ec_sales':
            result = await ExcelImportService.import_ec_sales_data(df, db)
        else:
            return ImportResult(
                success=True,
                message=f"无法识别的数据类型",
                data_type=data_type,
                rows_processed=0
            )

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"导入失败: {str(e)}")
