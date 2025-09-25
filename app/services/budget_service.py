from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.budget import BudgetModel
from collections import defaultdict

from app.models.target import TargetStoreDaily


class BudgetService:
    @staticmethod
    async def get_budget_data(db: AsyncSession, fiscal_month: str, keyword:str) -> dict:
        """
        获取预算数据并按门店横向展示

        Args:
            db: 数据库会话
            fiscal_month: 财务月份

        Returns:
            dict: 包含预算数据和表头信息的字典
        """
        # 执行SQL查询逻辑
        result = await db.execute(
            select(
                TargetStoreDaily.target_date.label('date'),
                BudgetModel.store_code,
                (BudgetModel.budget_value * TargetStoreDaily.monthly_percentage / 100).label('budget_date_value')
            )
                .select_from(
                BudgetModel.__table__.join(
                    TargetStoreDaily.__table__,
                    (BudgetModel.store_code == TargetStoreDaily.store_code) &
                    (BudgetModel.fiscal_month == TargetStoreDaily.fiscal_month)
                )
            )
                .where(
                BudgetModel.fiscal_month == fiscal_month
            )
                .order_by(TargetStoreDaily.target_date)
        )

        budget_data = result.all()

        if not budget_data:
            return {
                "data": [],
                "columns": ["date"],
                "store_codes": []
            }

        # 按日期分组数据并构建横向表结构
        date_groups = defaultdict(dict)
        store_codes = set()

        for row in budget_data:
            date_str = row.date.strftime('%Y-%m-%d')
            store_code = row.store_code
            budget_value = float(row.budget_date_value) if row.budget_date_value is not None else 0.0

            date_groups[date_str][store_code] = budget_value
            store_codes.add(store_code)

        # 构建结果数据
        sorted_store_codes = sorted(list(store_codes))
        columns = ["date"] + sorted_store_codes

        # 构建表格数据
        table_data = []
        for date_str, store_values in sorted(date_groups.items()):
            row_data = {"date": date_str}
            table_data.append(row_data)
            for store_code in sorted_store_codes:
                row_data[store_code] = store_values.get(store_code, 0.0)

        return {
            "data": table_data,
            "columns": columns,
            "store_codes": sorted_store_codes
        }


    @staticmethod
    async def batch_update_budget_value(db: AsyncSession, budget_updates: list):
        """
        批量更新 BudgetModel 的 budget_value

        Args:
            db: 数据库会话
            budget_updates: 包含 store_code, fiscal_month, budget_value 的字典列表
        """
        updated_budgets = []

        for update_data in budget_updates:
            store_code = update_data.get('store_code')
            fiscal_month = update_data.get('fiscal_month')
            budget_value = update_data.get('budget_value')

            # 查询现有记录
            result = await db.execute(select(BudgetModel).where(
                BudgetModel.store_code == store_code,
                BudgetModel.fiscal_month == fiscal_month
            ))
            budget_record = result.scalar_one_or_none()

            if budget_record:
                # 更新现有记录
                budget_record.budget_value = budget_value
                budget_record.updated_at = datetime.utcnow()
                updated_budgets.append(budget_record)
            else:
                # 创建新记录
                new_budget = BudgetModel(
                    store_code=store_code,
                    fiscal_month=fiscal_month,
                    budget_value=budget_value
                )
                db.add(new_budget)
                updated_budgets.append(new_budget)

        await db.commit()

        # 刷新所有对象以获取数据库生成的值
        for budget in updated_budgets:
            await db.refresh(budget)

        return updated_budgets
