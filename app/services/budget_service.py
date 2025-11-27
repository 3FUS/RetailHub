from datetime import datetime

from sqlalchemy import String
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.budget import BudgetModel
from collections import defaultdict

from app.models.dimension import DimensionDayWeek
from app.models.target import TargetStoreDaily, TargetStoreWeek, TargetStoreMain
from app.utils.permissions import build_store_permission_query
from app.utils.logger import app_logger
from sqlalchemy import update
class BudgetService:
    @staticmethod
    async def get_budget_data(db: AsyncSession, fiscal_month: str, key_word: str, status: str, role_code: str) -> dict:
        """
        获取预算数据并按门店横向展示

        Args:
            db: 数据库会话
            fiscal_month: 财务月份

        Returns:
            dict: 包含预算数据和表头信息的字典
        """
        try:
            app_logger.info(f"Starting get_budget_data for fiscal_month: {fiscal_month}, "
                            f"key_word: {key_word}, status: {status}, role_code: {role_code}")

            # 执行SQL查询逻辑
            store_permission_query = build_store_permission_query(role_code)
            store_alias = store_permission_query.subquery()

            # 修改后的 get_budget_data 方法部分代码
            query = select(
                TargetStoreDaily.target_date.label('date'),
                BudgetModel.store_code,
                TargetStoreDaily.budget_value.label('budget_date_value')
            ).select_from(
                BudgetModel.__table__.join(
                    TargetStoreDaily.__table__,
                    (BudgetModel.store_code == TargetStoreDaily.store_code) &
                    (BudgetModel.fiscal_month == TargetStoreDaily.fiscal_month)
                ).join(
                    TargetStoreMain.__table__,
                    (BudgetModel.store_code == TargetStoreMain.store_code) &
                    (BudgetModel.fiscal_month == TargetStoreMain.fiscal_month)
                ).join(
                    store_alias,
                    BudgetModel.store_code == store_alias.c.store_code
                )
            ).where(
                BudgetModel.fiscal_month == fiscal_month
            ).order_by(TargetStoreDaily.target_date, BudgetModel.store_code)

            # 如果有关键字过滤条件
            if key_word:
                app_logger.debug(f"Applying keyword filter: {key_word}")
                query = query.where(
                    BudgetModel.store_code.contains(key_word)
                )

            if status:
                app_logger.debug(f"Applying status filter: {status}")
                query = query.where(
                    TargetStoreMain.store_status == status
                )

            app_logger.debug("Executing budget data query")
            result = await db.execute(query)
            budget_data = result.all()
            app_logger.info(f"Fetched {len(budget_data)} rows from budget data query")

            if not budget_data:
                app_logger.warning(f"No budget data found for fiscal_month: {fiscal_month}")
                return {
                    "data": [],
                    "columns": ["date"],
                    "store_codes": []
                }

            # 按日期分组数据并构建横向表结构
            app_logger.debug("Grouping data by date")
            date_groups = defaultdict(dict)
            store_codes = set()

            for idx, row in enumerate(budget_data):
                date_str =row.date.strftime('%Y/%m/%d').lstrip('0').replace('/0', '/') if row.date else None
                store_code = row.store_code
                budget_value = row.budget_date_value if row.budget_date_value is not None else 0.0

                if date_str:
                    date_groups[date_str][store_code] = budget_value
                    store_codes.add(store_code)

                if idx > 0 and idx % 1000 == 0:
                    app_logger.debug(f"Processed {idx}/{len(budget_data)} rows")

            # 构建结果数据
            sorted_store_codes = sorted(list(store_codes))
            columns = ["Date"] + sorted_store_codes
            app_logger.debug(f"Found {len(sorted_store_codes)} unique store codes")

            # 构建表格数据
            # 构建表格数据
            app_logger.debug("Building table data")
            table_data = []
            # 使用 datetime 排序确保日期按时间顺序排列
            date_list = sorted(list(date_groups.keys()), key=lambda x: datetime.strptime(x, '%Y/%m/%d'))

            for date_idx, date_str in enumerate(date_list):
                store_values = date_groups[date_str]
                row_data = {"Date": date_str}
                table_data.append(row_data)
                for store_code in sorted_store_codes:
                    formatted_store_code = f"Store{store_code}"
                    row_data[formatted_store_code] = store_values.get(store_code, 0.0)

                if date_idx > 0 and date_idx % 1000 == 0:
                    app_logger.debug(f"Built {date_idx}/{len(date_list)} table rows")

            app_logger.info(f"Returning {len(table_data)} rows of formatted data with {len(columns)} columns")

            return {
                "data": table_data,
                "columns": columns,
                "store_codes": sorted_store_codes
            }

        except Exception as e:
            app_logger.error(f"Error in get_budget_data: {str(e)}", exc_info=True)
            raise e

    @staticmethod
    async def batch_update_budget_value(db: AsyncSession, budget_updates: list):
        """
        批量更新 BudgetModel 的 budget_value

        Args:
            db: 数据库会话
            budget_updates: 包含 store_code, fiscal_month, budget_value 的字典列表
        """

        if not budget_updates:
            return []

            # 获取 fiscal_month（假设所有更新记录具有相同的 fiscal_month）
        fiscal_month = budget_updates[0].get('fiscal_month')

        if not fiscal_month:
            raise ValueError("fiscal_month is required in budget_updates")

        # 第一步：将指定 fiscal_month 的所有 BudgetModel 记录的 budget_value 设置为 0
        await db.execute(
            update(BudgetModel)
                .where(BudgetModel.fiscal_month == fiscal_month)
                .values(budget_value=0)
        )

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
