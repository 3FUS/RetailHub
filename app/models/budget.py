# app/models/budget.py
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, func, TIMESTAMP

from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class BudgetModel(Base):
    __tablename__ = "budgets"

    store_code = Column(String(30),  primary_key=True)
    fiscal_month = Column(String(50), primary_key=True)
    budget_value = Column(Float)
    created_at = Column(TIMESTAMP, nullable=True, default=func.current_timestamp())
    updated_at = Column(TIMESTAMP, nullable=True, default=func.current_timestamp())