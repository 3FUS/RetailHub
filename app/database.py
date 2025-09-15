from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# 创建数据库引擎
# engine = create_async_engine("mysql+aiomysql://root:1qazXSW@@192.168.0.30:3306/datahub_dev")
engine = create_async_engine("mysql+aiomysql://root:1qazXSW%40@192.168.0.30:3306/datahub_dev")

# 创建会话工厂
SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# 基类
Base = declarative_base()

async def get_db():
    async_session = SessionLocal()
    try:
        yield async_session
    finally:
        await async_session.close()