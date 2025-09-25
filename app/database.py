from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
# 创建数据库引擎
# engine = create_async_engine("mysql+aiomysql://root:1qazXSW@@192.168.0.30:3306/datahub_dev")
engine = create_async_engine("mysql+aiomysql://datahub:4rfvBGT#6yhn@APCHALIVPPOS06:3306/datahub")

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


#
# from sqlalchemy import create_engine
# import pyodbc
# db_config = {
#                 'host': '192.168.0.220',
#                 'port': 1433,
#                 'user': 'sa',
#                 'password': 'Xstore123',
#                 'database': 'xcenter'
#             }
#
# ms_engine = create_engine(
#     f'mssql+pyodbc://{db_config["user"]}:{db_config["password"]}'
#     f'@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}'
#     f'?driver=ODBC+Driver+17+for+SQL+Server',
#     pool_size=10,
#     max_overflow=10,
#     pool_recycle=3600,
#     pool_pre_ping=True)
#
# SQLServerSessionLocal = sessionmaker(bind=ms_engine)
#
#
# async def get_sqlserver_db():
#     ms_session = SQLServerSessionLocal()
#     try:
#         yield ms_session
#     finally:
#         await ms_session.close()
