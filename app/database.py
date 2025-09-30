from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from app.utils.logger import app_logger

# 创建数据库引擎
# engine = create_async_engine("mysql+aiomysql://root:1qazXSW%40@192.168.0.30:3306/datahub_dev")
engine = create_async_engine("mysql+aiomysql://datahub:4rfvBGT#6yhn@APCHALIVPPOS06:3306/datahub")

# 创建会话工厂
SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# 基类
Base = declarative_base()


async def get_db():
    """获取MySQL数据库会话"""
    async_session = SessionLocal()
    try:
        app_logger.debug("Acquiring MySQL database session")
        yield async_session
    except SQLAlchemyError as e:
        app_logger.error(f"MySQL database operation error: {e}")
        raise
    except Exception as e:
        app_logger.error(f"Unknown error occurred while acquiring MySQL session: {e}")
        raise
    finally:
        try:
            await async_session.close()
            app_logger.debug("MySQL database session closed")
        except Exception as e:
            app_logger.error(f"Error closing MySQL session: {e}")

#
from sqlalchemy import create_engine

import pyodbc

db_config = {
    'host': 'apchalivpxcdb01',
    'port': 1433,
    'user': 'sa',
    'password': 'Xstore123',
    'database': 'xcenter'
}

try:
    ms_engine = create_engine(
        f'mssql+pyodbc://{db_config["user"]}:{db_config["password"]}'
        f'@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}'
        f'?driver=ODBC+Driver+17+for+SQL+Server',
        pool_size=10,
        max_overflow=10,
        pool_recycle=3600,
        pool_pre_ping=True
    )


    # 测试SQL Server连接
    def test_sqlserver_connection():
        try:
            with ms_engine.connect() as conn:
                conn.execute("SELECT 1")
            app_logger.info("SQL Server database connection successful")
        except Exception as e:
            app_logger.error(f"SQL Server database connection failed: {e}")


    SQLServerSessionLocal = sessionmaker(bind=ms_engine)
    app_logger.info("SQL Server database engine initialized successfully")

except Exception as e:
    app_logger.error(f"SQL Server database engine initialization failed: {e}")
    raise


async def get_sqlserver_db():
    """获取SQL Server数据库会话"""
    ms_session = SQLServerSessionLocal()
    try:
        app_logger.debug("Acquiring SQL Server database session")
        yield ms_session
    except SQLAlchemyError as e:
        app_logger.error(f"SQL Server database operation error: {e}")
        raise
    except Exception as e:
        app_logger.error(f"Unknown error occurred while acquiring SQL Server session: {e}")
        raise
    finally:
        try:
            ms_session.close()
            app_logger.debug("SQL Server database session closed")
        except Exception as e:
            app_logger.error(f"Error closing SQL Server session: {e}")
