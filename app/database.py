import yaml
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine
import pyodbc
from app.utils.logger import app_logger
import os


# 修改配置文件加载逻辑
def load_config():
    """加载配置文件，优先使用外部配置"""
    # 优先从exe文件所在目录查找配置文件
    exe_dir = os.path.dirname(os.path.abspath(__file__ if not getattr(sys, 'frozen', False) else sys.executable))
    external_config_path = os.path.join(exe_dir, 'app', 'config', 'config.yml')

    if os.path.exists(external_config_path):
        config_path = external_config_path
        app_logger.info(f"Loading external config from: {config_path}")
    else:
        # 如果外部配置不存在，则使用默认包内配置
        config_path = os.path.join(os.path.dirname(__file__), 'config', 'config.yml')
        app_logger.info(f"Loading internal config from: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


# 添加sys导入
import sys

config = load_config()

# 获取当前环境配置
current_env = config['current_env']
db_config = config['environments'][current_env]['database']
sqlserver_config = config['environments'][current_env]['sqlserver']

# 创建MySQL数据库引擎
mysql_url = f"mysql+aiomysql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"
engine = create_async_engine(mysql_url, pool_size=10,
                             max_overflow=20,
                             pool_pre_ping=True,
                             pool_recycle=3600,
                             echo=False)

# 创建MySQL会话工厂
SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# 基类
Base = declarative_base()


async def get_db():
    """获取MySQL数据库会话"""
    async_session = None
    try:
        async_session = SessionLocal()
        if async_session is None:
            raise Exception("Failed to create database session")
        yield async_session
    except SQLAlchemyError as e:
        app_logger.error(f"MySQL database operation error: {e}")
        raise
    except Exception as e:
        app_logger.error(f"Unknown error occurred while acquiring MySQL session: {e}")
        raise
    finally:
        if async_session is not None:
            try:
                await async_session.close()
            except Exception as e:
                app_logger.error(f"Error closing MySQL session: {e}")


# 创建SQL Server数据库引擎
try:
    ms_engine = create_engine(
        f'mssql+pyodbc://{sqlserver_config["username"]}:{sqlserver_config["password"]}'
        f'@{sqlserver_config["host"]}:{sqlserver_config["port"]}/{sqlserver_config["name"]}'
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
        except Exception as e:
            app_logger.error(f"Error closing SQL Server session: {e}")
