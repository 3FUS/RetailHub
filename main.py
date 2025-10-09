from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from starlette import status

from app.routes import target, commission, menu, dimension, excel_upload, report
from jose import JWTError, jwt
from datetime import datetime, timedelta

from app.core.security import authenticate_user, create_access_token, ACCESS_TOKEN_EXPIRE_MINUTES
from app.database import get_sqlserver_db
from app.core.jar_pwd_handler import get_password_handler
from app.utils.logger import app_logger
from contextlib import asynccontextmanager
import asyncio


async def init_jvm_async():
    """异步初始化JVM"""
    loop = asyncio.get_event_loop()
    try:
        password_handler = get_password_handler()
        # 在线程池中运行CPU密集型任务
        app_logger.info("Starting JVM initialization in thread pool")
        success = await loop.run_in_executor(None, password_handler.start_jvm)
        if success:
            app_logger.info("JVM initialized successfully")
        else:
            app_logger.error("JVM initialization failed")
            # 不要让应用因为JVM初始化失败而终止
    except Exception as e:
        app_logger.error(f"JVM initialization error: {e}")
        import traceback
        app_logger.error(f"JVM initialization traceback: {traceback.format_exc()}")
        # 继续运行应用，即使JVM初始化失败

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动事件
    app_logger.info("Starting application lifespan")

    # 在后台初始化JVM，不影响应用启动
    jvm_task = asyncio.create_task(init_jvm_async())

    # 等待应用程序完全启动
    await asyncio.sleep(2)

    yield  # 应用程序运行期间

    # 关闭事件
    try:
        app_logger.info("Waiting for JVM initialization to complete")
        # 等待JVM初始化任务完成，但设置超时
        await asyncio.wait_for(jvm_task, timeout=30.0)  # 增加超时时间

        app_logger.info("Shutting down JVM")
        password_handler = get_password_handler()
        if password_handler.jvm_started:
            password_handler.shutdown_jvm()
            app_logger.info("JVM资源已释放")
        else:
            app_logger.info("JVM was not started, no need to shut down")
    except asyncio.TimeoutError:
        app_logger.warning("等待JVM初始化超时，继续应用关闭过程")
    except Exception as e:
        app_logger.error(f"JVM资源释放失败：{e}")


app = FastAPI(lifespan=lifespan)
# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(target.router, prefix="/target")
app.include_router(commission.router, prefix="/commission")
app.include_router(menu.router, prefix="/menu")
app.include_router(dimension.router, prefix="/dimension")
app.include_router(excel_upload.router, prefix="/excel_upload")
app.include_router(report.router, prefix="/report")

@app.post("/retail_hub_api/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), session=Depends(get_sqlserver_db)):
    user = await authenticate_user(session, form_data.username, form_data.password)
    if not user:
        return {"code": 301, "msg": "Incorrect username or password"}

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user['user_code']},
        expires_delta=access_token_expires
    )

    return {
        "code": 200,
        "access_token": access_token,
        "token_type": "bearer"
    }


@app.get("/health")
async def health_check():
    """健康检查端点"""
    try:
        password_handler = get_password_handler()
        jvm_status = "started" if password_handler.jvm_started else "not started"

        return {
            "status": "healthy",
            "jvm_status": jvm_status,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }
#
import asyncio
from app.models.commission import Base as CommissionBase
from app.models.staff import Base as StaffBase
from app.models.target import Base as TargetBase
from app.models.budget import Base as BudgetBase
from app.models.sales import Base as SalesBase
from app.database import engine


#

async def init_db():
    """
    初始化数据库并创建所有表
    """
    # 创建所有表
    async with engine.begin() as conn:
        # 导入所有模型后创建所有表
        await conn.run_sync(CommissionBase.metadata.create_all)
        await conn.run_sync(StaffBase.metadata.create_all)
        await conn.run_sync(TargetBase.metadata.create_all)
        await conn.run_sync(BudgetBase.metadata.create_all)
        await conn.run_sync(SalesBase.metadata.create_all)

    print("Database tables created successfully!")


if __name__ == "__main__":
    # asyncio.run(init_db())
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8002, log_level="info")
