from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import target, commission, menu

app = FastAPI()

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

import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from app.models.commission import Base as CommissionBase
from app.models.staff import Base as StaffBase
from app.models.target import Base as TargetBase
from app.database import engine


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

    print("数据库表创建成功!")


async def drop_db():
    """
    删除所有表（谨慎使用）
    """
    async with engine.begin() as conn:
        await conn.run_sync(CommissionBase.metadata.drop_all)
        await conn.run_sync(StaffBase.metadata.drop_all)
        await conn.run_sync(TargetBase.metadata.drop_all)

    print("所有表已删除!")

# if __name__ == "__main__":
#     asyncio.run(init_db())
#
#     import uvicorn
#     uvicorn.run(
#         "main:app",
#         host="127.0.0.1",
#         port=8000,
#         reload=True,
#         log_level="info"
#     )
