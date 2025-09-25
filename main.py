from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from starlette import status

from app.routes import target, commission, menu, dimension, excel_upload, report
from jose import JWTError, jwt
from datetime import datetime, timedelta

from app.core.security import authenticate_user, create_access_token, ACCESS_TOKEN_EXPIRE_MINUTES
from app.database import get_db

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
app.include_router(dimension.router, prefix="/dimension")
app.include_router(excel_upload.router, prefix="/excel_upload")  # 添加这一行
app.include_router(report.router, prefix="/report")


@app.post("/retail_hub_api/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), session=Depends(get_db)):
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
    asyncio.run(init_db())
    import uvicorn

    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8002,
        reload=True,
        log_level="info"
    )
