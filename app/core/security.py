# /Users/fu/Downloads/XY/TB/tbcommision/app/core/security.py

from datetime import datetime, timedelta
from typing import Optional, Union
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from sqlalchemy.orm import Session

from app.services.access_service import verify_password
# from app.database import get_sqlserver_db

# OAuth2配置
SECRET_KEY = "secret"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 720

# OAuth2密码流
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="retail_hub_api/token")


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:

    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(
        token: str = Depends(oauth2_scheme)
) -> dict:
    """
    获取当前用户信息
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_code: str = payload.get("sub")

        if user_code is None:
            raise credentials_exception

    except JWTError:
        raise credentials_exception

    return {"user_code": user_code}

async def authenticate_user(
        session: Session,
        user_code: str,
        password: str
) -> Optional[dict]:
    """
    验证用户凭据
    """
    if await verify_password(session, user_code, password):
        return {"user_code": user_code}
    return None
