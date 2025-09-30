from sqlalchemy.orm import Session
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt

from app.utils.logger import app_logger

from app.models.dimension import SysUser

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="retail_hub_api/token")
SECRET_KEY = "secret"
ALGORITHM = "HS256"


async def get_current_user(token: str = Depends(oauth2_scheme)) -> str:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        userid: str = payload.get("sub")

        if userid is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    return userid


async def verify_password(session: Session, user_code: str, user_password: str) -> bool:
    # 查询用户
    result = session.query(SysUser.password).filter(SysUser.login_name == user_code).first()
    # 如果用户不存在，返回 False
    if not result:
        app_logger.warning(f"user not found: {user_code}")
        return False

    # 获取数据库中的哈希密码
    # hashed_password = result[0]
    #
    # # 使用 bcrypt 验证密码
    # return bcrypt.checkpw(user_password.encode('utf-8')
    return True
