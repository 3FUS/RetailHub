from sqlalchemy.orm import Session
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from sqlalchemy import text
from app.utils.logger import app_logger
from app.models.dimension import SysUser

# from app.core.jar_pwd_handler import get_password_handler
from app.core.python_ssha2_hasher import Ssha2Hasher

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


_password_hasher = Ssha2Hasher()


async def verify_password(session: Session, user_code: str, user_password: str):
    # 查询用户
    result = session.query(SysUser.password, SysUser.id).filter(SysUser.login_name == user_code).first()
    # 如果用户不存在，返回 False
    if not result:
        app_logger.warning(f"user not found: {user_code}")
        return {"verify_result": False, "approve": False}

    hashed_password = result[0]
    user_id = result[1]
    # hasher = Ssha2Hasher()
    # 复用全局 hasher 实例
    verify_result = _password_hasher.verify(hashed_password, user_password)

    # return verify_result

    sql = text("""
        SELECT menu_rel_id from sys_user_role_rel a 
        INNER JOIN sys_role_menu_rel b on a.role_rel_id=b.sys_role_id 
        where sys_user_id=:sys_user_id and menu_rel_id='approve'
    """)

    result = session.execute(sql, {"sys_user_id": user_id}).fetchone()

    if result:
        app_logger.info(f"User {user_code}   have 'approve' permission.")
        approve = True
    else:
        approve = False

    return {"verify_result": verify_result, "approve": approve}
