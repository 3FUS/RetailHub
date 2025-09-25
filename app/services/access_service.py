from sqlalchemy.orm import Session
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from typing import Optional

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
    # result = session.query(SysUser.user_password).filter(SysUser.user_code == user_code).first()
    # if not result:
    #     return False
    # hashed_password = result[0]
    # return bcrypt.checkpw(user_password.encode('utf-8'), hashed_password.encode('utf-8'))
    return True
