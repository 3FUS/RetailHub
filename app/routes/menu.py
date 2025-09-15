from typing import List

from fastapi import APIRouter
from app.schemas.menu import MenuResponse

router = APIRouter()

@router.get("/list", response_model=List[MenuResponse])
async def get_menus():
    # 返回菜单数据
    return [
        {
            "id": 1,
            "name": "首页",
            "url": "/home",
            "icon": "home"
        },
        {
            "id": 2,
            "name": "目标管理",
            "url": "/target",
            "icon": "target"
        },
        {
            "id": 3,
            "name": "奖金管理",
            "url": "/commission",
            "icon": "commission"
        }
    ]