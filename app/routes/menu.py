from typing import List
import re

from fastapi import APIRouter, Depends

from app.core.security import get_current_user

router = APIRouter()

from app.database import get_sqlserver_db
from app.utils.logger import app_logger
from sqlalchemy import text


def remove_port_from_url(url: str) -> str:
    """
    移除URL中的端口号（如 :8084, :8081, :8002 等）
    """
    # 使用正则表达式匹配并移除端口号
    return re.sub(r':\d+', '', url)


@router.get("/list")
async def get_menus(current_user: dict = Depends(get_current_user), db=Depends(get_sqlserver_db)):
    # 记录请求开始的日志
    app_logger.info(f"Fetching menus for user: {current_user['user_code']}")

    # 从数据库查询用户对应的菜单和权限
    user_code = current_user['user_code']

    # 实际查询SQL
    query = text("""
           SELECT distinct a.parent_id,a.parent_id_cn, a.id, a.description, a.menu_url, a.menu_name,a.menu_name_cn, a.type,a.icon,a.parent_icon,a.sort
           FROM sys_menu a 
           INNER JOIN sys_role_menu_rel b ON a.id = b.menu_rel_id 
           INNER JOIN sys_user_role_rel c ON b.sys_role_id = c.role_rel_id 
           INNER JOIN sys_user u ON u.id = c.sys_user_id 
           WHERE u.login_name = :user_code
           AND a.update_user = 'SSRS' and isnull(a.[type],'') <>'menu' ORDER BY a.sort
           """)

    try:

        result = db.execute(query, {"user_code": user_code})
        results = result.fetchall()
        app_logger.debug(f"Query returned {len(results)} rows")
    except Exception as e:
        app_logger.error(f"Database query failed: {str(e)}")
        raise

    # 构造权限列表
    permission_list = [row.menu_name for row in results if row.type == 'permission']
    app_logger.debug(f"Constructed permission list with {len(permission_list)} items")

    # 构造菜单树结构（两级结构）
    # 按parent_id分组，只处理两级菜单
    menu_dict = {}
    parent_names = {}  # 用于存储parent_id对应的中文名称
    parent_icons = {}
    for row in results:
        if row.type != 'permission':  # 排除权限类型
            menu_item = {
                "id": row.id,
                "name_cn": row.menu_name_cn,
                "name_en": row.menu_name,
                "url": remove_port_from_url(row.menu_url.replace("http://", "https://") if row.menu_url.startswith(
                    "http://") else row.menu_url),
                "icon": row.icon or "",
            }
            if row.parent_id not in menu_dict:
                menu_dict[row.parent_id] = []
                # 存储parent_id对应的中文名称
                parent_names[row.parent_id] = row.parent_id_cn
                parent_icons[row.parent_id] = row.parent_icon
            menu_dict[row.parent_id].append(menu_item)

    app_logger.debug(f"Constructed menu_dict with {len(menu_dict)} parent nodes")

    # 构建两级菜单树
    menu_tree = []
    for parent_id in menu_dict.keys():
        # 使用parent_id_cn作为根菜单的显示名称
        parent_name = parent_names.get(parent_id, str(parent_id))
        parent_icon = parent_icons.get(parent_id, str(parent_id))

        root_menu = {
            "id": parent_id or "0",  # 使用parent_id作为id
            "name_cn": parent_name,  # 使用parent_id_cn作为中文名称
            "name_en": parent_id,  # 使用parent_id_cn作为英文名称
            "icon": parent_icon,  # 可以根据需要调整
            "children": []
        }

        # 为每个子菜单项添加权限
        for child in menu_dict[parent_id]:
            child["permissions"] = permission_list

        root_menu["children"] = menu_dict[parent_id]
        menu_tree.append(root_menu)

    app_logger.info(f"Successfully built menu tree with {len(menu_tree)} root items")
    return menu_tree
#
#
# @router.get("/listBak")
# async def get_menusBak(current_user: dict = Depends(get_current_user)):
#     # 返回支持中英文的两层结构菜单数据
#     user_code = current_user['user_code']
#     if user_code == 'admin':
#         permission_list = ['save', 'submit', 'withdrawn', 'approve', 'reject', 'adjustment', 'MonthEnd']
#     elif user_code == 'admin1':
#         permission_list = ['approve', 'reject', 'adjustment', 'MonthEnd']
#     else:
#         permission_list = ['save', 'submit', 'withdrawn']
#     return [
#         # {
#         #     "id": 1,
#         #     "name_cn": "首页",
#         #     "name_en": "Home",
#         #     "url": "/home",
#         #     "icon": "home",
#         #     "children": []
#         # },
#         {
#             "id": 2,
#             "name_cn": "目标奖金管理",
#             "name_en": "Target Commission",
#             "icon": "target",
#             "children": [
#                 {
#                     "id": 21,
#                     "name_cn": "目标分摊",
#                     "name_en": "Target Setting",
#                     "url": "/performance/target",
#                     "icon": "setting",
#                     "permissions": permission_list
#                 },
#                 {
#                     "id": 22,
#                     "name_cn": "提成列表",
#                     "name_en": "Commission List",
#                     "url": "/commission/list",
#                     "icon": "Commission",
#                     "permissions": permission_list
#                 },
#                 {
#                     "id": 23,
#                     "name_cn": "数据上传",
#                     "name_en": "Import Data",
#                     "url": "/commission/import",
#                     "icon": "import"
#                 },{
#                     "id": 31,
#                     "name_cn": "目标报表-门店",
#                     "name_en": "Store Target Report",
#                     "url": "/performance/report?report_type=target_by_store",
#                     "icon": "report"
#                 }
#                 ,
#                 {
#                     "id": 31,
#                     "name_cn": "目标报表-导购",
#                     "name_en": "Staff Target Report",
#                     "url": "/performance/report?report_type=target_by_staff",
#                     "icon": "report"
#                 },
#                 {
#                     "id": 31,
#                     "name_cn": "提成报表",
#                     "name_en": "Commission Report",
#                     "url": "/performance/report?report_type=commission",
#                     "icon": "report"
#                 },
#                 {
#                     "id": 32,
#                     "name_cn": "预算报表",
#                     "name_en": "Budget Report",
#                     "url": "/performance/report?report_type=budget",
#                     "icon": "report"
#                 }
#             ]
#         },
#         {
#             "id": 6,
#             "name_cn": "报表分析",
#             "name_en": "Sales Report",
#             "icon": "report",
#             "children": [
#                 {
#                     "id": 31,
#                     "name_cn": "SALES 1",
#                     "name_en": "SALES 1",
#                     "url": "http://apchalivppos01:8443/ReportServer/Pages/ReportViewer.aspx?%2frtp_DiscountAnalysisReport&rs:Embed=true&loginName={loginName}",
#                     "icon": "report"
#                 },
#                 {
#                     "id": 32,
#                     "name_cn": "SALES 2",
#                     "name_en": "SALES 2",
#                     "url": "http://apchalivppos01:8443/ReportServer/Pages/ReportViewer.aspx?%2frtp_DiscountAnalysisReport&rs:Embed=true&loginName={loginName}",
#                     "icon": "report"
#                 }
#             ]
#         }
#     ]
