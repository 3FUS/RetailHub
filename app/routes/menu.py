from typing import List

from fastapi import APIRouter, Depends

from app.core.security import get_current_user

router = APIRouter()

from app.database import get_sqlserver_db

@router.get("/list")
async def get_menus(current_user: dict = Depends(get_current_user)):
    # 返回支持中英文的两层结构菜单数据
    user_code = current_user['user_code']
    if user_code == 'admin':
        permission_list = ['save', 'submit', 'withdrawn', 'approve', 'reject', 'adjustment', 'MonthEnd']
    elif user_code == 'admin1':
        permission_list = ['approve', 'reject', 'adjustment', 'MonthEnd']
    else:
        permission_list = ['save', 'submit', 'withdrawn']
    return [
        # {
        #     "id": 1,
        #     "name_cn": "首页",
        #     "name_en": "Home",
        #     "url": "/home",
        #     "icon": "home",
        #     "children": []
        # },
        {
            "id": 2,
            "name_cn": "目标奖金管理",
            "name_en": "Target Commission",
            "icon": "target",
            "children": [
                {
                    "id": 21,
                    "name_cn": "目标分摊",
                    "name_en": "Target Setting",
                    "url": "/performance/target",
                    "icon": "setting",
                    "permissions": permission_list
                },
                {
                    "id": 22,
                    "name_cn": "提成列表",
                    "name_en": "Commission List",
                    "url": "/commission/list",
                    "icon": "Commission",
                    "permissions": permission_list
                },
                {
                    "id": 23,
                    "name_cn": "数据上传",
                    "name_en": "Import Data",
                    "url": "/commission/import",
                    "icon": "import"
                },{
                    "id": 31,
                    "name_cn": "目标报表-门店",
                    "name_en": "Store Target Report",
                    "url": "/performance/report?report_type=target_by_store",
                    "icon": "report"
                }
                ,
                {
                    "id": 31,
                    "name_cn": "目标报表-导购",
                    "name_en": "Staff Target Report",
                    "url": "/performance/report?report_type=target_by_staff",
                    "icon": "report"
                },
                {
                    "id": 31,
                    "name_cn": "提成报表",
                    "name_en": "Commission Report",
                    "url": "/performance/report?report_type=commission",
                    "icon": "report"
                },
                {
                    "id": 32,
                    "name_cn": "预算报表",
                    "name_en": "Budget Report",
                    "url": "/performance/report?report_type=budget",
                    "icon": "report"
                }
            ]
        },
        # {
        #     "id": 3,
        #     "name_cn": "奖金管理",
        #     "name_en": "Commission",
        #     "icon": "commission",
        #     "children": [
        #         {
        #             "id": 31,
        #             "name_cn": "提成列表",
        #             "name_en": "Commission List",
        #             "url": "/commission/list",
        #             "icon": "Commission",
        #             "permissions": permission_list
        #         }
        #     ]
        # },
        # {
        #     "id": 4,
        #     "name_cn": "数据上传",
        #     "name_en": "DATA UPLOAD",
        #     "icon": "import",
        #     "children": [
        #         {
        #             "id": 31,
        #             "name_cn": "数据上传",
        #             "name_en": "Import Data",
        #             "url": "/commission/import",
        #             "icon": "import"
        #         }
        #     ]
        # },
        # {
        #     "id": 5,
        #     "name_cn": "财务报表",
        #     "name_en": "Commission Report",
        #     "icon": "report",
        #     "children": [
        #         {
        #             "id": 31,
        #             "name_cn": "目标报表",
        #             "name_en": "Target Report",
        #             "url": "/performance/report",
        #             "icon": "report"
        #         },
        #         {
        #             "id": 32,
        #             "name_cn": "预算报表",
        #             "name_en": "Budget Report",
        #             "url": "",
        #             "icon": "report"
        #         }
        #     ]
        # },
        {
            "id": 6,
            "name_cn": "报表分析",
            "name_en": "Sales Report",
            "icon": "report",
            "children": [
                {
                    "id": 31,
                    "name_cn": "SALES 1",
                    "name_en": "SALES 1",
                    "url": "http://apchalivppos01:8443/ReportServer/Pages/ReportViewer.aspx?%2frtp_DiscountAnalysisReport&rs:Embed=true&loginName={loginName}",
                    "icon": "report"
                },
                {
                    "id": 32,
                    "name_cn": "SALES 2",
                    "name_en": "SALES 2",
                    "url": "http://apchalivppos01:8443/ReportServer/Pages/ReportViewer.aspx?%2frtp_DiscountAnalysisReport&rs:Embed=true&loginName={loginName}",
                    "icon": "report"
                }
            ]
        }
    ]
