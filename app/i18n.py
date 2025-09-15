"""
Internationalization support
"""

import json
from typing import Dict, Any

class I18n:
    def __init__(self):
        self.translations = {}
        self.load_translations()
    
    def load_translations(self):
        # 加载翻译文件
        self.translations = {
            "zh-CN": {
                "welcome": "欢迎使用",
                "target": "目标",
                "commission": "奖金",
                "menu": "菜单",
                "create": "创建",
                "update": "更新",
                "submit": "提交",
                "audit": "审核",
                "unaudit": "反审核",
                "list": "列表",
                "detail": "详情",
                "success": "成功",
                "error": "错误"
            },
            "en-US": {
                "welcome": "Welcome to",
                "target": "Target",
                "commission": "Commission",
                "menu": "Menu",
                "create": "Create",
                "update": "Update",
                "submit": "Submit",
                "audit": "Audit",
                "unaudit": "Unaudit",
                "list": "List",
                "detail": "Detail",
                "success": "Success",
                "error": "Error"
            }
        }
    
    def translate(self, key: str, language: str = "zh-CN") -> str:
        return self.translations.get(language, {}).get(key, key)