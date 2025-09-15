"""
Configuration file for the application
"""

import os

# Database configuration
# DATABASE_URL = os.getenv("DATABASE_URL", "mysql+aiomysql://root:rootroot@localhost:3306/tb_retail_hub")

# Application configuration
APP_NAME = "tb_retail_hub_api"
APP_VERSION = "0.1.0"

# Language settings
SUPPORTED_LANGUAGES = ["zh-CN", "en-US"]
DEFAULT_LANGUAGE = "zh-CN"