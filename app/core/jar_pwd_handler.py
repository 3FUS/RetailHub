# /Users/fu/Downloads/XY/TB/RH/app/core/jar_pwd_handler.py
# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Java JAR package password processing module
Used to call methods in Oracle retail system password processing JAR package for password verification

Author: Demo
Date: 2024
"""
import platform
import winreg
from typing import List
import jpype
import os
from pathlib import Path
from typing import Optional
from app.utils.logger import app_logger
import time
import threading


class JarPasswordHandler:
    """JAR package password handler"""

    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(JarPasswordHandler, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self.jar_path = None
            self.jvm_started = False
            self.encoder = None
            self._initialized = True

    def _find_windows_jvm(self) -> Optional[str]:
        """Find JVM on Windows"""
        app_logger.debug("Searching for JVM on Windows")

        # 1. 首先检查 JAVA_HOME 环境变量
        java_home = os.environ.get('JAVA_HOME')
        if java_home and os.path.exists(java_home):
            jvm_path = self._get_jvm_path_from_java_home(java_home)
            if jvm_path and os.path.exists(jvm_path):
                app_logger.info(f"Found JVM via JAVA_HOME: {jvm_path}")
                return jvm_path

        # 2. 通过Windows注册表查找Java安装
        jvm_paths = self._find_jvm_in_registry()
        for path in jvm_paths:
            if os.path.exists(path):
                app_logger.info(f"Found JVM via registry: {path}")
                return path

        # 3. 查找常见安装目录
        common_paths = self._get_common_windows_jvm_paths()
        for path in common_paths:
            if os.path.exists(path):
                app_logger.info(f"Found JVM in common path: {path}")
                return path

        app_logger.debug("No JVM found on Windows")
        return None

    def _find_jvm_in_registry(self) -> List[str]:
        """通过Windows注册表查找JVM路径"""
        jvm_paths = []

        # 注册表路径列表
        registry_keys = [
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\JavaSoft\Java Runtime Environment"),
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\JavaSoft\Java Development Kit"),
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Wow6432Node\JavaSoft\Java Runtime Environment"),
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Wow6432Node\JavaSoft\Java Development Kit"),
        ]

        for hkey, key_path in registry_keys:
            try:
                with winreg.OpenKey(hkey, key_path) as base_key:
                    # 获取最新版本
                    version = None
                    try:
                        version, _ = winreg.QueryValueEx(base_key, "CurrentVersion")
                    except FileNotFoundError:
                        # 尝试获取第一个子键作为版本
                        try:
                            version = winreg.EnumKey(base_key, 0)
                        except OSError:
                            continue

                    if version:
                        try:
                            with winreg.OpenKey(hkey, f"{key_path}\\{version}") as version_key:
                                java_home, _ = winreg.QueryValueEx(version_key, "JavaHome")
                                jvm_path = self._get_jvm_path_from_java_home(java_home)
                                if jvm_path:
                                    jvm_paths.append(jvm_path)
                        except FileNotFoundError:
                            continue
            except OSError:
                continue

        return jvm_paths

    def _get_common_windows_jvm_paths(self) -> List[str]:
        """获取Windows常见JVM安装路径"""
        paths = []

        # 常见Java安装目录
        program_files = os.environ.get('ProgramFiles', r'C:\Program Files')
        program_files_x86 = os.environ.get('ProgramFiles(x86)', r'C:\Program Files (x86)')

        java_locations = [
            # Oracle JDK/JRE
            os.path.join(program_files, "Java"),
            os.path.join(program_files_x86, "Java"),
            # Amazon Corretto
            os.path.join(program_files, "Amazon Corretto"),
            os.path.join(program_files_x86, "Amazon Corretto"),
            # AdoptOpenJDK/Eclipse Temurin
            os.path.join(program_files, "AdoptOpenJDK"),
            os.path.join(program_files_x86, "AdoptOpenJDK"),
            os.path.join(program_files, "Eclipse Adoptium"),
            os.path.join(program_files_x86, "Eclipse Adoptium"),
        ]

        # 遍历可能的Java安装目录
        for location in java_locations:
            if os.path.exists(location):
                try:
                    # 遍历所有子目录查找Java安装
                    for item in os.listdir(location):
                        java_home = os.path.join(location, item)
                        if os.path.isdir(java_home):
                            jvm_path = self._get_jvm_path_from_java_home(java_home)
                            if jvm_path:
                                paths.append(jvm_path)
                except OSError:
                    continue

        return paths

    def _get_jvm_path_from_java_home(self, java_home: str) -> Optional[str]:
        """Get JVM path from JAVA_HOME"""
        system = platform.system().lower()

        if system == "windows":
            # Windows JVM路径
            jvm_paths = [
                os.path.join(java_home, "jre", "bin", "server", "jvm.dll"),
                os.path.join(java_home, "bin", "server", "jvm.dll"),
                os.path.join(java_home, "jre", "bin", "client", "jvm.dll"),
                os.path.join(java_home, "bin", "client", "jvm.dll")
            ]
        else:
            # 其他系统保持原样
            jvm_paths = [
                os.path.join(java_home, "jre", "bin", "server", "jvm.dll"),
                os.path.join(java_home, "bin", "server", "jvm.dll"),
                os.path.join(java_home, "jre", "bin", "client", "jvm.dll"),
                os.path.join(java_home, "bin", "client", "jvm.dll")
            ]

        for path in jvm_paths:
            if os.path.exists(path):
                return path
        return None

    def find_jvm_path(self) -> str:
        """Find available JVM path"""
        app_logger.info("Starting to find available JVM path")

        # Windows特殊处理
        system = platform.system().lower()
        if system == "windows":
            jvm_path = self._find_windows_jvm()
            if jvm_path:
                return jvm_path

        # Try environment variables first
        java_home = os.environ.get('JAVA_HOME')
        if java_home and os.path.exists(java_home):
            jvm_path = self._get_jvm_path_from_java_home(java_home)
            if jvm_path and os.path.exists(jvm_path):
                app_logger.info(f"Found JVM path from JAVA_HOME: {jvm_path}")
                return jvm_path

        # Fallback paths (仅Windows)
        if system == "windows":
            possible_paths = [
                "C:\\Program Files\\Java\\jre1.8.0_421\\bin\\server\\jvm.dll",
                "C:\\Program Files\\Java\\jdk1.8.0_421\\jre\\bin\\server\\jvm.dll",
                "C:\\Program Files\\Java\\jdk1.8.0_421\\bin\\server\\jvm.dll",
                "C:\\Program Files\\Amazon Corretto\\jdk1.8.0_422\\jre\\bin\\server\\jvm.dll",
                "C:\\Program Files (x86)\\Java\\jre1.8.0_421\\bin\\server\\jvm.dll",
                "C:\\Program Files (x86)\\Java\\jdk1.8.0_421\\jre\\bin\\server\\jvm.dll",
                "C:\\Program Files (x86)\\Java\\jdk1.8.0_421\\bin\\server\\jvm.dll"
            ]
        else:
            possible_paths = [
                jpype.getDefaultJVMPath(),
                "/Library/Java/JavaVirtualMachines/jdk1.8.0_391.jdk/Contents/Home/lib/server/libjvm.dylib",
                "/Library/Java/JavaVirtualMachines/jdk1.8.0_391.jdk/Contents/Home/lib/jli/libjli.dylib",
                "/System/Library/Frameworks/JavaVM.framework/JavaVM"
            ]

        for path in possible_paths:
            if path and os.path.exists(path):
                app_logger.info(f"Found JVM using fallback path: {path}")
                return path

        error_msg = "Unable to find available JVM path"
        app_logger.error(error_msg)
        raise RuntimeError(error_msg)

    def start_jvm(self) -> bool:
        """Start JVM and load JAR package"""
        app_logger.info("Starting JVM and loading JAR package")
        if self.jvm_started and jpype.isJVMStarted():
            app_logger.info("JVM already started, returning True")
            return True

        try:
            # Get JAR package path
            import sys
            if getattr(sys, 'frozen', False):
                # 运行在 PyInstaller 打包环境中
                bundle_dir = sys._MEIPASS
                self.jar_path = Path(bundle_dir) / "app" / "core" / "lib" / "dtv-password.jar"
            else:
                # 运行在开发环境中
                current_dir = Path(__file__).parent
                self.jar_path = current_dir / "lib" / "dtv-password.jar"

            app_logger.info(f"JAR path: {self.jar_path}")
            app_logger.info(f"JAR exists: {self.jar_path.exists()}")

            if not self.jar_path.exists():
                warning_msg = f"Warning: JAR file does not exist: {self.jar_path}"
                app_logger.warning(warning_msg)
                return False

            # 验证JAR文件是否可读
            try:
                with open(self.jar_path, 'rb') as f:
                    magic = f.read(4)
                    if magic != b'\x50\x4b\x03\x04':  # ZIP文件魔数
                        app_logger.error(f"Invalid JAR file format: {self.jar_path}")
                        return False
            except Exception as e:
                app_logger.error(f"Cannot read JAR file: {e}")
                return False

            # Start JVM
            if not jpype.isJVMStarted():
                jvm_path = self.find_jvm_path()
                app_logger.info(f"Using JVM path: {jvm_path}")

                # 添加信号处理以捕获致命错误
                import signal
                import sys

                def signal_handler(signum, frame):
                    app_logger.error(f"Received signal {signum} during JVM startup")
                    sys.exit(1)

                # 注册信号处理器
                signal.signal(signal.SIGTERM, signal_handler)
                signal.signal(signal.SIGINT, signal_handler)

                # 在主线程中直接启动JVM（避免线程问题）
                try:
                    app_logger.info("Directly executing jpype.startJVM in main thread...")
                    jpype.startJVM(
                        jvm_path,
                        f"-Djava.class.path={self.jar_path}",
                        convertStrings=False
                    )
                    app_logger.info("jpype.startJVM completed successfully")
                except Exception as e:
                    app_logger.error(f"JVM start exception: {str(e)}")
                    import traceback
                    app_logger.error(f"JVM start traceback: {traceback.format_exc()}")
                    return False
                except BaseException as e:
                    app_logger.error(f"JVM start fatal error: {str(e)}")
                    import traceback
                    app_logger.error(f"JVM start fatal traceback: {traceback.format_exc()}")
                    return False

                self.jvm_started = True
                app_logger.info("JVM started successfully")

                # Initialize password encoder
                try:
                    app_logger.info("Initializing password encoder")
                    CustomerPasswordEncoder = jpype.JClass('com.example.report.config.CustomerPasswordEncoder')
                    app_logger.info("CustomerPasswordEncoder class loaded successfully")
                    self.encoder = CustomerPasswordEncoder()
                    app_logger.info("Password encoder initialized successfully")
                except Exception as e:
                    error_msg = f"Failed to initialize password encoder: {e}"
                    app_logger.error(error_msg)
                    import traceback
                    app_logger.error(f"Password encoder traceback: {traceback.format_exc()}")
                    return False
            else:
                app_logger.info("JVM already started")

            return True

        except Exception as e:
            error_msg = f"Failed to start JVM: {e}"
            app_logger.error(error_msg)
            import traceback
            app_logger.error(f"JVM start traceback: {traceback.format_exc()}")
            return False
        except BaseException as e:
            error_msg = f"Fatal error during JVM startup: {e}"
            app_logger.error(error_msg)
            import traceback
            app_logger.error(f"Fatal error traceback: {traceback.format_exc()}")
            return False

    def shutdown_jvm(self):
        """Shutdown JVM"""
        if self.jvm_started and jpype.isJVMStarted():
            try:
                jpype.shutdownJVM()
                self.jvm_started = False
                app_logger.info("JVM shut down")
            except Exception as e:
                app_logger.error(f"Error shutting down JVM: {e}")
        else:
            app_logger.info("JVM not started or already shut down")

    def encode_password(self, raw_password: str) -> Optional[str]:
        """Encode password"""
        app_logger.debug(f"Start encoding password, raw password length: {len(raw_password) if raw_password else 0}")
        try:
            if not self.jvm_started and not self.start_jvm():
                app_logger.warning("JVM startup failed, cannot encode password")
                return None

            if not self.encoder:
                app_logger.warning("Password encoder not initialized")
                return None

            encoded_password = self.encoder.encode(raw_password)
            app_logger.debug("Password encoded successfully")
            return str(encoded_password)

        except Exception as e:
            error_msg = f"Password encoding failed: {e}"
            app_logger.error(error_msg)
            return None

    def verify_password(self, raw_password: str, encoded_password: str) -> bool:
        """Verify if passwords match"""
        app_logger.info(
            f"Start verifying password, raw password length: {len(raw_password)}, encoded password length: {len(encoded_password)}")
        try:
            if not self.jvm_started and not self.start_jvm():
                app_logger.warning("JVM startup failed, cannot verify password")
                return False

            if not self.encoder:
                app_logger.warning("Password encoder not initialized")
                return False

            is_match = self.encoder.matches(raw_password, encoded_password)
            app_logger.info(f"Password verification result: {is_match}")
            return bool(is_match)

        except Exception as e:
            error_msg = f"Password verification failed: {e}"
            app_logger.error(error_msg)
            return False


# Global singleton instance
password_handler = JarPasswordHandler()


def get_password_handler() -> JarPasswordHandler:
    """Get password handler instance"""
    app_logger.info("Getting password handler instance")
    return password_handler
