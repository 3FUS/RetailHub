# /Users/fu/Downloads/XY/TB/RH/app/core/jar_pwd_handler.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Java JAR package password processing module
Used to call methods in Oracle retail system password processing JAR package for password verification

Author: Demo
Date: 2024
"""

import jpype
import os
from pathlib import Path
from typing import Optional
from app.utils.logger import app_logger


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

    def find_jvm_path(self) -> str:
        """Find available JVM path"""
        app_logger.info("Starting to find available JVM path")
        # Try to get the correct Java path using java_home command
        try:
            import subprocess
            result = subprocess.run(["/usr/libexec/java_home"], capture_output=True, text=True)
            if result.returncode == 0:
                java_home = result.stdout.strip()
                app_logger.info(f"Found Java Home: {java_home}")

                # Prefer server version of JVM (macOS/Linux)
                jvm_path = os.path.join(java_home, "lib", "server", "libjvm.dylib")
                if os.path.exists(jvm_path):
                    app_logger.info(f"Found JVM path: {jvm_path}")
                    return jvm_path

                # Windows path
                jvm_path = os.path.join(java_home, "bin", "server", "jvm.dll")
                if os.path.exists(jvm_path):
                    app_logger.info(f"Found JVM path: {jvm_path}")
                    return jvm_path

                # Try libjli.dylib
                jvm_path = os.path.join(java_home, "lib", "libjli.dylib")
                if os.path.exists(jvm_path):
                    app_logger.info(f"Found JVM path: {jvm_path}")
                    return jvm_path

                # Try JRE path
                jvm_path = os.path.join(java_home, "jre", "lib", "server", "libjvm.dylib")
                if os.path.exists(jvm_path):
                    app_logger.info(f"Found JVM path: {jvm_path}")
                    return jvm_path

                jvm_path = os.path.join(java_home, "jre", "lib", "jli", "libjli.dylib")
                if os.path.exists(jvm_path):
                    app_logger.info(f"Found JVM path: {jvm_path}")
                    return jvm_path
        except Exception as e:
            app_logger.warning(f"java_home command execution failed: {e}")

        # Fallback paths
        possible_paths = [
            jpype.getDefaultJVMPath(),
            "/Library/Java/JavaVirtualMachines/jdk1.8.0_391.jdk/Contents/Home/lib/server/libjvm.dylib",
            "/Library/Java/JavaVirtualMachines/jdk1.8.0_391.jdk/Contents/Home/lib/jli/libjli.dylib",
            "/System/Library/Frameworks/JavaVM.framework/JavaVM",
            "C:\\Program Files\\Java\\jre1.8.0_421\\bin\\server\\jvm.dll"
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
            return True
        try:
            # Get JAR package path
            current_dir = Path(__file__).parent
            self.jar_path = current_dir / "lib" / "dtv-password.jar"

            if not self.jar_path.exists():
                warning_msg = f"Warning: JAR file does not exist: {self.jar_path}"
                app_logger.warning(warning_msg)
                return False

            # Start JVM
            if not jpype.isJVMStarted():
                jvm_path = self.find_jvm_path()
                app_logger.info(f"Using JVM path: {jvm_path}")
                jpype.startJVM(
                    jvm_path,
                    f"-Djava.class.path={self.jar_path}",
                    convertStrings=False
                )
                self.jvm_started = True
                app_logger.info("JVM started successfully")

                # Initialize password encoder
                try:
                    app_logger.info("Initializing password encoder")
                    CustomerPasswordEncoder = jpype.JClass('com.example.report.config.CustomerPasswordEncoder')
                    self.encoder = CustomerPasswordEncoder()
                    app_logger.info("Password encoder initialized successfully")
                except Exception as e:
                    error_msg = f"Failed to initialize password encoder: {e}"
                    app_logger.error(error_msg)
                    return False
            else:
                app_logger.info("JVM already started")

            return True

        except Exception as e:
            error_msg = f"Failed to start JVM: {e}"
            app_logger.error(error_msg)
            return False

    def shutdown_jvm(self):
        """Shutdown JVM"""
        if self.jvm_started and jpype.isJVMStarted():
            jpype.shutdownJVM()
            self.jvm_started = False
            app_logger.info("JVM shut down")

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
        app_logger.debug(f"Start verifying password, raw password length: {len(raw_password)}, encoded password length: {len(encoded_password)}")
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
