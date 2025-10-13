import hashlib
import secrets
import os
from typing import Optional, Tuple


class Ssha2Hash:
    """
    内部类，用于解析和构造{SHA512}hash$salt$iterations格式的哈希字符串
    """
    def __init__(self, arg_digest=None, arg_salt=None, arg_iterations=None):
        """
        构造函数，支持两种方式：
        1. 从字符串解析：Ssha2Hash(digest_string)
        2. 从字节数组构造：Ssha2Hash(digest_bytes, salt_bytes, iterations)
        """
        if isinstance(arg_digest, str):
            # 从字符串解析
            self._parse_from_string(arg_digest)
        elif arg_digest is not None and arg_salt is not None and arg_iterations is not None:
            # 从字节数组构造
            self._digest = bytes(arg_digest)
            self._salt = bytes(arg_salt)
            self._iterations = int(arg_iterations)
        else:
            raise ValueError("Invalid arguments for Ssha2Hash constructor")
    
    def _parse_from_string(self, arg_digest: str):
        """从字符串解析哈希信息"""
        digest = arg_digest
        
        # 移除{SHA512}前缀（如果存在）
        if digest.startswith("{SHA512}"):
            digest = digest[len("{SHA512}"):]
        
        # 按$分割
        parts = digest.split('$')
        
        # 解析digest（十六进制）
        self._digest = bytes.fromhex(parts[0])
        
        # 解析salt（十六进制）
        if len(parts) > 1:
            self._salt = bytes.fromhex(parts[1])
        else:
            self._salt = b''
        
        # 解析iterations
        if len(parts) > 2:
            self._iterations = int(parts[2])
        else:
            self._iterations = 100000  # DEFAULT_ITERATIONS
    
    def get_digest(self) -> bytes:
        """获取digest字节数组"""
        return self._digest
    
    def get_salt(self) -> bytes:
        """获取salt字节数组"""
        return self._salt
    
    def get_iterations(self) -> int:
        """获取迭代次数"""
        return self._iterations
    
    def __str__(self) -> str:
        """转换为字符串格式：{SHA512}digest$salt$iterations"""
        result = []
        result.append("{SHA512}")
        result.append(self._digest.hex())
        result.append('$')
        result.append(self._salt.hex())
        result.append('$')
        result.append(str(self._iterations))
        return ''.join(result)


class Ssha2Hasher:
    DEFAULT_ITERATIONS = 100000
    PREFIX = "{SHA512}"
    SALT_LENGTH = 8  # 8字节盐值
    
    def __init__(self):
        """构造函数"""
        pass
    
    def random_bytes(self, length: int) -> bytes:
        """生成随机字节数组"""
        return secrets.token_bytes(length)
    
    def calc_digest(self, plaintext_chars: str, salt: bytes, iterations: int) -> bytes:
        """
        计算PBKDF2-SHA512摘要
        """
        # 将字符串转换为UTF-8字节数组
        password_bytes = plaintext_chars.encode('utf-8')
        
        # 使用SHA-512进行迭代哈希，而不是PBKDF2
        digest_bytes = password_bytes
        
        for i in range(iterations):
            hasher = hashlib.sha512()
            hasher.update(digest_bytes)
            hasher.update(salt)
            digest_bytes = hasher.digest()
        
        return digest_bytes
    
    def hash(self, plaintext_chars: str) -> str:
        """
        生成密码哈希
        """
        # 生成8字节随机盐值
        salt = self.random_bytes(self.SALT_LENGTH)
        
        # 使用默认迭代次数
        iterations = self.DEFAULT_ITERATIONS
        
        # 计算摘要
        digest = self.calc_digest(plaintext_chars, salt, iterations)
        
        # 构造Ssha2Hash对象并返回字符串
        hash_obj = Ssha2Hash(digest, salt, iterations)
        return str(hash_obj)
    
    def matches(self, arg_hash: str) -> bool:
        """
        检查哈希字符串是否匹配SHA512格式
        """
        return arg_hash.upper().startswith(self.PREFIX.upper())
    
    def verify(self, arg_digest: str, plaintext_chars: str) -> bool:
        """
        验证密码
        """
        try:
            # 移除{SHA512}前缀
            digest_without_prefix = arg_digest
            if digest_without_prefix.startswith(self.PREFIX):
                digest_without_prefix = digest_without_prefix[len(self.PREFIX):]
            
            # 解析哈希信息
            hash_obj = Ssha2Hash(digest_without_prefix)
            
            # 计算当前密码的摘要
            calculated = self.calc_digest(plaintext_chars, hash_obj.get_salt(), hash_obj.get_iterations())
            
            # 比较摘要
            return calculated == hash_obj.get_digest()
            
        except Exception as e:
            print(f"验证过程中出错: {e}")
            return False


def test_python_ssha2_hasher():
    """测试Python实现的Ssha2Hasher"""
    
    # 测试数据
    encrypted_password = "{SHA512}8752e64c9ecc01ca6e427da46f323d1bd38e6325b5aef2fcc14df0f64905da4e86e861a7c2558387db5ae81bd957524afd56ebe180f1eb56e9a0973556cb4bd0$e1594e972f151268$100000"
    plain_password = "123456"
    
    print("=== Python Ssha2Hasher 测试 ===")
    print(f"测试密码: {plain_password}")
    print(f"目标哈希: {encrypted_password}")
    print()
    
    # 创建hasher实例
    hasher = Ssha2Hasher()
    
    # 测试格式匹配
    print("=== 格式匹配测试 ===")
    matches = hasher.matches(encrypted_password)
    print(f"格式匹配: {'✓' if matches else '✗'}")
    print()
    
    # 测试密码验证
    print("=== 密码验证测试 ===")
    verify_result = hasher.verify(encrypted_password, plain_password)
    print(f"密码验证: {'✓ 成功' if verify_result else '✗ 失败'}")
    return verify_result

#
# if __name__ == "__main__":
#     test_python_ssha2_hasher()