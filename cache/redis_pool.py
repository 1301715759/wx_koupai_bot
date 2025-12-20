import redis
import threading
from typing import Optional

class RedisConnectionPool:
    """Redis连接池管理器"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(RedisConnectionPool, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        # 避免重复初始化
        if hasattr(self, '_initialized'):
            return
            
        # 创建Redis连接池
        self.connection_pool = redis.ConnectionPool(
            host='127.0.0.1',
            port=6379,
            db=0,
            max_connections=20,
            decode_responses=True
        )
        
        # 创建用于后台任务结果的连接池
        self.backend_pool = redis.ConnectionPool(
            host='127.0.0.1',
            port=6379,
            db=1,
            max_connections=10
        )
        
        self._initialized = True
    
    def get_connection(self, db: int = 0) -> redis.Redis:
        """获取Redis连接"""
        if db == 0:
            return redis.Redis(connection_pool=self.connection_pool)
        elif db == 1:
            return redis.Redis(connection_pool=self.backend_pool)
        else:
            return redis.Redis(
                host='127.0.0.1',
                port=6379,
                db=db,
                decode_responses=True
            )

# 全局Redis连接池实例
redis_pool = RedisConnectionPool()

def get_redis_connection(db: int = 0) -> redis.Redis:
    """获取Redis连接的便捷函数"""
    return redis_pool.get_connection(db)