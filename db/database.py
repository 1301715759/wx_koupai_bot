import aiosqlite
import asyncio
from typing import Optional, AsyncGenerator
from contextlib import asynccontextmanager
import logging
from collections import deque

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 数据库路径
DB_PATH = "local_group.db"

class DatabaseManager:
    """异步数据库管理器，使用aiosqlite实现"""
    
    def __init__(self, db_path="local_group.db", pool_size=10):
        self.db_path = db_path
        self.pool_size = pool_size
        self._connection_pool = deque()
        self._current_connections = 0
        self._lock = asyncio.Lock()
        self._closed = False
        
    async def initialize(self):
        """初始化数据库，创建必要的表"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                # 创建群组信息表
                await db.execute('''
                CREATE TABLE IF NOT EXISTS groups_config (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_wxid TEXT UNIQUE NOT NULL,      -- 群ID（唯一标识）
                    group_name TEXT,                    -- 群名称
                    welcome_msg TEXT DEFAULT '',        -- 欢迎词
                    exit_msg TEXT DEFAULT '',           -- 退群词
                    is_active BOOLEAN DEFAULT 0,        -- 是否启用
                    maixu_desc TEXT DEFAULT '',         -- 群的麦序描述
                    start_koupai INTEGER DEFAULT 45,    -- 开始扣排时间（单位分钟）
                    end_koupai INTEGER DEFAULT 58,      -- 结束扣排时间（单位分钟）
                    end_renwu INTEGER DEFAULT 58,       -- 结束任务时间（单位分钟）
                    limit_koupai INTEGER DEFAULT 8,    --  最大人数 
                    verify_mode TEXT DEFAULT '字符',     --  扣排模式
                    created_at TIMESTAMP DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')),
                    updated_at TIMESTAMP DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime'))
                )
                ''')
                
                 # 创建主持时间段配置表
                await db.execute('''
                CREATE TABLE IF NOT EXISTS host_schedule (
                    -- id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_wxid TEXT NOT NULL,             -- 群ID
                    start_hour INTEGER NOT NULL,        -- 开始时间(0-23)
                    end_hour INTEGER NOT NULL,          -- 结束时间(0-23)
                    host_desc TEXT,                -- 主持别称
                    lianpai_desc BOOLEAN DEFAULT 0,  -- 是否连排
                    created_at TIMESTAMP DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')),
                    updated_at TIMESTAMP DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')),
                    FOREIGN KEY (group_wxid) REFERENCES groups_config (group_wxid) ON DELETE CASCADE,
                    CHECK(start_hour >= 0 AND start_hour <= 23),
                    CHECK(end_hour >= 0 AND end_hour <= 24),
                    CHECK(start_hour < end_hour)      -- 开始时间必须小于结束时间
                )
                ''')
                
                # 创建更新时间的触发器
                await db.execute('''
                CREATE TRIGGER IF NOT EXISTS update_groups_timestamp 
                AFTER UPDATE ON groups_config
                BEGIN
                    UPDATE groups_config SET updated_at = (datetime(CURRENT_TIMESTAMP, 'localtime')) WHERE id = NEW.id;
                END;
                ''')

                # 创建命令记录表
                await db.execute('''
                CREATE TABLE IF NOT EXISTS commands (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_wxid TEXT NOT NULL,
                    command TEXT NOT NULL,
                    executed_at TIMESTAMP DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')),
                    FOREIGN KEY (group_wxid) REFERENCES groups_config (group_wxid) ON DELETE CASCADE
                )
                ''')
                
                await db.commit()
                logger.info("数据库初始化成功")
                # 重置关闭状态（如果之前已关闭）
                self._closed = False
        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            raise
    
    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[aiosqlite.Connection, None]:
        """获取数据库连接的上下文管理器（支持连接池）"""
        if self._closed:
            raise RuntimeError("Database manager is closed")
            
        conn = None
        async with self._lock:
            if self._connection_pool:
                # 从连接池中获取连接
                conn = self._connection_pool.popleft()
            elif self._current_connections < self.pool_size:
                # 创建新连接
                conn = await aiosqlite.connect(self.db_path)
                self._current_connections += 1
            else:
                # 连接池已满，等待可用连接
                # 这里简化处理，直接创建临时连接（在生产环境中可能需要更好的处理方式）
                conn = await aiosqlite.connect(self.db_path)
                self._current_connections += 1
        
        try:
            # 检查连接是否有效
            if conn:
                yield conn
            else:
                raise RuntimeError("Failed to acquire database connection")
        except Exception as e:
            # 如果发生异常，确保连接被正确处理
            if conn:
                async with self._lock:
                    await conn.close()
                    self._current_connections -= 1
            raise
        finally:
            # 将连接返回到连接池
            if conn:
                async with self._lock:
                    if not self._closed and len(self._connection_pool) < self.pool_size:
                        self._connection_pool.append(conn)
                    else:
                        # 连接池已满或已关闭，关闭连接
                        await conn.close()
                        self._current_connections -= 1
    
    async def execute_query(self, query: str, params: tuple = ()) -> list:
        """执行查询并返回所有结果"""
        try:
            async with self.get_connection() as conn:
                cursor = await conn.execute(query, params)
                results = await cursor.fetchall()
                return results
        except Exception as e:
            logger.error(f"查询执行失败: {query}, 参数: {params}, 错误: {e}")
            raise
    
    async def execute_single_query(self, query: str, params: tuple = ()):
        """执行查询并返回单个结果"""
        try:
            async with self.get_connection() as conn:
                cursor = await conn.execute(query, params)
                result = await cursor.fetchone()
                return result
        except Exception as e:
            logger.error(f"单条查询执行失败: {query}, 参数: {params}, 错误: {e}")
            raise
    
    async def execute_update(self, query: str, params: tuple = ()) -> int:
        """执行更新操作并返回影响的行数"""
        try:
            async with self.get_connection() as conn:
                cursor = await conn.execute(query, params)
                await conn.commit()
                rowcount = cursor.rowcount
                logger.info(f"更新操作成功，影响行数: {rowcount}")
                return rowcount
        except Exception as e:
            logger.error(f"更新操作失败: {query}, 参数: {params}, 错误: {e}")
            raise
    
    async def add_group(self, group_wxid: str) -> bool:
        """添加群组信息"""
        try:
            async with self.get_connection() as conn:
                await conn.execute(
                    "INSERT OR IGNORE INTO groups_config (group_wxid) VALUES (?)",
                    (group_wxid)
                )
                await conn.commit()
                logger.info(f"群组添加成功: {group_wxid}")
                return True
        except Exception as e:
            logger.error(f"添加群组时出错: {e}")
            return False
    
    async def record_command(self, group_wxid: str, command: str) -> bool:
        """记录命令执行历史"""
        try:
            async with self.get_connection() as conn:
                await conn.execute(
                    "INSERT INTO commands (group_wxid, command) VALUES (?, ?)",
                    (group_wxid, command)
                )
                await conn.commit()
                logger.info(f"命令记录成功: {group_wxid} - {command}")
                return True
        except Exception as e:
            logger.error(f"记录命令时出错: {e}")
            return False

    async def close_all_connections(self):
        """关闭所有连接"""
        async with self._lock:
            # 标记数据库管理器已关闭
            self._closed = True
            
            # 关闭连接池中的所有连接
            while self._connection_pool:
                conn = self._connection_pool.popleft()
                try:
                    await conn.close()
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")
                    
            self._current_connections = 0
            logger.info("All database connections closed")

# 全局数据库管理器实例
db_manager = DatabaseManager()

# 初始化数据库
async def init_database():
    """初始化数据库"""
    await db_manager.initialize()