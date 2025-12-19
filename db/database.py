import aiosqlite
import asyncio
from typing import Optional, AsyncGenerator
from contextlib import asynccontextmanager
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 数据库路径
DB_PATH = "local_group.db"

class DatabaseManager:
    """异步数据库管理器，使用aiosqlite实现"""
    
    def __init__(self, db_path = "local_group.db"):
        self.db_path = db_path
        self._connection_pool = []
        
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
                    is_active BOOLEAN DEFAULT 1,        -- 是否启用
                    maixu_desc TEXT DEFAULT '',         -- 群的麦序描述
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                ''')
                
                 # 创建主持时间段配置表
                await db.execute('''
                CREATE TABLE IF NOT EXISTS host_schedule (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_wxid TEXT NOT NULL,             -- 群ID
                    start_hour INTEGER NOT NULL,        -- 开始时间(0-23)
                    end_hour INTEGER NOT NULL,          -- 结束时间(0-23)
                    hsot_desc TEXT,                -- 主持用户名
                    host_wxid TEXT,                     -- 主持用户ID
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (group_wxid) REFERENCES groups_config (group_wxid) ON DELETE CASCADE,
                    CHECK(start_hour >= 0 AND start_hour <= 23),
                    CHECK(end_hour >= 0 AND end_hour <= 23),
                    CHECK(start_hour < end_hour)      -- 开始时间必须小于结束时间
                )
                ''')
                
                # 创建更新时间的触发器
                await db.execute('''
                CREATE TRIGGER IF NOT EXISTS update_groups_timestamp 
                AFTER UPDATE ON groups_config
                BEGIN
                    UPDATE groups_config SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
                END;
                ''')
                
                await db.commit()
                logger.info("数据库初始化成功")
        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            raise
    
    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[aiosqlite.Connection, None]:
        """获取数据库连接的上下文管理器"""
        conn = await aiosqlite.connect(self.db_path)
        try:
            yield conn
        finally:
            await conn.close()
    
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

# 全局数据库管理器实例
db_manager = DatabaseManager()

# 初始化数据库
async def init_database():
    """初始化数据库"""
    await db_manager.initialize()