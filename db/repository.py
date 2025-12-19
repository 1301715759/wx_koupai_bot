"""数据库仓库模块，提供对各个表的操作方法"""

from .database import db_manager

class GroupRepository:
    """群组信息数据访问类"""
    
    @staticmethod
    async def get_all_groups():
        """获取所有群组信息"""
        query = "SELECT * FROM groups_config ORDER BY created_at DESC"
        return await db_manager.execute_query(query)
    
    @staticmethod
    async def get_group_by_wxid(group_wxid: str):
        """根据微信ID获取群组信息"""
        query = "SELECT * FROM groups_config WHERE group_wxid = ?"
        return await db_manager.execute_single_query(query, (group_wxid,))
    @staticmethod
    async def update_group_welcome(group_wxid: str, new_welcome: str):
        """更新群组欢迎词"""
        query = "UPDATE groups_config SET welcome_msg = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (new_welcome, group_wxid))
    @staticmethod
    async def update_group_leave(group_wxid: str, new_leave: str):
        """更新群组退群词"""
        query = "UPDATE groups_config SET exit_msg = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (new_leave, group_wxid))
    
    @staticmethod
    async def create_group(group_wxid: str, group_name: str = "", welcome_msg: str = ""):
        """创建新群组"""
        query = "INSERT INTO groups_config (group_wxid, group_name, welcome_msg) VALUES (?, ?, ?)"
        return await db_manager.execute_update(query, (group_wxid, group_name, welcome_msg))
    
    @staticmethod
    async def delete_group(group_wxid: str):
        """删除群组信息"""
        query = "DELETE FROM groups_config  WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (group_wxid,))
    

class CommandRepository:
    """命令记录数据访问类"""
    
    @staticmethod
    async def get_commands_by_group(group_wxid: str, limit: int = 10):
        """获取指定群组的命令历史"""
        query = "SELECT * FROM commands WHERE group_wxid = ? ORDER BY executed_at DESC LIMIT ?"
        return await db_manager.execute_query(query, (group_wxid, limit))
    
    @staticmethod
    async def get_all_commands(limit: int = 50):
        """获取所有命令历史"""
        query = "SELECT * FROM commands ORDER BY executed_at DESC LIMIT ?"
        return await db_manager.execute_query(query, (limit,))
    
    @staticmethod
    async def delete_old_commands(days: int = 30):
        """删除指定天数之前的旧命令记录"""
        query = "DELETE FROM commands WHERE executed_at < datetime('now', '-{} days')".format(days)
        return await db_manager.execute_update(query)

# 创建仓库实例
group_repo = GroupRepository()
command_repo = CommandRepository()