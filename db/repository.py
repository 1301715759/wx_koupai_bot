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
    async def get_all_active_groups_info():
        """获取所有活跃群组信息"""
        query = "SELECT group_wxid, start_koupai, end_koupai, end_renwu, limit_koupai, verify_mode, maixu_desc, welcome_msg, exit_msg FROM groups_config WHERE is_active = 1"
        return await db_manager.execute_query(query)
    @staticmethod
    async def get_all_active_groups():
        """获取所有活跃群组"""
        query = "SELECT group_wxid FROM groups_config WHERE is_active = 1"
        return await db_manager.execute_query(query)
    @staticmethod
    async def get_group_by_wxid(group_wxid: str):
        """根据微信ID获取群组信息"""
        query = "SELECT * FROM groups_config WHERE group_wxid = ?"
        return await db_manager.execute_single_query(query, (group_wxid,))
    @staticmethod
    async def get_group_welcome(group_wxid: str):
        """获取群组欢迎词"""
        query = "SELECT welcome_msg FROM groups_config WHERE group_wxid = ?"
        return await db_manager.execute_single_query(query, (group_wxid,))
    @staticmethod
    async def get_group_exit(group_wxid: str):
        """获取群组退群词"""
        query = "SELECT exit_msg FROM groups_config WHERE group_wxid = ?"
        return await db_manager.execute_single_query(query, (group_wxid,))
    @staticmethod
    async def get_group_maixu_desc(group_wxid: str):
        """获取群组麦序描述"""
        query = "SELECT maixu_desc FROM groups_config WHERE group_wxid = ?"
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
    async def create_group(group_wxid: str, group_name: str = "", is_active: bool = True):
        """创建新群组"""
        query = "INSERT INTO groups_config (group_wxid, group_name, is_active) VALUES (?, ?, ?)"
        return await db_manager.execute_update(query, (group_wxid, group_name, is_active))
    
    @staticmethod
    async def delete_group(group_wxid: str):
        """删除群组信息"""
        query = "DELETE FROM groups_config  WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (group_wxid,))
    @staticmethod
    async def update_group_active(group_wxid: str, new_active: bool):
        """更新群组是否启用"""
        query = "UPDATE groups_config SET is_active = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (new_active, group_wxid))
    @staticmethod
    async def update_group_maixu_desc(group_wxid: str, new_maixu_desc: str):
        """更新群组麦序描述"""
        query = "UPDATE groups_config SET maixu_desc = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (new_maixu_desc, group_wxid))
    @staticmethod
    async def update_group_start_koupai(group_wxid: str, start_koupai: int):
        """更新群组扣排开始时间"""
        query = "UPDATE groups_config SET start_koupai = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (start_koupai, group_wxid))
    @staticmethod
    async def update_group_end_koupai(group_wxid: str, end_koupai: int):
        """更新群组扣排结束时间"""
        query = "UPDATE groups_config SET end_koupai = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (end_koupai, group_wxid))
    @staticmethod
    async def update_group_end_task(group_wxid: str, end_renwu: int):
        """更新群组结束任务时间"""
        query = "UPDATE groups_config SET end_renwu = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (end_renwu, group_wxid))
    @staticmethod
    async def update_group_limit_koupai(group_wxid: str, limit_koupai: int):
        """更新群组扣排最大人数"""
        query = "UPDATE groups_config SET limit_koupai = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (limit_koupai, group_wxid))
    @staticmethod
    async def update_group_mode_koupai(group_wxid: str, verify_mode: str):
        """更新群组扣排模式"""
        query = "UPDATE groups_config SET verify_mode = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (verify_mode, group_wxid))
    @staticmethod
    async def add_group_host(group_wxid: str, start_hour: int, end_hour: int, host_desc: str):
        """添加群组主持"""
        query = """
        INSERT INTO host_schedule (group_wxid, start_hour, end_hour, host_desc)
        VALUES (?, ?, ?, ?)
        """
        return await db_manager.execute_update(query, (group_wxid, start_hour, end_hour, host_desc))
    @staticmethod
    async def delete_group_host(group_wxid: str):
        """删除指定group_wxid host记录"""
        query = "DELETE FROM host_schedule WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (group_wxid,))
    @staticmethod
    async def get_group_hosts(group_wxid: str):
        """获取群组主持信息"""
        query = "SELECT * FROM host_schedule WHERE group_wxid = ? ORDER BY start_hour"
        return await db_manager.execute_query(query, (group_wxid,))
    @staticmethod
    async def get_groups_config_has_hosts():
        """获取存在主持任务的群组扣排相关信息"""
        query = """
        SELECT h.group_wxid, 
               c.start_koupai, c.end_koupai, c.end_renwu, c.limit_koupai, c.verify_mode
        FROM host_schedule h
        LEFT JOIN groups_config c ON h.group_wxid = c.group_wxid
        ORDER BY h.group_wxid, h.start_hour
        """
        return await db_manager.execute_query(query)
    @staticmethod
    async def get_all_hosts():
        """获取所有群组主持信息"""
        query = "SELECT group_wxid, start_hour, end_hour, host_desc, lianpai_desc  FROM host_schedule ORDER BY group_wxid, start_hour"
        return await db_manager.execute_query(query)

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