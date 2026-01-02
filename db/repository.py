"""数据库仓库模块，提供对各个表的操作方法"""

from .database import db_manager
import json


class GroupRepository:
    """群组信息数据访问类"""

    @staticmethod
    async def get_all_groups():
        """获取所有群组信息"""
        query = "SELECT * FROM groups_config ORDER BY created_at DESC"
        return await db_manager.execute_query(query)

    @staticmethod
    async def get_all_active_groups_info(group_wxid: str = None):
        """获取所有活跃群组信息。如果存在传入的group_wxid，则只返回该群组的信息"""
        if group_wxid:
            query = "SELECT group_wxid, start_koupai, end_koupai, end_renwu, limit_koupai, verify_mode, maixu_desc, welcome_msg, exit_msg, renwu_desc, re_time, qu_time, p_qu, renwu_qu, bb_time, bb_limit, bb_in_hour, bb_timeout_desc, bb_back_desc, fixed_p_num, fixed_renwu_desc FROM groups_config WHERE is_active = 1 AND group_wxid = ?"
            return await db_manager.execute_single_query(query, (group_wxid,))
        query = "SELECT group_wxid, start_koupai, end_koupai, end_renwu, limit_koupai, verify_mode, maixu_desc, welcome_msg, exit_msg, renwu_desc, re_time, qu_time, p_qu, renwu_qu, bb_time, bb_limit, bb_in_hour, bb_timeout_desc, bb_back_desc, fixed_p_num, fixed_renwu_desc FROM groups_config WHERE is_active = 1"
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
    async def create_group(
        group_wxid: str, group_name: str = "", is_active: bool = True
    ):
        """创建新群组"""
        query = "INSERT INTO groups_config (group_wxid, group_name, is_active) VALUES (?, ?, ?)"
        return await db_manager.execute_update(
            query, (group_wxid, group_name, is_active)
        )

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
    async def update_group_fixed_p_num(group_wxid: str, fixed_num: int):
        """更新群组固定手速排人数"""
        query = "UPDATE groups_config SET fixed_p_num = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (fixed_num, group_wxid))

    @staticmethod
    async def update_group_fixed_renwu_desc(group_wxid: str, fixed_renwu_desc: str):
        """更新群组固定手速排可以打下来的任务"""
        query = "UPDATE groups_config SET fixed_renwu_desc = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (fixed_renwu_desc, group_wxid))

    @staticmethod
    async def update_group_mode_koupai(group_wxid: str, verify_mode: str):
        """更新群组扣排模式"""
        query = "UPDATE groups_config SET verify_mode = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (verify_mode, group_wxid))

    @staticmethod
    async def update_group_renwu_desc(group_wxid: str, new_renwu_desc: str):
        """更新群组任务描述"""
        query = "UPDATE groups_config SET renwu_desc = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (new_renwu_desc, group_wxid))

    @staticmethod
    async def add_group_host(
        group_wxid: str,
        start_hour: int,
        end_hour: int,
        host_desc: str,
        lianpai_desc: str = "",
        start_schedule: int = 0,
        end_schedule: int = 0,
    ):
        """添加群组主持"""
        query = """
        INSERT INTO host_schedule (group_wxid, start_hour, end_hour, host_desc, lianpai_desc, start_schedule, end_schedule)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        return await db_manager.execute_update(
            query,
            (
                group_wxid,
                start_hour,
                end_hour,
                host_desc,
                lianpai_desc,
                start_schedule,
                end_schedule,
            ),
        )
    @staticmethod
    async def add_group_members_tasks(tasks_members: list[tuple[str, str, str, str, str, str]]):
        """批量添加群成员扣排记录(不需要score)并无视报错"""
        query = """
        INSERT OR IGNORE INTO group_members_tasks (group_wxid, member_wxid, koupai_type, state, task_time_hour, task_time_date)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        params_list = [
            (group_wxid, member_wxid, koupai_type, state, hour, date)
            for group_wxid, member_wxid, koupai_type, state, hour, date in tasks_members
        ]
        return await db_manager.execute_many(query, params_list)
    @staticmethod
    async def get_group_members_tasks_by_date(group_wxid: str, date: str, start_hour: int = None, end_hour: int = None):
        """获取群组成员扣排记录"""
        if start_hour is None:
            start_hour = 0
        if end_hour is None:
            end_hour = 24
        query = """
        SELECT member_wxid, koupai_type, state, task_time_hour, task_time_date FROM group_members_tasks
        WHERE group_wxid = ? AND task_time_date = ? AND task_time_hour >= ? AND task_time_hour < ?
        """
        return await db_manager.execute_query(query, (group_wxid, date, start_hour, end_hour))

    @staticmethod
    async def add_group_member_bb(group_wxid: str, member_wxid: str, msg_content: str = None, create_time: int = None, back_time: int = None,  is_timeout: int = 0):
        """添加群组成员报备（如果存在则更新）"""
        query = """
        INSERT OR REPLACE INTO group_members_bb (group_wxid, member_wxid, msg_content, create_time, back_time, is_timeout)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(group_wxid, member_wxid, create_time) DO UPDATE SET
        msg_content = COALESCE(?, msg_content),  -- 只更新非NULL值
        back_time = COALESCE(?, back_time),
        is_timeout = COALESCE(?, is_timeout),
        updated_at = datetime(CURRENT_TIMESTAMP, 'localtime')
        """
        return await db_manager.execute_update(query, (
            group_wxid, member_wxid, msg_content, create_time, back_time, is_timeout, 
            msg_content, back_time, is_timeout,  ))
    @staticmethod
    async def update_group_member_score(group_wxid: str, member_wxid: str, accumulate_score: float = None, complete_score: float = None):
        """更新群组成员积分（如果不存在则插入，保留其他数据）"""
        query = """
        INSERT OR REPLACE INTO group_members_roles (group_wxid, member_wxid, accumulate_score, complete_score)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(group_wxid, member_wxid) DO UPDATE SET
        accumulate_score = COALESCE(?, accumulate_score),
        complete_score = COALESCE(?, complete_score),
        updated_at = datetime(CURRENT_TIMESTAMP, 'localtime')
        """
        return await db_manager.execute_update(query, (
            group_wxid, member_wxid, accumulate_score, complete_score,
            accumulate_score, complete_score))
    @staticmethod
    async def update_group_re_time(group_wxid: str, re_time: int):
        """更新群组补位时间"""
        query = "UPDATE groups_config SET re_time = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (re_time, group_wxid))

    @staticmethod
    async def update_group_p_qu(group_wxid: str, p_qu: bool):
        """更新群组手速不可取"""
        query = "UPDATE groups_config SET p_qu = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (p_qu, group_wxid))

    @staticmethod
    async def update_group_renwu_qu(group_wxid: str, renwu_qu: bool):
        """更新群组任务排不可取"""
        query = "UPDATE groups_config SET renwu_qu = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (renwu_qu, group_wxid))

    @staticmethod
    async def update_group_qu_time(group_wxid: str, qu_time: int):
        """更新群组取时间"""
        query = "UPDATE groups_config SET qu_time = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (qu_time, group_wxid))

    @staticmethod
    async def update_group_bb_time(group_wxid: str, bb_time: int):
        """更新群组报备时间"""
        query = "UPDATE groups_config SET bb_time = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (bb_time, group_wxid))

    @staticmethod
    async def update_group_bb_limit(group_wxid: str, bb_limit: int):
        """更新群组报备最大人数"""
        query = "UPDATE groups_config SET bb_limit = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (bb_limit, group_wxid))

    @staticmethod
    async def update_group_bb_in_hour(group_wxid: str, bb_in_hour: int):
        """更新群组报备小时内报备次数"""
        query = "UPDATE groups_config SET bb_in_hour = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (bb_in_hour, group_wxid))

    @staticmethod
    async def update_group_timeout_desc(group_wxid: str, timeout_desc: str):
        """更新群组超时提醒词"""
        query = "UPDATE groups_config SET bb_timeout_desc = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (timeout_desc, group_wxid))

    @staticmethod
    async def update_group_bb_back_desc(group_wxid: str, bb_back_desc: str):
        """更新群组回厅词"""
        query = "UPDATE groups_config SET bb_back_desc = ? WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (bb_back_desc, group_wxid))

    @staticmethod
    async def update_group_member_is_baned(
        group_wxid: str, member_wxid: str, is_baned: bool
    ):
        """更新群组成员是否被ban(如果不存在该成员则插入)"""
        query = """
        INSERT INTO group_members_roles (group_wxid, member_wxid, is_baned)
        VALUES (?, ?, ?)
        ON CONFLICT(group_wxid, member_wxid) DO UPDATE SET
            is_baned = excluded.is_baned,
            updated_at = datetime(CURRENT_TIMESTAMP, 'localtime')
        """
        return await db_manager.execute_update(
            query, (group_wxid, member_wxid, is_baned)
        )

    @staticmethod
    async def add_group_member_benefits(
        group_wxid: str,
        member_wxid: str,
        card: str,
        num: int,
        expire_at: str = "9999-12-31 23:59:59",
    ):
        """添加群组成员权益卡片"""
        # 添加的时候应当检查是否存在相同的记录
        # 如果存在相同的记录，那么就更新expire_at, num = old_num+new_num
        query = """
        INSERT INTO group_members_benefits (group_wxid, member_wxid, card, num, expire_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(group_wxid, member_wxid, card) DO UPDATE SET
            num = num + excluded.num,
            expire_at = excluded.expire_at,
            updated_at = datetime(CURRENT_TIMESTAMP, 'localtime')
        """

        return await db_manager.execute_update(
            query, (group_wxid, member_wxid, card, num, expire_at)
        )

    @staticmethod
    async def delete_group_host(group_wxid: str):
        """删除指定group_wxid host记录"""
        query = "DELETE FROM host_schedule WHERE group_wxid = ?"
        return await db_manager.execute_update(query, (group_wxid,))

    @staticmethod
    async def delete_group_fixed_host(group_wxid: str, fixed_wxid: str):
        """删除指定group_wxid fixed_host记录"""
        query = (
            "DELETE FROM fixed_host_schedule WHERE group_wxid = ? AND fixed_wxid = ?"
        )
        return await db_manager.execute_update(query, (group_wxid, fixed_wxid))

    @staticmethod
    async def add_group_host_with_fixed_wxid(
        group_wxid: str, start_hour: int, end_hour: int, fixed_wxid: str
    ):
        """
        存在相同group_wxid和start_hour fixed_wxid 那就不添加
        不存在就直接插入。
        """
        query = """
        INSERT OR IGNORE INTO fixed_host_schedule (group_wxid, start_hour, end_hour, fixed_wxid)
        VALUES (?, ?, ?, ?)
        """
        return await db_manager.execute_update(
            query, (group_wxid, start_hour, end_hour, fixed_wxid)
        )

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
    async def get_fixed_hosts(group_wxid: str):
        """获取群组固定排成员信息"""
        query = "SELECT group_wxid, start_hour, end_hour, fixed_wxid FROM fixed_host_schedule WHERE group_wxid = ? ORDER BY start_hour"
        return await db_manager.execute_query(query, (group_wxid,))

    @staticmethod
    async def get_all_hosts(group_wxid: str = None):
        """获取所有群组主持信息。如果存在传入的group_wxid，则只返回该群组的信息"""
        if group_wxid:
            query = "SELECT group_wxid, start_hour, end_hour, host_desc, lianpai_desc, start_schedule, end_schedule FROM host_schedule WHERE group_wxid = ? ORDER BY start_hour"
            return await db_manager.execute_query(query, (group_wxid,))
        query = "SELECT group_wxid, start_hour, end_hour, host_desc, lianpai_desc, start_schedule, end_schedule FROM host_schedule ORDER BY group_wxid, start_hour"
        return await db_manager.execute_query(query)

    @staticmethod
    async def get_all_fixed_hosts(group_wxid: str = None):
        """获取所有群组固定排成员信息。如果存在传入的group_wxid，则只返回该群组的信息"""
        if group_wxid:
            query = "SELECT group_wxid, start_hour, end_hour, fixed_wxid FROM fixed_host_schedule WHERE group_wxid = ? ORDER BY start_hour"
            return await db_manager.execute_query(query, (group_wxid,))
        query = "SELECT group_wxid, start_hour, end_hour, fixed_wxid FROM fixed_host_schedule ORDER BY group_wxid, start_hour"
        return await db_manager.execute_query(query)
    @staticmethod
    async def get_all_group_members_roles(group_wxid: str = None, member_wxid: str = None):
        """获取所有群组成员角色信息。如果存在传入的group_wxid，则只返回该群组的信息。如果存在传入的member_wxid，则只返回该成员的信息"""
        if group_wxid and member_wxid:
            query = "SELECT group_wxid, member_wxid, roles, is_baned, accumulate_score, complete_score FROM group_members_roles WHERE group_wxid = ? AND member_wxid = ?"
            return await db_manager.execute_single_query(query, (group_wxid, member_wxid))
        if group_wxid:
            query = "SELECT group_wxid, member_wxid, roles, is_baned, accumulate_score, complete_score FROM group_members_roles WHERE group_wxid = ? ORDER BY member_wxid"
            return await db_manager.execute_query(query, (group_wxid,))
        query = "SELECT group_wxid, member_wxid, roles, is_baned, accumulate_score, complete_score FROM group_members_roles ORDER BY group_wxid, member_wxid"
        return await db_manager.execute_query(query)
    @staticmethod
    async def get_all_group_member_baned():
        """获取所有群组被禁排成员信息"""
        query = """
        SELECT group_wxid, member_wxid, is_baned FROM group_members_roles 
        WHERE is_baned = 1
        ORDER BY group_wxid, member_wxid
        """
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
        query = "DELETE FROM commands WHERE executed_at < datetime('now', '-{} days')".format(
            days
        )
        return await db_manager.execute_update(query)


# 创建仓库实例
group_repo = GroupRepository()
command_repo = CommandRepository()
