import asyncio
import re
from db.repository import group_repo, command_repo
from db.database import db_manager
from utils.send_utils import send_message, change_groupname
from command.rules.hostPhrase_rules import validate_time_slots_array, parse_time_slots
import json
from celery_tasks.initialize_tasks import initialize_tasks
class CommandHandler:
    """命令处理器，处理各种用户命令"""

    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.commands = {}
        self.register_commands()
    
    def register_commands(self):
        """注册所有可用命令"""
        self.commands = {
            "修改群昵称": {
                "description": "修改群昵称",
                "handler": self.handle_change_group_name
            },
            "设置欢迎词": {
                "description": "设置欢迎词",
                "handler": self.handle_set_welcome
            },
            "设置退群词": {
                "description": "设置退群词",
                "handler": self.handle_set_leave
            },
            "设置麦序文档": {
                "description": "设置麦序文档",
                "handler": self.handle_set_group_maixudesc
            },
            "设置主持": {
                "description": "设置主持，格式\n"
                                "设置主持\n"
                                "0-1aa\n"
                                "0-2bb\n",
                "handler": self.handle_set_host
            },
            "查询主持": {
                "description": "查询主持",
                "handler": self.handle_view_host
            },
            "查询麦序文档": {
                "description": "查询麦序文档",
                "handler": self.handle_view_group_maixudesc
            },
            "设置扣排时间": {
                "description": "设置扣排时间+数字（0-59分钟）",
                "handler": self.handle_set_koupai_start_time
            },
            "设置扣排截止时间": {
                "description": "设置扣排截止时间+数字（0-59分钟）" ,
                "handler": self.handle_set_koupai_end_time
            },
            "设置扣排人数": {
                "description": "设置扣排人数+数字（0-20）",
                "handler": self.handle_set_koupai_limit
            }
        }
    
    async def handle_command(self, command: str, group_wxid: str, **kwargs):
        """处理用户命令"""
        # 先查询数据库中是否存在该群，不存在则创建
        
        # 根据命令类型进行处理
        if command.startswith("修改昵称"):
            return await self.handle_change_group_name(command, group_wxid)
        elif command.startswith("设置欢迎词"):
            return await self.handle_set_welcome(command, group_wxid)
        elif command.startswith("设置退群词"):
            return await self.handle_set_leave(command, group_wxid)
        elif command.startswith("设置麦序文档"):
            return await self.handle_set_group_maixudesc(command, group_wxid)
        elif command.startswith("查询麦序文档"):
            return await self.handle_view_group_maixudesc(group_wxid)
        elif command.startswith("设置主持"):
            return await self.handle_set_host(command, group_wxid)
        elif command.startswith("查询主持"):
            return await self.handle_view_host(group_wxid)
        elif command.startswith("设置扣排时间"):
            return await self.handle_set_koupai_start_time(command, group_wxid)
        elif command.startswith("设置扣排截止时间"):
            return await self.handle_set_koupai_end_time(command, group_wxid)
        elif command.startswith("设置扣排人数"):
            return await self.handle_set_koupai_limit(command, group_wxid)
        elif command == "p":
            return
        else:
            return "未注册命令，请输入 /help 查看帮助信息"
    async def handle_event(self, event_type: str, group_wxid: str):
        """群成员进退群事件处理器"""
        if event_type == 1:
            group_welcome = await group_repo.get_group_welcome(group_wxid)
            if group_welcome:
                await send_message(group_wxid, group_welcome)
                return f"{group_wxid}欢迎词已发送"
        elif event_type == 0:
            group_exit = await group_repo.get_group_exit(group_wxid)
            if group_exit:
                await send_message(group_wxid, group_exit)
                return f"{group_wxid}退群词已发送"
    async def handle_help_command(self, group_wxid: str):
        """处理帮助命令"""
        help_text = "可用命令列表：\n"
        for cmd, info in self.commands.items():
            help_text += f"{cmd} - {info['description']}\n"
        return help_text
    
    async def handle_change_group_name(self, command: str, group_wxid: str):
        """修改群昵称命令"""
        # 解析命令格式：修改昵称 新名称
        parts = re.search(r'修改昵称(.*)', command)
        if not parts:
            return "命令格式错误，请使用：修改昵称 新名称"
        
        new_name = parts.group(1).strip()[:20]
        if not new_name:
            return "请提供新的群昵称"
        
        # 更新群组名称
        try:
            await change_groupname(group_wxid, new_name)
            await send_message(group_wxid, f"机器人昵称已修改为：{new_name}")
            return f"机器人昵称已修改为：{new_name}"
        except Exception as e:
            return f"修改机器人昵称失败：{str(e)}"
    
    async def handle_set_welcome(self, command: str, group_wxid: str):
        """处理设置欢迎词命令"""
        # 解析命令格式：设置欢迎词 欢迎词内容
        parts = re.search(r'设置欢迎词(.*)', command)
        if not parts:
            return "命令格式错误，请使用：设置欢迎词 欢迎词内容"
        
        welcome_msg = parts.group(1).strip()
        if not welcome_msg:
            return "请提供欢迎词内容"
        await group_repo.update_group_welcome(group_wxid, welcome_msg)
        await send_message(group_wxid, f"欢迎词已设置为：{welcome_msg}")
        return f"欢迎词已设置为：{welcome_msg}"
    
    async def handle_set_leave(self, command: str, group_wxid: str):
        """处理设置退群词命令"""
        # 解析命令格式：设置退群词 退群词内容
        parts = re.search(r'设置退群词(.*)', command)
        if not parts:
            return "命令格式错误，请使用：设置退群词 退群词内容"
        
        leave_msg = parts.group(1).strip()
        if not leave_msg:
            return "请提供退群词内容"
        
        # 这里可以保存退群词到数据库或其他存储
        await group_repo.update_group_leave(group_wxid, leave_msg)
        await send_message(group_wxid, f"退群词已设置为：{leave_msg}")
        return f"退群词已设置为：{leave_msg}"
    async def handle_set_group_maixudesc(self, command: str, group_wxid: str):
        """处理设置麦序文档命令"""
        # 解析命令格式：设置麦序文档 文档链接
        # 解析命令格式：设置麦序文档\r\n1234\r\n7788，截取1234、7788等行
        lines = command.splitlines()  # 分割换行
        maixudesc_lines = [line.strip() for line in lines[1:] if line.strip()]
        if not maixudesc_lines:
            return "命令格式错误，请使用：设置麦序文档\\r\\n文档内容"
        maixudesc = '\r'.join(maixudesc_lines)
        print(f"设置麦序文档内容：{maixudesc}")

        # 这里可以保存麦序文档到数据库或其他存储
        await group_repo.update_group_maixu_desc(group_wxid, maixudesc)
        await initialize_tasks.update_groups_config(group_wxid, {"maixu_desc": maixudesc})
        await send_message(group_wxid, f"麦序文档已设置为：\r{maixudesc}")
        return f"麦序文档已设置为：\r{maixudesc}"
    async def handle_view_group_maixudesc(self, group_wxid: str):
        """处理查看麦序文档命令"""
        maixudesc = await group_repo.get_group_maixu_desc(group_wxid)
        if maixudesc[0]:
            await send_message(group_wxid, f"当前麦序文档：\r{maixudesc[0]}")
        else:
            return "暂无麦序文档"
        
        return f"当前麦序文档：\r{send_line}"
    async def handle_set_host(self, command: str, group_wxid: str):
        """处理设置主持命令"""
        # 解析命令格式：设置主持\r\n0-2aa\r\n2-3bb，截取0-2aa、2-3bb等行
        lines = command.splitlines() #分割换行
        hosts = [line.strip() for line in lines[1:] if line.strip()]
        if not hosts:
            return "命令格式错误，请使用：设置主持\\r\\n0-2aa\\r\\n2-3bb"
        try:
            parsed_slots = parse_time_slots(hosts)
            print(f"解析后的主持时间槽：{parsed_slots}")
        except ValueError as e:
            await send_message(group_wxid, str(e))
            return str(e)
        # 删除旧的主持记录
        await group_repo.delete_group_host(group_wxid)
        
        # 保存到数据库
        if parsed_slots:
            for slot in parsed_slots:
                host_desc, start_hour, end_hour = slot
                await group_repo.add_group_host(group_wxid, start_hour, end_hour, host_desc)
            # hosts_schedules = await group_repo.get_group_hosts(group_wxid)
            # hosts_schedule = '\r'.join([f"{slot[1]}-{slot[2]} {slot[3]}" for slot in hosts_schedules])
            await initialize_tasks.update_groups_tasks(group_wxid, parsed_slots)
            await send_message(group_wxid, f"主持设置成功")
            return f"主持设置成功："
        else:
            return "设置主持内容为空"
        # print(f"设置主持内容：\n{parsed_slots}")
    async def handle_view_host(self, group_wxid: str):
        """处理查看主持命令"""
        hosts_schedules = await group_repo.get_group_hosts(group_wxid)
        if hosts_schedules:
            hosts_schedule = '\r'.join([f"{slot[1]}-{slot[2]} {slot[3]}" for slot in hosts_schedules])
            await send_message(group_wxid, f"当前主持：\r{hosts_schedule}")
            return f"当前主持：\r{hosts_schedule}"
        else:
            return "当前没有设置主持"
    async def handle_set_koupai_start_time(self, command: str, group_wxid: str):
        """设置扣排开始时间"""
        start_time = re.search(r'设置扣排时间(.*)', command)
        if not start_time:
            return "命令格式错误，请使用：设置扣排时间20（分钟）"
        start_time = start_time.group(1).strip()
        if int(start_time) < 0 or int(start_time) > 59:
            return "扣排开始时间必须在0-59分钟之间"
        # 保存到数据库
        await group_repo.update_group_start_koupai(group_wxid, int(start_time))
        await initialize_tasks.update_groups_config(group_wxid, {"start_koupai": int(start_time)})
        await send_message(group_wxid, f"扣排开始时间已设置为：{start_time}分钟")
        return f"扣排开始时间已设置为：{start_time}分钟"
    async def handle_set_koupai_end_time(self, command: str, group_wxid: str):
        """设置扣排截止时间"""
        end_time = re.search(r'设置扣排截止时间(.*)', command)
        if not end_time:
            return "命令格式错误，请使用：设置扣排截止时间20（分钟）"
        end_time = end_time.group(1).strip()
        if int(end_time) < 0 or int(end_time) > 59:
            return "扣排截止时间必须在0-59分钟之间"
        # 保存到数据库
        await group_repo.update_group_end_koupai(group_wxid, int(end_time))
        await group_repo.update_group_end_task(group_wxid, int(end_time))
        await initialize_tasks.update_groups_config(group_wxid, {"end_koupai": int(end_time), "end_renwu": int(end_time)})
        await send_message(group_wxid, f"扣排截止时间已设置为：{end_time}分钟")
        return f"扣排截止时间已设置为：{end_time}分钟"
    async def handle_set_koupai_limit(self, command: str, group_wxid: str):
        """设置扣排人数"""
        limit = re.search(r'设置扣排人数(.*)', command)
        if not limit:
            return "命令格式错误，请使用：设置扣排人数20（人）"
        limit = limit.group(1).strip()
        if int(limit) < 0 or int(limit) > 10:
            return "扣排人数必须在0-20人之间"
        # 保存到数据库
        await group_repo.update_group_limit_koupai(group_wxid, int(limit))
        await initialize_tasks.update_groups_config(group_wxid, {"limit_koupai": int(limit)})
        await send_message(group_wxid, f"扣排人数已设置为：{limit}人")
        return f"扣排人数已设置为：{limit}人"
    async def handle_info_command(self, group_wxid: str):
        """处理信息命令"""
        group_info = await group_repo.get_group_by_wxid(group_wxid)
        if group_info:
            info_text = f"""群组信息：
            群组ID：{group_info[1]}
            群组名称：{group_info[2]}
            创建时间：{group_info[3]}
            """
            return info_text
        else:
            return "未找到群组信息"
    
    async def handle_list_command(self, group_wxid: str):
        """处理列表命令"""
        groups = await group_repo.get_all_groups()
        if groups:
            list_text = "群组列表：\n"
            for group in groups:
                list_text += f"- {group[2]} ({group[1]})\n"
            return list_text
        else:
            return "暂无群组信息"

# 创建命令处理器实例
command_handler = CommandHandler(db_manager)