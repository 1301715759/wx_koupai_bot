import asyncio
import re
from db.repository import group_repo, command_repo
from db.database import db_manager
from utils.send_utils import send_message, change_groupname
from command.rules.hostPhrase_rules import validate_time_slots_array, parse_time_slots
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
            "设置主持": {
                "description": "设置主持，格式\n"
                                "设置主持\n"
                                "0-1aa\n"
                                "0-2bb\n",
                "handler": self.handle_set_host
            }
        }
    
    async def handle_command(self, command: str, group_wxid: str):
        """处理用户命令"""
        # 先查询数据库中是否存在该群，不存在则创建
        group_info = await group_repo.get_group_by_wxid(group_wxid)
        if not group_info:
            # 创建默认群组数据
            await group_repo.create_group(group_wxid, "", "")
        
        # 根据命令类型进行处理
        # 根据命令类型进行处理
        if command.startswith("修改昵称"):
            return await self.handle_change_group_name(command, group_wxid)
        elif command.startswith("设置欢迎词"):
            return await self.handle_set_welcome(command, group_wxid)
        elif command.startswith("设置退群词"):
            return await self.handle_set_leave(command, group_wxid)
        elif command.startswith("设置主持"):
            return await self.handle_set_host(command, group_wxid)
        else:
            return "未注册命令，请输入 /help 查看帮助信息"
    
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
    async def handle_set_host(self, command: str, group_wxid: str):
        """处理设置主持命令"""

        # 解析命令格式：设置主持\r\n0-2aa\r\n2-3bb，截取0-2aa、2-3bb等行
        lines = command.splitlines() #分割换行
        hosts = [line.strip() for line in lines[1:] if line.strip()]
        if not hosts:
            return "命令格式错误，请使用：设置主持\\r\\n0-2aa\\r\\n2-3bb"
        try:
            parsed_slots = parse_time_slots(hosts)
        except ValueError as e:
            await send_message(group_wxid, str(e))
            return str(e)
        print(f"设置主持内容：\n{parsed_slots}")

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