import asyncio
import re
from db.repository import group_repo, command_repo
from db.database import db_manager
from utils.send_utils import send_message, change_groupname
from command.rules.hostPhrase_rules import validate_time_slots_array, parse_time_slots, parse_at_message
import json
from celery_tasks.initialize_tasks import initialize_tasks
from celery_tasks.schedule_tasks import scheduled_task, delete_koupai_member, add_koupai_member, get_current_maixu, transfer_koupai_member
from datetime import datetime
from cache.redis_pool import get_redis_connection
class CommandHandler:
    """命令处理器，处理各种用户命令"""

    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.commands = {}
        self.register_commands()
        self.redis_conn = get_redis_connection(0)
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
            },
            "设置任务": {
                "description": "设置任务描述",
                "handler": self.handle_set_renwu
            },
            "取": {
                "description": "取排",
                "handler": self.handle_remove_member
            },
            "补": {
                "description": "补排",
                "handler": self.handle_re_member
            },
            "当前麦序/查询麦序/查询当前麦序": {
                "description": "查询当前麦序",
                "handler": self.handle_view_current_maixu
            },
            "转麦序": {
                "description": "转麦序+@用户",
                "handler": self.handle_transfer_koupai
            },
            "固定排": {
                "description": "数字-数字固定排+@多个用户（可选，没有at的时候默认为自己），\n"
                                "例如：\n"
                                "0-2固定排@某人"
                                ,
                "handler": self.handle_set_fixed_koupai
            },
            "清空固定排": {
                "description": "清空固定排+@用户 同固定排",
                "handler": self.handle_remove_fixed_koupai
            },
        }
    
    async def handle_command(self, command: str, group_wxid: str, **kwargs):
        """处理用户命令"""
        # 如果包含艾特消息
        # if kwargs.get("at_user"):
        #     command = parse_at_message(command)
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
        elif command.startswith("设置任务"):
            return await self.handle_set_renwu(command, group_wxid)
        elif command.startswith("查看任务"):
            return await self.handle_view_renwu(group_wxid)
        elif command.startswith("当前麦序") or command.startswith("查询麦序") or command.startswith("查询当前麦序"):
            return await self.handle_view_current_maixu(group_wxid)
        elif command == "取":
            return await self.handle_remove_member(group_wxid, kwargs.get("msg_owner"), kwargs.get("at_user"))
        elif command == "补":
            return await self.handle_re_member(group_wxid, kwargs.get("msg_owner"), kwargs.get("at_user"))
        elif command.startswith("转麦序"):
            return await self.handle_transfer_koupai(command, group_wxid, kwargs.get("msg_owner"), kwargs.get("at_user"))
        elif command.startswith("清空固定排"):
            return await self.handle_remove_fixed_koupai(command, group_wxid, kwargs.get("msg_owner"), kwargs.get("at_user"))
        elif "固定排" in command:
            return await self.handle_set_fixed_koupai(command, group_wxid, kwargs.get("msg_owner"), kwargs.get("at_user"))
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
                host_desc, lianpai_desc, start_hour, end_hour = slot
                await group_repo.add_group_host(group_wxid, start_hour, end_hour, host_desc, lianpai_desc)
            # hosts_schedules = await group_repo.get_group_hosts(group_wxid)
            # hosts_schedule = '\r'.join([f"{slot[1]}-{slot[2]} {slot[3]}" for slot in hosts_schedules])
            await initialize_tasks.add_koupai_groups(group_wxid)
            await initialize_tasks.update_groups_tasks(group_wxid, parsed_slots)
            # 立即执行一次扣排任务查询
            scheduled_task.delay()
            await send_message(group_wxid, f"主持设置成功")
            return f"主持设置成功："
        else:
            return "设置主持内容为空"
        # print(f"设置主持内容：\n{parsed_slots}")
    async def handle_set_fixed_koupai(self, command: str, group_wxid: str, msg_owner: str, at_user: list = []):
        """处理固定排"""
        try:
            # 如果不是以固定排开始或者结尾，说明格式错误
            if not command.startswith("固定排") and not command.endswith("固定排"):
                return "不是固定排命令"
            print(f"设置固定排命令：{command}")
            # 存在at_user，说明是固定操作，指向的用户应当为at_user
            if at_user:
                msg_owner = at_user
                print(f"固定排命令中的用户：{msg_owner}")
            else:
                msg_owner = [msg_owner]
            # 当command不以 数字1-数字2固定排 并且数字1 < 数字2 格式时说明格式错误。比如 0-2固定排  为正确格式
            if not re.match(r'^(\d{1,2})-(\d{1,2})$', command):
                #格式正确时候分割数字1和数字2
                print(f"固定排命令中的数字：{command.split('固定排')[0]}")
                fixed_koupai_range = command.split("固定排")[0]
                fixed_koupai_range = fixed_koupai_range.split("-")
                fixed_koupai_range = [int(x) for x in fixed_koupai_range]
                if fixed_koupai_range[0] >= fixed_koupai_range[1] or fixed_koupai_range[0] < 0 or fixed_koupai_range[1] > 24:
                    return "命令格式错误，请使用：设置固定排 数字1-数字2固定排  数字1 < 数字2"
                #从第一位开始累加
                time_range = list(range(fixed_koupai_range[0], fixed_koupai_range[1]))
                for start_time in time_range:
                    for fixed_wxid in msg_owner:
                        await group_repo.add_group_host_with_fixed_wxid(group_wxid, start_time, start_time+1, fixed_wxid)
                # 更新群组的fixed_hosts
                await initialize_tasks.update_group_tasks_fixed_hosts(group_wxid)
                await send_message(group_wxid, f"已固定排成功")

        except Exception as e:
            # logger.error(f"固定排成员时出错: {e}")
            return f"固定排成员 {msg_owner} 时出错 {e}"
        return f"成员 {msg_owner} 已固定排至 {at_user}"
    async def handle_remove_fixed_koupai(self, command: str, group_wxid: str, msg_owner: str, at_user: list = []):
        """处理清空固定排"""
        try:
            # 如果不是以固定排开始或者结尾，说明格式错误
            if not command.startswith("清空固定排") and not command.endswith("清空固定排"):
                return "不是清空固定排命令"
            print(f"清空固定排命令：{command}")
            # 存在at_user，说明是固定操作，指向的用户应当为at_user
            if at_user:
                msg_owner = at_user
                print(f"清空固定排命令中的用户：{msg_owner}")
            else:
                msg_owner = [msg_owner]
            # 从数据库中删除固定排成员
            for user in msg_owner:
                await group_repo.delete_group_fixed_host(group_wxid, user)
            # 更新群组的fixed_hosts
            await initialize_tasks.update_group_tasks_fixed_hosts(group_wxid)
            await send_message(group_wxid, f"已清空固定排成功")
        except Exception as e:
            # logger.error(f"清空固定排成员时出错: {e}")
            return f"清空固定排成员 {msg_owner} 时出错 {e}"
    async def handle_view_host(self, group_wxid: str):
        """处理查看主持命令"""
        hosts_schedules = await group_repo.get_group_hosts(group_wxid)
        if hosts_schedules:
            hosts_schedule = '\r'.join([f"{slot[2]}-{slot[3]} {slot[4]}" for slot in hosts_schedules])
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
        # 立即执行一次扣排任务查询，如果当前秒钟为0，则等待1秒（不等待会出现诡异的情况）
        # if datetime.now().second == 0:
        #     await asyncio.sleep(1)
        scheduled_task.delay(koupai_type="start", update_group=group_wxid)
        await send_message(group_wxid, f"扣排开始时间已设置为：{start_time}分钟")
        return f"扣排开始时间已设置为：{start_time}分钟"
    async def handle_set_koupai_end_time(self, command: str, group_wxid: str):
        """设置扣排截止时间"""
        end_time = re.search(r'设置扣排截止时间(.*)', command)
        if not end_time:
            return "命令格式错误，请使用：设置扣排截止时间20（分钟）"
        end_time = end_time.group(1).strip()
        
        if int(end_time) < 0 or int(end_time) > 60 :
            return "扣排截止时间必须在0-59分钟之间"
        # 保存到数据库
        await group_repo.update_group_end_koupai(group_wxid, int(end_time))
        await group_repo.update_group_end_task(group_wxid, int(end_time))
        await initialize_tasks.update_groups_config(group_wxid, {"end_koupai": int(end_time), "end_renwu": int(end_time)})
        # 立即执行一次扣排任务查询，如果当前秒钟为0，则等待3秒（不等待会出现诡异的情况）
        if datetime.now().second == 0:
            await asyncio.sleep(3)
        scheduled_task.delay(koupai_type="end", update_group=group_wxid)
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
    async def handle_set_renwu(self, command: str, group_wxid: str):
        """设置任务描述"""
        if re.search(r'设置任务成功(.*)', command, re.S):
            # 防止自我调用
            return 
        renwu_desc = re.search(r'设置任务(.*)', command, re.S)
        if not renwu_desc:
            return "命令格式错误，请使用：设置任务描述 任务描述"
        # 0.3<0.5<1.0<互动排<1.5 这样的类型
        renwu_desc = renwu_desc.group(1).strip()
        # 保存到数据库
        await group_repo.update_group_renwu_desc(group_wxid, renwu_desc)
        await initialize_tasks.update_groups_config(group_wxid, {"renwu_desc": renwu_desc})
        await send_message(group_wxid, f"设置任务成功：\r\n{renwu_desc}")
        return f"任务描述已设置为：{renwu_desc}"
    async def handle_view_current_maixu(self, group_wxid: str):
        """查询当前麦序"""
        get_current_maixu.delay(group_wxid)
        # if current_maixu:
        #     return f"当前麦序：{current_maixu}"
        # else:
        #     return "当前没有设置麦序"
    async def handle_remove_member(self, group_wxid: str, msg_owner: str, at_user: str = ""):
        """处理移除麦序成员"""
        try:
            # 存在at_user，说明是替取操作，指向的用户应当为at_user
            if at_user:
                msg_owner = at_user[0]
            delete_koupai_member.delay(group_wxid, msg_owner)
        except Exception as e:
            logger.error(f"移除成员时出错: {e}")
            return f"移除成员 {msg_owner} 时出错"


        return f"成员 {msg_owner} 已从群组中移除"
    async def handle_re_member(self, group_wxid: str, msg_owner: str, at_user: list = []):
        """处理补排成员"""
        try:
            # 存在at_user，说明是替补操作，指向的用户应当为at_user
            if at_user:
                msg_owner = at_user[0]
            add_koupai_member.delay(group_wxid, msg_owner,"补")
        except Exception as e:
            logger.error(f"补排成员时出错: {e}")
            return f"补排成员 {msg_owner} 时出错"
        return f"成员 {msg_owner} 已补排"
    async def handle_transfer_koupai(self, command: str, group_wxid: str, msg_owner: str, at_user: list):
        """处理转麦序"""
        transfer_koupai_member.delay(group_wxid, msg_owner, at_user[0], "转")
        return f"成员 {msg_owner} 已转至 {at_user[0]}"

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