from fastapi import FastAPI
from contextlib import asynccontextmanager
import json
import asyncio
from command.command_handler import command_handler
from db.database import init_database, db_manager
from db.repository import group_repo
from utils.send_utils_sync import at_user, get_member_nick
from utils.send_utils import send_message
from celery_tasks.initialize_tasks import initialize_tasks
from celery_tasks.schedule_tasks import add_koupai_member, update_koupai_member, add_mai89_member, add_bb_member, delete_bb_member, add_daizou_member
from celery_tasks.tasks_crud import get_renwu_list
from celery_app import cleanup_expired_results
from cache.redis_pool import get_redis_connection
import re
from command.rules.hostPhrase_rules import parse_at_message
from common.global_vars import *
# from celery_app import celery_app

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )


redis_conn = get_redis_connection(0)
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 在应用启动时初始化数据库
    await init_database()
    # 初始化任务前清空所有任务（以防意外中断程序导致缓存数据未清空）
    await initialize_tasks.clear_all_tasks()
    # 初始化任务
    await initialize_tasks.load_from_database()
    # 清理过期任务结果
    cleanup_expired_results.delay()
    
    # 设置全局变量enable_groups
    set_enable_groups([group[0] for group in await group_repo.get_all_active_groups()])
    # 从数据库中获取所有被禁排成员和对应群 [(group_wxid, member_wxid, is_baned)] 由tuple组成的List类型
    set_baned_list([(baned[0], baned[1]) for baned in await group_repo.get_all_group_member_baned()])
    print(f"active的群组: {get_enable_groups()}")
    print(f"被禁排成员: {get_baned_list()}")
    # 打印所有已注册的命令
    print("已注册的命令:")
    for cmd, info in command_handler.commands.items():
        print(f"  {cmd} - {info['description']}")
    print("=====初始化完成======")
    yield
    # 在应用关闭时清理资源
    await db_manager.close_all_connections()
    await initialize_tasks.clear_all_tasks()
    print("数据库连接已关闭")
# 创建FastAPI应用实例
app = FastAPI(lifespan=lifespan)


@app.post("/wechat/callback")
async def handle_event(event: dict):
    """处理微信回调事件"""
    event_type = event.get("type", 0)
    local_wxid = event.get("wxid", "")
    print(f"完整数据: {json.dumps(event, ensure_ascii=False, indent=4)}")
    data = event.get("data", {})
    msg_content = data.get("msg", "")
    group_wxid = data.get("fromWxid", "")
    print(f"group_wxid: {group_wxid}")
    # 处理消息事件
    # 文本消息
    if local_wxid == data.get("finalFromWxid", ""):
        return
    if event_type == "recvMsg" and msg_content == "ping" and group_wxid not in enable_groups:
        print(f"{group_wxid}收到ping，激活群机器人")
        
        await group_repo.create_group(group_wxid, is_active=True)
        await initialize_tasks.load_from_database(groups_wxid=group_wxid)
        add_enable_group(group_wxid)
        
        await send_message(group_wxid, "pong")
    # 修复：enable_groups 在 startup_event 中是局部变量，需改为全局
    
    
    if event_type == "recvMsg" and  (group_wxid in get_enable_groups()): 
        msg_owner = data.get("finalFromWxid", {})
        at_user = None
        # 当存在@list的时候先尝试解析@list中的内容
        if data.get("atWxidList", "") :
            at_user = data.get("atWxidList", "")
            msg_content = parse_at_message(msg_content)

        print(f"收到消息: {msg_content}")
        if msg_content.startswith(("修改昵称", "设置欢迎词", "设置退群词", "设置麦序文档", 
                                    "设置主持", "查询主持", "查询麦序文档", 
                                    "设置扣排时间", "设置扣排截止时间", "设置任务截止时间", "设置扣排人数",
                                    "设置任务", "取", "补", "当前麦序", "查询麦序", "查询当前麦序", "转麦序", 
                                    "清空固定排", "查询固定排", "添加", "设置手速", "设置任务排",
                                    "设置报备时间", "设置报备人数", "设置固定手速",
                                    "设置报备次数", "设置报备回厅词", "设置报备超时提示词",
                                    "设置麦序作废人数", "本档作废", "上档作废", "换主持", "禁排", "取消禁排",
                                    "今日麦序", "昨日麦序", "累计任务", "累计过")) or "固定排" in msg_content:
            print(f"收到命令: {msg_content}")
            response = await command_handler.handle_command(msg_content, group_wxid, msg_owner=msg_owner, at_user=at_user)
            print(f"命令响应: {response}")
            return
        elif msg_content in ["p", "P", "排"] :
            print(f"收到成员输入p:[{msg_owner}]: {msg_content}")
            member_wxid = at_user[0] if at_user else msg_owner
            if (group_wxid, member_wxid) not in get_baned_list():
                add_koupai_member.delay(group_wxid, member_wxid = member_wxid, msg_content=msg_content,)
            else:
                await send_message(group_wxid, f"{get_member_nick(group_wxid, member_wxid)} 已被禁排")
            return
        elif msg_content.startswith(("bb","BB","Bb", "bB", "报备")):
            print(f"收到成员输入报备: {msg_content}")
            add_bb_member.delay(group_wxid, member_wxid = msg_owner, msg_content=msg_content)
            return
        elif msg_content == "回":
            print(f"收到成员输入回厅词: {msg_content}")
            delete_bb_member.delay(group_wxid, member_wxid = msg_owner)
            return
        elif msg_content in get_renwu_list(redis_conn, group_wxid) or msg_content.startswith(("买8", "买9")):
            print(f"收到成员输入对应任务: {msg_content}")
            member_wxid = at_user[0] if at_user else msg_owner
            if (group_wxid, member_wxid) in get_baned_list():
                await send_message(group_wxid, f"{get_member_nick(group_wxid, member_wxid)} 已被禁排")
                return
            if msg_content.startswith(("买8", "买9")):
                add_mai89_member.delay(group_wxid, member_wxid = member_wxid, msg_content=msg_content)
            else:
                update_koupai_member.delay(group_wxid, member_wxid = member_wxid, msg_content=msg_content)
            return
        elif msg_content == "带走":
            print(f"收到成员输入带走: {msg_content}")
            member_wxid = at_user[0] if at_user else msg_owner
            if (group_wxid, member_wxid) in get_baned_list():
                await send_message(group_wxid, f"{get_member_nick(group_wxid, member_wxid)} 已被禁排")
                return
            add_daizou_member.delay(group_wxid, member_wxid = member_wxid)
            return
            
    # 群成员进退群事件
    elif event_type == "groupMemberChanges":
        print(f"群成员变化: {msg_content}")
        response = await command_handler.handle_event(data.get("eventType"), group_wxid)
        print(f"事件响应: {response}")
    return {"status": "success"}
def add_baned_member(group_wxid: str, member_wxid: str):
    """为全局变量baned_list添加禁排成员，可供其他文件内调用"""
    global baned_list
    baned_list.append((group_wxid, member_wxid))
def remove_baned_member(group_wxid: str, member_wxid: str):
    """从全局变量baned_list移除禁排成员，可供其他文件内调用"""
    global baned_list
    baned_list.remove((group_wxid, member_wxid))
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app='app:app', host="127.0.0.1", port=989, reload=True, reload_excludes=["test_tasks2.py"])

