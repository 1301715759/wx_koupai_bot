from fastapi import FastAPI
from contextlib import asynccontextmanager
import json
import asyncio
from command.command_handler import command_handler
from db.database import init_database, db_manager
from db.repository import group_repo
from utils.send_utils import send_message
from celery_tasks.initialize_tasks import initialize_tasks
# from celery_app import celery_app

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )
enable_groups = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 在应用启动时初始化数据库
    await init_database()
    # 初始化任务前清空所有任务（以防意外中断程序导致缓存数据未清空）
    await initialize_tasks.clear_all_tasks()
    # 初始化任务
    await initialize_tasks.load_from_database()
    global enable_groups
    enable_groups = [group[0] for group in await group_repo.get_all_active_groups()]
    print(f"active的群组: {enable_groups}")
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
    print(f"完整数据: {json.dumps(event, ensure_ascii=False, indent=4)}")
    data = event.get("data", {})
    msg_content = data.get("msg", "")
    group_wxid = data.get("fromWxid", "")
    print(f"group_wxid: {group_wxid}")
    # 处理消息事件
    # 文本消息
    if event_type == "recvMsg" and msg_content == "ping":
        print(f"{group_wxid}收到ping，激活群机器人")
        
        await group_repo.create_group(group_wxid, is_active=True)
        await initialize_tasks.load_from_database()
        enable_groups.append(group_wxid)
        await send_message(group_wxid, "pong")
    # 修复：enable_groups 在 startup_event 中是局部变量，需改为全局
    
    
    if event_type == "recvMsg" and  (group_wxid in enable_groups): 
        msg_owner = data.get("finalFromWxid", {})
        print(f"收到消息: {msg_content}")
        if msg_content.startswith(("修改昵称", "设置欢迎词", "设置退群词", "设置麦序文档", 
                                    "设置主持", "查询主持", "查询麦序文档", 
                                    "设置扣排时间", "设置扣排截止时间", "设置扣排人数"
                                    )):
            print(f"收到命令: {msg_content}")
            response = await command_handler.handle_command(msg_content, group_wxid, msg_owner=msg_owner)
            print(f"命令响应: {response}")
    # 群成员进退群事件
    elif event_type == "groupMemberChanges":
        print(f"群成员变化: {msg_content}")
        response = await command_handler.handle_event(data.get("eventType"), group_wxid)
        print(f"事件响应: {response}")
    return {"status": "success"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app='app:app', host="127.0.0.1", port=989, reload=True)