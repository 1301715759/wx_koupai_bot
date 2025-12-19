from fastapi import FastAPI
import json
import asyncio
from command.command_handler import command_handler
from db.database import init_database

app = FastAPI()
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# 在应用启动时初始化数据库和注册命令
@app.on_event("startup")
async def startup_event():
    """应用启动时初始化数据库和注册命令"""
    await init_database()
    print("数据库初始化完成")
    print("命令注册完成")
    # 打印所有已注册的命令
    print("已注册的命令:")
    for cmd, info in command_handler.commands.items():
        print(f"  {cmd} - {info['description']}")

@app.post("/wechat/callback")
async def handle_event(event: dict):
    """处理微信回调事件"""
    event_type = event.get("type", 0)
    print(f"事件类型: {event_type}")
    print(f"完整数据: {json.dumps(event, ensure_ascii=False, indent=4)}")
    data = event.get("data", {})
    msg_content = data.get("msg", "")
    # 处理消息事件
    if event_type == "recvMsg":  # 文本消息

        print(f"收到消息: {msg_content}")
        if msg_content.startswith(("修改昵称", "设置欢迎词", "设置退群词", "设置主持")) and data.get("fromWxid"):
            print(f"收到命令: {msg_content}")
            response = await command_handler.handle_command(msg_content, data.get("fromWxid"))
            print(f"命令响应: {response}")
    elif event_type == "groupMemberChanges":
        print(f"群成员变化: {msg_content}")
    return {"status": "success"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app='app:app', host="127.0.0.1", port=989, reload=True)