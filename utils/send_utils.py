import aiohttp
import json
import asyncio

async def change_groupname(group_id, new_name):
    """
    改变指定群组的昵称。
    """
    data = {
        "wxid": group_id,
        "nick": new_name
    }
    return await send_request(group_id, "editSelfMemberNick", data)


async def send_request(wxid, request_type, data):   
    """
    发送请求到微信服务器的通用函数。
    """
    url = "http://127.0.0.1:28888/wechat/httpapi"

    payload = json.dumps({
        "type": request_type,
        "data": data
    }, ensure_ascii=False)  # 确保中文字符能够正确传输

    headers = {
        'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
        'Content-Type': 'application/json'
    }

    print(f"Sending {request_type} to {wxid}: {data}")
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=payload.encode('utf-8')) as response:
            response_text = await response.text()
            print(f"Response from server: {response_text}")  # 打印服务器响应
            return response


async def send_message(wxid, msg):
    """发送文本消息"""
    data = {
        "wxid": wxid,
        "msg": msg,
        "compatible": "0"
    }
    return await send_request(wxid, "sendText", data)


async def send_file(wxid, file_path, file_name):
    """发送文件"""
    data = {
        "wxid": wxid,
        "path": file_path,
        "fileName": file_name
    }
    return await send_request(wxid, "sendFile", data)
