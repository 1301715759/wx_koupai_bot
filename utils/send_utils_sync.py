import json
import requests

def change_groupname(group_id, new_name):
    """
    改变指定群组的昵称。
    """
    data = {
        "wxid": group_id,
        "nick": new_name
    }
    return send_request(group_id, "editSelfMemberNick", data)


def send_request(wxid, request_type, data):   
    """
    发送请求到微信服务器的通用函数。
    """
    url = "http://127.0.0.1:8888/wechat/httpapi"

    payload = json.dumps({
        "type": request_type,
        "data": data
    }, ensure_ascii=False)  # 确保中文字符能够正确传输

    headers = {
        'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
        'Content-Type': 'application/json'
    }

    print(f"Sending {request_type} to {wxid}: {data}")
    response = requests.post(url, headers=headers, data=payload.encode('utf-8'))  # 确保以 UTF-8 编码发送
    print(f"Response from server: {response.text}")  # 打印服务器响应


def send_message(wxid, msg):
    """发送文本消息"""
    data = {
        "wxid": wxid,
        "msg": msg,
        "compatible": "0"
    }
    return send_request(wxid, "sendText", data)

def send_file(wxid, file_path, file_name):
    """发送文件"""
    data = {
        "wxid": wxid,
        "path": file_path,
        "fileName": file_name
    }
    return  send_request(wxid, "sendFile", data)
