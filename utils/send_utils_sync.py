import json
import requests
from utils.emoji_map import emoji_map
def generate_custom_msg_content(**kwargs) -> str:
    """
    生成自定义消息内容
    """
    #为kwargs生成字典
    msg_dic = {}
    for key, value in kwargs.items():
        msg_dic[key] = value
    return msg_dic

def get_member_nick(group_wxid: str, member_wxid: str) -> str:
    """
    获取指定用户的昵称。
    """
    data = {
        "wxid": group_wxid,
        "objWxid": member_wxid,
    }
    response = send_request(group_wxid, "getMemberNick", data)
    # print(f"@@@response: {response}")
    group_nickid = response.get("result", {}).get("groupNick", "")
    

    if group_nickid:
        return group_nickid
    return ""

def at_user(wxid: str, trueAt: bool = True) -> str:
    """
    生成@用户的字符串。
    """
    at_msg = f"[@,wxid={wxid},nick=,isAuto={'true' if trueAt else 'false'}]"
    print(f"生成的@用户字符串: {at_msg}")
    return at_msg

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
    params:
        wxid: 接收请求的微信ID
        request_type: 请求类型，例如 "sendText"
        data: 请求数据，包含必要的参数
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
    response = requests.post(url, headers=headers, data=payload.encode('utf-8'))  # 确保以 UTF-8 编码发送
    print(f"Response from server: {response.text}")  # 打印服务器响应
    return response.json()

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
