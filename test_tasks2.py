import requests
import aiohttp
import datetime
import time
import redis
import re
import asyncio
import platform
from db.repository import group_repo
from celery_tasks.schedule_tasks import (send_task_schedule, scheduled_task, 
                                        delete_koupai_members, send_task_schedule_day,
                                        save_task_schedule_day_history)
now = datetime.datetime.now()
current_minute = now.minute
current_hour = now.hour


def get_next_hour_group(groups_wxid: list, current_hour: int) -> list:
    """检查指定群组的开牌任务是否在当前时间"""
    valid_groups = []
    task_hour = current_hour + 1
    for group_wxid in groups_wxid:
        field = f"tasks:hosts_tasks:{group_wxid}:{task_hour}"
        print(f"field: {field}")
        check_hour = redis_conn.keys(field)
        if not check_hour:
            continue
        valid_groups.append(group_wxid)

    return valid_groups
    
def get_next_minute_group(groups_wxid: list, current_minute: int) -> list:
    """检查指定群组的开牌任务是否在当前时间"""
    valid_groups = []
    task_minute = current_minute + 1
    for group_wxid in groups_wxid:
        field = f"groups_config:{group_wxid}"
        print(f"field: {field}")
        check_minute = (redis_conn.hget(field, "start_koupai") == str(task_minute))
        if not check_minute:
            continue
        valid_groups.append(group_wxid)

    return valid_groups

def add_with_timestamp(redis_conn, member_wxid:str, base_score:float = 0):
    """添加成员到有序集合，分数为当前时间戳。无论如何，不带base_score的分数始终低于带base_score"""
    cureent_time = time.time()
    MAX_TIME = 4102444800  # 2100-01-01 的时间戳
    
    time_score = MAX_TIME - cureent_time
    print(f"time_score: {time_score}")
    # 截取整数前四位，即时间咋10000秒内的排序
    minute_part = time_score%10000
    print(f"minute_part: {minute_part}")
    
    score = base_score*100 + minute_part / 10000
    print(f"score: {score}")
    redis_conn.zadd("tasks:launch_tasks:49484317759@chatroom:19", {member_wxid: score})
    time.sleep(0.0001)
redis_conn = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)

# groups_keys = list(redis_conn.smembers("groups_config:koupai_groups"))

# # tasks_keys = redis_conn.keys(f"tasks:hosts_tasks:52069341938@chatroom:1")

# print(f"groups_keys: {groups_keys} {type(groups_keys)}")
# # print(f"tasks_keys: {tasks_keys}")

# valid_groups = get_next_hour_group(groups_keys, 0)
# print(f"valid_groups: {valid_groups}")

# valid_groups = get_next_minute_group(groups_keys, 44)
# print(f"valid_groups: {valid_groups}")

# redis_conn.set("tasks:launch_tasks:49484317759@chatroom:19","")

'''

redis_conn.delete("tasks:launch_tasks:49484317759@chatroom:19")

add_with_timestamp("a")

add_with_timestamp("b")

add_with_timestamp("c")
add_with_timestamp("d", 0.1)
add_with_timestamp("e", 0.1)
add_with_timestamp("f")
add_with_timestamp("g")
add_with_timestamp("同分不被挤出去")

count = redis_conn.zcard("tasks:launch_tasks:49484317759@chatroom:19")
print(f"count: {count}")
members = redis_conn.zrevrange("tasks:launch_tasks:49484317759@chatroom:19", 0, 7, withscores=True)
print(f"members: {list(members)}")
#添加新成员后告知谁被挤出去了
'''

# redis_conn.sadd(f"tasks:launch_tasks:tasks_list", "49484317759@chatroom:19")
# redis_conn.srem(f"tasks:launch_tasks:renwu_tasks_list", "52069341938@chatroom:1")

# command = "设置手速不可"
# match = re.search(r'设置手速不可取(.*)', command)
# if match:
#     print(f"match: {match}")
# redis_conn.zadd("tasks:launch_tasks:42973360766@chatroom:15", {"wxid_dofg3jonqvre22:10.0": 13})
# send_task_schedule.delay("42973360766@chatroom", 17)
current_minute = datetime.datetime.now().minute
scheduled_task.delay(update_group="42973360766@chatroom", koupai_type="all", schedule_minute=current_minute)
# delete_koupai_members.delay("42973360766@chatroom", 16, 1)
# send_task_schedule_day.delay("42973360766@chatroom", date="2025-12-31")

# 模拟request post同时异步发送了十条不同成员发的post请求，每个请求的参数不同
"""
{
    "type": "recvMsg",
    "des": "收到消息",
    "data": {
        "timeStamp": "1767119446100",
        "fromType": 2,
        "msgType": 1,
        "msgSource": 0,
        "fromWxid": "42973360766@chatroom",
        "finalFromWxid": "wxid_2tkacjo984zq22",
        "atWxidList": [],
        "silence": 1,
        "membercount": 3,
        "signature": "N0_V1_sMM6sefm|v1_6pSJrYFv",
        "msg": "p",
        "msgId": "8274637004044835273",
        "sendId": "",
        "msgXml": ""
    },
    "timestamp": "1767119447052",
    "wxid": "wxid_dofg3jonqvre22",
    "port": 8888,
    "pid": 3940,
    "flag": "7888"
}

"""
async def post_data_async(url, data):
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=data) as response:
            return await response.json()
    # 端口为989， /wechat/callback 接收post请求
# Windows 下需要显式设置 SelectorEventLoop，否则 aiodns 会报错
if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 并发发送 10 条请求
async def send_all():
    tasks = []
    for i in range(10):
        member_wxid = f"wxid_{i}"
        await asyncio.sleep(0.01*i)
        print(f"member_wxid: {member_wxid}")
        data = {
            "type": "recvMsg",
            "des": "收到消息",
            "data": {
                "timeStamp": "1767119446100",
                "fromType": 2,
                "msgType": 1,
                "msgSource": 0,
                "fromWxid": "42973360766@chatroom",
                "finalFromWxid": member_wxid,
                "atWxidList": [],
                "silence": 1,
                "membercount": 3,
                "signature": "N0_V1_sMM6sefm|v1_6pSJrYFv",
                "msg": "p",
                "msgId": "8274637004044835273",
                "sendId": "",
                "msgXml": ""
            },
            "timestamp": "1767119447052",
            "wxid": "wxid_dofg3jonqvre22",
            "port": 8888,
            "pid": 3940,
            "flag": "7888"
        }
        tasks.append(post_data_async("http://localhost:989/wechat/callback", data))
    await asyncio.gather(*tasks)

# 运行协程
# asyncio.run(send_all())

# date = datetime.datetime.now().strftime("%Y:%m:%d")
# group_keys = redis_conn.smembers("groups_config:koupai_groups")
# # print(f"info: {redis_conn.info()}")
# for group_key in group_keys:
#     print(f"group_key: {group_key}")
#     # 复制tasks:launch_tasks:{group_wxid}:*到history:tasks:{group_wxid}:{date}下
#     # redis是扁平存储，因此不能一次性复制所有key，需要遍历所有key
#     for key in redis_conn.scan_iter(f"tasks:launch_tasks:{group_key}:*"):
#         redis_conn.copy(key, f"history:tasks:{group_key}:{date}:{key.split(':')[-1]}", replace=True)
# scheduled_task.delay(save_history = True)

