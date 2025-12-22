import datetime
import time
import redis
import re

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

at_text = "@\\u2764\\uFE0F\\u2005p"
#直接分割，因为我们不需要\u2005前面的内容
at_user_id, msg_content = at_text.split("\\u2005", 1)
print("parts:", at_text.split("\\u2005", 1))
print(f"at_user_id: {at_user_id}")
print(f"msg_content: {msg_content.strip()}")
