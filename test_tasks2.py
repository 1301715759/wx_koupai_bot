import datetime
import redis

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


redis_conn = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)

groups_keys = list(redis_conn.smembers("groups_config:koupai_groups"))

# tasks_keys = redis_conn.keys(f"tasks:hosts_tasks:52069341938@chatroom:1")

print(f"groups_keys: {groups_keys} {type(groups_keys)}")
# print(f"tasks_keys: {tasks_keys}")

valid_groups = get_next_hour_group(groups_keys, 0)
print(f"valid_groups: {valid_groups}")

valid_groups = get_next_minute_group(groups_keys, 44)
print(f"valid_groups: {valid_groups}")
