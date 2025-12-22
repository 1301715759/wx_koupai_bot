import time
import redis
from datetime import datetime, timedelta
def get_next_hour_group(redis_conn, groups_wxid: list, current_hour: int) -> list:
    """检查符合下一个小时扣排任务的群组"""
    valid_groups = []
    task_hour = (current_hour + 1) % 24
    for group_wxid in groups_wxid:
        field = f"tasks:hosts_tasks_config:{group_wxid}:{task_hour}"
        # print(f"field: {field}")
        check_hour = redis_conn.keys(field)
        if not check_hour:
            continue
        valid_groups.append(group_wxid)
    return valid_groups

def get_next_minute_group(redis_conn, groups_wxid: list, current_minute: int, field: str) -> list:
    """检查符合下一个分钟扣排任务的群组"""
    valid_groups = []
    task_minute = (current_minute + 1) % 60
    for group_wxid in groups_wxid:
        key = f"groups_config:{group_wxid}"
        # print(f"field: {key}")
        check_minute = (redis_conn.hget(key, field) == str(task_minute))
        if check_minute:
            valid_groups.append(group_wxid)
    return valid_groups



def add_with_timestamp(redis_conn, group_wxid: str, member_wxid:str, base_score:float = 0, **kwargs):
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
    redis_conn.zadd(f"tasks:launch_tasks:{group_wxid}:{kwargs.get('current_hour', '')}", {member_wxid: score})
    time.sleep(0.0001)
def get_group_config(redis_conn, group_wxid: str) -> dict:
    """获取群组的配置"""
    config = redis_conn.hgetall(f"groups_config:{group_wxid}")
    print(f"group config: {config}")
    
    return config
def get_group_hosts_config(redis_conn, group_wxid: str, current_hour: int) -> dict:
    """获取群组的扣排配置"""
    config = redis_conn.hgetall(f"tasks:hosts_tasks_config:{group_wxid}:{(current_hour+1)%24}")
    print(f"hosts config: {config}")
    return config

def get_group_task_members(redis_conn, group_wxid: str, current_hour: int) -> list:
    """获取群组的扣排麦序信息"""
    members = redis_conn.zrevrange(f"tasks:launch_tasks:{group_wxid}:{(current_hour+1)%24}", 0, -1, withscores=True)
    # [('wxid_dofg3jonqvre22:p', 0.2816195680141449), ('wxid_2tkacjo984zq22:p', 0.28242833766937253)]
    # 将成员id ‘wxid_dofg3jonqvre22:p’ 拆分成 wxid_dofg3jonqvre22 和 p 不需要score
    tasks_members = [(member.split(':')[0], member.split(':')[1]) for member, _ in members]
    
    print(f"tasks_members: {tasks_members}")
    return tasks_members

redis_conn = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
get_group_task_members(redis_conn, "49484317759@chatroom", 23)