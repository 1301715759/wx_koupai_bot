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
    task_minute = (current_minute) % 60
    for group_wxid in groups_wxid:
        key = f"groups_config:{group_wxid}"
        # print(f"field: {key}")
        check_minute = (redis_conn.hget(key, field) == str(task_minute))
        print(f"!!!!check_minute: {check_minute}")
        if check_minute:
            valid_groups.append(group_wxid)
    return valid_groups


def add_with_timestamp(redis_conn, group_wxid: str, member_wxid:str, base_score:float = 0, msg_content: str = "", limit_koupai: int = 8,  **kwargs):
    """添加成员到有序集合，分数为当前时间戳。无论如何，不带base_score的分数始终低于带base_score"""
    print(f"进入add_with_timestamp: {kwargs}")
    cureent_time = time.time()
    MAX_TIME = 4102444800  # 2100-01-01 的时间戳
    
    time_score = MAX_TIME - cureent_time
    # 截取整数前四位，即时间咋10000秒内的排序
    minute_part = time_score%10000
    
    score = kwargs.get('extend_score', base_score*100 + minute_part / 10000 )
    # 先尝试移除对应的成员（带p）
    redis_conn.zrem(f"tasks:launch_tasks:{group_wxid}:{kwargs.get('current_hour', '')}", f"{member_wxid}:p")
    # 获取限制人数内的成员，因为会有在范围内重复打榜的可能
    members = redis_conn.zrange(f"tasks:launch_tasks:{group_wxid}:{kwargs.get('current_hour', '')}", 0, limit_koupai-1)
    # 移除 member_wxid:*的成员
    for member in members:
        if member.startswith(f"{member_wxid}:"):
            redis_conn.zrem(f"tasks:launch_tasks:{group_wxid}:{kwargs.get('current_hour', '')}", member)
    
    redis_conn.zadd(f"tasks:launch_tasks:{group_wxid}:{kwargs.get('current_hour', '')}", {f"{member_wxid}:{msg_content}": score})
    if limit_koupai < redis_conn.zcard(f"tasks:launch_tasks:{group_wxid}:{kwargs.get('current_hour', '')}"):
        # 移除分数最低成员（即被挤出去的成员）
        redis_conn.zremrangebyrank(f"tasks:launch_tasks:{group_wxid}:{kwargs.get('current_hour', '')}", 0, 0)
    print(f"add_with_timestamp: {kwargs} 成功")
    # time.sleep(0.0001)
def delete_member(redis_conn, group_wxid: str, member_wxid: str, current_hour: int, limit_koupai: int = 8) -> int:
    """
    删除成员从有序集合
    返回剩余成员数量
    """
    # 先尝试获得所有成员
    print(f"delete_member: [{member_wxid}]")
    members = redis_conn.zrange(f"tasks:launch_tasks:{group_wxid}:{current_hour}", 0, limit_koupai-1)
    # 删除所有包含 member_wxid 的成员
    for member in members:
        if member.startswith(f"{member_wxid}:"):
            redis_conn.zrem(f"tasks:launch_tasks:{group_wxid}:{current_hour}", member)
    # 返回空余成员数量
    return limit_koupai - redis_conn.zcard(f"tasks:launch_tasks:{group_wxid}:{current_hour}")
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


def get_group_task_members(redis_conn, group_wxid: str, current_hour: int, limit:int = 8) -> list:
    """获取群组的扣排麦序信息"""
    members = redis_conn.zrevrange(f"tasks:launch_tasks:{group_wxid}:{(current_hour+1)%24}", 0, limit-1, withscores=True)
    # [('wxid_dofg3jonqvre22:p', 0.2816195680141449), ('wxid_2tkacjo984zq22:p', 0.28242833766937253)]
    # 将成员id ‘wxid_dofg3jonqvre22:p’ 拆分成 wxid_dofg3jonqvre22 和 p 需要score
    tasks_members = [(member.split(':')[0], member.split(':')[1], score) for member, score in members]
    
    print(f"tasks_members: {tasks_members}")
    return tasks_members

def get_renwu_list(redis_conn, group_wxid: str) -> list:
    """获取群组的任务列表"""
    rules = redis_conn.hget(f"groups_config:{group_wxid}", "renwu_desc")
    # 返回指定字段 "0.3<0.5<1.0 .....100.0<新人置顶<魅力置顶"
    # 去除<号，生成对应列表 
    try:
        rule_items = [item.strip() for item in rules.strip().split('<') if item.strip()]
    except:
        rule_items = []
    return rule_items



def get_renwu_dict(renwu_list: list) -> dict:
    """获取群组的任务规则"""
    # 返回指定字段 "0.3<0.5<1.0 .....100.0<新人置顶<魅力置顶"
    # 去除<号，生成对应字典 
    # {"0.3": 1, "0.5": 2, "1.0": 3,...."100.0": 100, "新人置顶": 101, "魅力置顶": 102}
    # 当自定义规则（新人置顶、魅力置顶，即非浮点数，这里的浮点数指的是 由[数字.数字]构成的字符）在明确数字区间内时，则在区间中间值插入指定值。
    # 我们规定权值为从1开始的整数，递增值为1
    rule_dict = {}
    for index, item in enumerate(renwu_list, 1):
        rule_dict[item] = index 
    # print(f"rules: {rule_list}")
    return rule_dict

redis_conn = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
# get_group_task_members(redis_conn, "49484317759@chatroom", 23)
# get_renwu_rule(redis_conn, "42973360766@chatroom")
# print(get_renwu_list(redis_conn, "42973360766@chatroom"))
# print(get_renwu_dict(get_renwu_list(redis_conn, "42973360766@chatroom")))
