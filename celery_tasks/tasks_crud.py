import time
import redis
from datetime import datetime, timedelta
from db.repository import group_repo
import asyncio
def get_next_hour_group(redis_conn, groups_wxid: list, current_hour: int) -> list:
    """检查符合下一个小时扣排任务的群组"""
    valid_groups = []
    task_hour = (current_hour + 1) % 24
    for group_wxid in groups_wxid:
        # 先检测stage是否为start ，如果不是则跳过
        # 不论是发送截止还是扣排任务，都需要检测stage是否为start
        if redis_conn.hget(f"tasks:hosts_tasks_config:{group_wxid}:{task_hour}", "stage") != "start":
            print(f"群组 {group_wxid} 下一个小时 {task_hour} 不是扣牌时间")
            continue
        field = f"tasks:hosts_tasks_config:{group_wxid}:{task_hour}"
        # print(f"field: {field}")
        check_hour = redis_conn.scan_iter(field)
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
        print(f"群组 {group_wxid} 下一个分钟 {task_minute} 检查结果 {check_minute}")
        # print(f"!!!!check_minute: {check_minute}")
        if check_minute:
            valid_groups.append(group_wxid)
    return valid_groups
def get_next_schedule_group(redis_conn, groups_wxid:list, current_hour:int) -> list:
    """检查符合发送打卡记录表的群组"""
    valid_groups = []
    for group_wxid in groups_wxid:
        last_hour = (current_hour - 1) % 24
        key_last_hour = f"tasks:hosts_tasks_config:{group_wxid}:{last_hour}"
        end_schedule = redis_conn.hget(key_last_hour, "end_schedule")
        # 如果从配置中读取到end_schedule为当前小时，则说明上场次结束，需要发送上场打卡记录表
        if end_schedule:
            if int(end_schedule) % 24 == current_hour:
                valid_groups.append(group_wxid)
    print(f"符合发送打卡记录表的群组: {valid_groups}")
    return valid_groups
def add_with_timestamp(redis_conn, group_wxid: str, member_wxid:str, base_score:float = 0, msg_content: str = "", limit_koupai: int = 8, mai_type:str = "", **kwargs) -> str:
    """添加成员到有序集合，分数为当前时间戳。无论如何，不带base_score的分数始终低于带base_score，返回被挤出去的成员"""
    print(f"进入add_with_timestamp: {kwargs}")
    cureent_time = time.time()
    MAX_TIME = 4102444800  # 2100-01-01 的时间戳
    
    time_score = MAX_TIME - cureent_time
    # 截取整数前四位，即时间10000秒内的排序
    minute_part = time_score%10000
    
    score = kwargs.get('extend_score', base_score + minute_part / 10000 )
    # 先尝试移除对应的成员（带p）
    redis_conn.zrem(f"tasks:launch_tasks:{group_wxid}:{kwargs.get('current_hour', '')}", f"{member_wxid}:p")
    # 获取限制人数内的成员，因为会有在范围内重复打榜的可能
    members = redis_conn.zrange(f"tasks:launch_tasks:{group_wxid}:{kwargs.get('current_hour', '')}", 0, limit_koupai-1)
    # 移除 member_wxid:*的成员
    for member in members:
        if member.startswith(f"{member_wxid}:"):
            redis_conn.zrem(f"tasks:launch_tasks:{group_wxid}:{kwargs.get('current_hour', '')}", member)
    
    redis_conn.zadd(f"tasks:launch_tasks:{group_wxid}:{kwargs.get('current_hour', '')}", {f"{member_wxid}:{msg_content}": score})
    # 当限制人数小于正分成员人数的时候，需要移除分数最低的正分成员（即被挤出去的成员）
    # 当 mai_type 为空时，需要移除分数最低的正分成员（即被挤出去的成员）
    if not mai_type:
        
        if limit_koupai < redis_conn.zcount(f"tasks:launch_tasks:{group_wxid}:{kwargs.get('current_hour', '')}", 0, float('inf')):
            
            positive_min_member = redis_conn.zrangebyscore(f"tasks:launch_tasks:{group_wxid}:{kwargs.get('current_hour', '')}", 0, float('inf'), start=0, num=1)
            print(f"negative_min_member: {negative_min_member}")
            # 移除分数最低的正分成员（即被挤出去的成员）
            if positive_min_member:
                redis_conn.zrem(f"tasks:launch_tasks:{group_wxid}:{kwargs.get('current_hour', '')}", positive_min_member[0])
                # 返回被挤出去的成员
                print(f"被挤出去的正分成员: {positive_min_member[0].split(':')[0]}")
                return positive_min_member[0].split(":")[0]
    
    if mai_type == "p8 " or mai_type == "p9 ":
        # 当负分范围在-200~0 的成员数量 > 1时，移除分数最小的负分成员
        # mai8 负分范围在-200~0
        # mai9 负分范围在 -1000~-500
        print(f"mai_type: {mai_type}")
        min_score = -200 if mai_type == "mai8" else -1000
        max_score = 0 if mai_type == "mai8" else -500
        if redis_conn.zcount(f"tasks:launch_tasks:{group_wxid}:{kwargs.get('current_hour', '')}", min_score, max_score) > 1:
            negative_min_member = redis_conn.zrangebyscore(f"tasks:launch_tasks:{group_wxid}:{kwargs.get('current_hour', '')}", min=min_score, max=max_score, start=0, num=1)
            print(f"negative_min_member: {negative_min_member}")

            # 移除分数最小的负分成员（即被挤出去的成员）
            if negative_min_member:
                redis_conn.zrem(f"tasks:launch_tasks:{group_wxid}:{kwargs.get('current_hour', '')}", negative_min_member[0])
                # 返回被挤出去的成员
                print(f"被挤出去的负分成员: {negative_min_member[0].split(':')[0]}")
                return negative_min_member[0].split(":")[0]
    return ""

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
    # 返回空余正分成员数量
    return limit_koupai - redis_conn.zcount(f"tasks:launch_tasks:{group_wxid}:{current_hour}", 0, float('inf'))
def delete_members(redis_conn, group_wxid: str, current_hour: int, count: int = 1):
    """
    删除多个成员从有序集合(从最分数为-1000以上的开始删除，从小到大)
    将删除的成员member后缀改为:作废
    返回剩余成员数量
    """
    min_members = redis_conn.zrangebyscore(f"tasks:launch_tasks:{group_wxid}:{current_hour}", min=-1000, max=float('inf'), start=0, num=count, withscores=True)
   
    for member, score in min_members:
         # 将删除的成员member后缀拼接上:作废
        redis_conn.zrem(f"tasks:launch_tasks:{group_wxid}:{current_hour}", member)
        redis_conn.zadd(f"tasks:launch_tasks:{group_wxid}:{current_hour}", {f"{member}:作废": score})
    
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

def get_group_hosts_all(redis_conn, group_wxid: str) -> list:
    """获取群组的所有扣排配置"""
    hosts_all = []
    # 先尝试获取该群所有key
    for key in redis_conn.scan_iter(f"tasks:hosts_tasks_config:{group_wxid}:*"):
        # 拼接start_hour,host_desc
        start_hour = redis_conn.hget(key, "start_hour")
        host_desc = redis_conn.hget(key, "host_desc")
        hosts_all.append({"start_hour": start_hour, "host_desc": host_desc})
    # 按start_hour排序
    hosts_all.sort(key=lambda x: int(x["start_hour"]))
    return hosts_all

def get_group_task_members(redis_conn, group_wxid: str, current_hour: int, end_hour: int = None, with_daizou: bool = False, date: str = None) -> list:
    """
    获取群组的扣排麦序信息
    with_daizou: 是否包含带走的成员
    返回: 群组的扣排麦序信息列表，每个元素为 (成员id, 任务类型, 分数, 状态)
    """
    # 我们将带走的数值设置为-1000以下
    min_score = -2000 if with_daizou else -1000
    max_score = float('inf')
    members = []
    
    key = f"tasks:launch_tasks:{group_wxid}"
    if date not in ["", None]:
        key = f"history:tasks:{group_wxid}:{date}"
    print(f"key: {key}")
    tasks_members = []
    for hour in range((current_hour+1)%24, end_hour if end_hour != None else (current_hour+1)%24 + 1):
        print(f"hour: {hour}")
        tasks = redis_conn.zrevrangebyscore(f"{key}:{hour}", min=min_score, max=max_score, withscores=True)
        # [('wxid_dofg3jonqvre22:p', 0.2816195680141449), ('wxid_2tkacjo984zq22:p', 0.28242833766937253)]
        # 将成员id wxid_dofg3jonqvre22:p:带走 (wxid_dofg3jonqvre22, p, score, state, hour)
        #         wxid_dofg3jonqvre22:p      (wxid_dofg3jonqvre22, p, score, '', hour)
        tasks_members.extend(
                (
                    member.split(':')[0],
                    member.split(':')[1], 
                    score,
                    member.split(':')[2] if len(member.split(':')) > 2 else '',
                    hour,
                    date,
                    group_wxid
                ) for member, score in tasks
            )
    
    return tasks_members
def generate_task_members(group_tasks_members: list, with_zuofei: bool = False) -> dict:
    """
    生成群组的扣排麦序信息字典
    with_zuofei: 是否包含作废场次的信息
    返回: 群组的扣排麦序信息字典，例子：{成员id: [扣排列表]}
    """
    # 生成字典，key为成员id，value为扣排列表
    tasks_members_dict = {}
    for member, koupai_type, score, state, _, _, _ in group_tasks_members:
        # 排除作废场次的信息
        if not with_zuofei and state == "作废":
            continue
        if member not in tasks_members_dict:
            tasks_members_dict[member] = []
        
        tasks_members_dict[member].append((koupai_type, score, state, ))
    
    return tasks_members_dict
def get_member_task(redis_conn, group_wxid: str) -> dict:
    """获取群组的成员任务累积"""
    task = {}

    for key in redis_conn.scan_iter(f"member_task:{group_wxid}:*"):
        accumulate_score = redis_conn.hget(key, "accumulate_score")
        complete_score = redis_conn.hget(key, "complete_score")
        print(f"accumulate_score: {accumulate_score}, complete_score: {complete_score}")
        task[key.split(':')[-1]] = {"accumulate_score": float(accumulate_score), "complete_score": float(complete_score)}
    return task
def update_group_member_task(redis_conn, group_wxid: str, group_tasks_members: list):
    """更新群组的成员任务累积"""
    # 从redis中获取成员的任务累积

    member_tasks = get_member_task(redis_conn, group_wxid)
    # 遍历 group_tasks_members list，将成员的扣排类型转尝试化为float，如果转化成功则追加到member_tasks[member_wxid]["accumulate_score"]
    # 如果转化失败则忽略
    # 如果成员不在member_tasks中，则初始化。如果state为作废，则不初始化并且不追加。如果为带走或者过，则追加complete_score
    print(f"更新前member_tasks===: {member_tasks}")
    for member_wxid, koupai_type, _, state, _, _, _ in group_tasks_members:
        if member_wxid not in member_tasks and state != "作废":
            member_tasks[member_wxid] = {"accumulate_score": 0, "complete_score": 0}
        try:
            # 当koupai_type为p8或p9时，需要特殊处理（移除掉p8或p9）
            if koupai_type.startswith("p8") or koupai_type.startswith("p9"):
                koupai_type = koupai_type.replace("p8", "").replace("p9", "")
            float(koupai_type)
            if state != "作废":
                member_tasks[member_wxid]["accumulate_score"] += float(koupai_type)
            if state in ["带走", "过"]:
                member_tasks[member_wxid]["complete_score"] += float(koupai_type)
        except ValueError:
            pass

    # 更新redis中的成员任务累积
    print(f"更新后的member_tasks===: {member_tasks}")
    for member_wxid, task_info in member_tasks.items():
        update_member_task(redis_conn, group_wxid, member_wxid, task_info["accumulate_score"], task_info["complete_score"])
def update_member_task(redis_conn, group_wxid: str, member_wxid: str, accumulate_score: float = None, complete_score: float = None):
    """更新群组的成员任务累积"""
    if accumulate_score is None:
        accumulate_score = float(redis_conn.hget(f"member_task:{group_wxid}:{member_wxid}", "accumulate_score"))
    if complete_score is None:
        complete_score = float(redis_conn.hget(f"member_task:{group_wxid}:{member_wxid}", "complete_score"))
    # 当分数小于0时，设置为0
    if accumulate_score < 0:
        accumulate_score = 0
    if complete_score < 0:
        complete_score = 0
    redis_conn.hset(f"member_task:{group_wxid}:{member_wxid}", mapping={
        "group_wxid": group_wxid,
        "member_wxid": member_wxid,
        "accumulate_score": accumulate_score,
        "complete_score": complete_score
    })
    # 同时更新sqlite数据库里的数据累积
    asyncio.run(group_repo.update_group_member_score(group_wxid, member_wxid, accumulate_score, complete_score))
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

def check_koupai_limit(redis_conn, group_wxid: str, current_hour: int) -> bool:
    """检查扣排人数是否超过限制"""
    limit = int(get_group_config(redis_conn, group_wxid).get("limit_koupai", 8))
    return redis_conn.zcard(f"tasks:launch_tasks:{group_wxid}:{(current_hour+1)%24}") >= limit

def copy_to_history_task(redis_conn, group_wxid: str, date: str):
    """复制指定群组的所有扣排记录到历史记录里"""
    # 复制到历史记录里
    current_date = date
    group_keys = redis_conn.scan_iter(f"tasks:launch_tasks:{group_wxid}:*")
    for key in group_keys:
        # 复制到历史记录里
        redis_conn.copy(key, f"history:tasks:{group_wxid}:{current_date}:{key.split(':')[-1]}", replace=True)



redis_conn = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
# get_group_task_members(redis_conn, "49484317759@chatroom", 23)
# get_renwu_rule(redis_conn, "42973360766@chatroom")
# print(get_renwu_list(redis_conn, "42973360766@chatroom"))
# print(get_renwu_dict(get_renwu_list(redis_conn, "42973360766@chatroom")))

