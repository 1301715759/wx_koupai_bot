import json
import redis
from db.repository import group_repo
from utils.send_utils_sync import send_message, at_user, get_member_nick
from celery_app import celery_app
from celery import group
import time
from common.koupai import KoupaiModel
from datetime import datetime, timedelta
import logging
from cache.redis_pool import get_redis_connection
from celery_tasks.tasks_crud import *
logger = logging.getLogger(__name__)

def process_valid_groups(current_hour: int, **kwargs):
    """处理符合下一个分钟扣排任务的群组"""
    valid_groups_start = kwargs.get("valid_groups_start", [])
    valid_groups_end = kwargs.get("valid_groups_end", [])
    
    tasks = []
    for group_wxid in valid_groups_start:
        task_start = send_koupai_task_start.s(group_wxid, current_hour).set(
                task_id=f"start:tasks:hosts_tasks:{group_wxid}:{(current_hour+1)%24}",
                queue="celery"
            )
        tasks.append(task_start)
    for group_wxid in valid_groups_end:
        task_end = send_koupai_task_end.s(group_wxid, current_hour).set(
                task_id=f"end:tasks:hosts_tasks:{group_wxid}:{(current_hour+1)%24}",
                queue="celery"
            )
        tasks.append(task_end)
    task_group = group(tasks)
    logger.info(f"创建 group，包含 {len(tasks)} 个任务")
    return task_group


@celery_app.task
def scheduled_task():
    """
    定时任务，每分钟检查一次群组的开牌时间。
    """
    logger.info("每分钟定时任务开始")
    #获取当前分钟、小时
    now = datetime.now()
    current_minute = now.minute
    current_hour = now.hour
    # 检查所有群组的开牌时间
    redis_conn = get_redis_connection(0)
    # 先获取所有扣排任务群组
    groups_keys = list(redis_conn.smembers("groups_config:koupai_groups"))
    valid_groups = []
    # 检查下一个小时的开牌任务
    try:
        valid_groups = get_next_hour_group(redis_conn, groups_keys, current_hour)
        print(f"valid_groups 下一个小时的扣牌任务: {valid_groups}")

        # 检查下一个分钟的开牌任务
        valid_groups_start = get_next_minute_group(redis_conn, valid_groups, current_minute, "start_koupai")
        valid_groups_end = get_next_minute_group(redis_conn, valid_groups, current_minute, "end_koupai")
        print(f"valid_groups 下一个分钟的扣牌任务: {valid_groups_start}")

        task_group = process_valid_groups(current_hour, valid_groups_start=valid_groups_start, valid_groups_end=valid_groups_end)
        launch_time = now.replace(minute=current_minute+1, second=0, microsecond=0)
        print(f"下一分钟到了就开始执行扣排任务: {launch_time}")
        # 立即执行
        utc_time = datetime.utcfromtimestamp(launch_time.timestamp())
        task_group.apply_async(eta=utc_time)
    except Exception as e:
        logger.error(f"检查开牌任务时出错: {e}")
    

    




@celery_app.task
def send_koupai_task_start(group_wxid: str, current_hour: int):
    """
    发送指定群组的扣排信息。
    """
    try:
        redis_conn = get_redis_connection(0)
        #在任务列表单里添加指定任务id
        redis_conn.sadd(f"tasks:launch_tasks:tasks_list", f"{group_wxid}:{(current_hour+1)%24}")
        #获取群扣排相关信息
        group_config = get_group_config(redis_conn, group_wxid)
        maixu_desc, verify_mode = group_config["maixu_desc"], group_config["verify_mode"]
        hosts_config = get_group_hosts_config(redis_conn, group_wxid, current_hour)
        hsot_desc = hosts_config["host_desc"]
        now_date = datetime.now().strftime('%m-%d')
        send_message(group_wxid, f"主持: {hsot_desc}\r"
                                 f"时间: {(current_hour+1)%24}-{((current_hour+2)%24)}\r"
                                 f"日期: {now_date}\r"
                                 f"{maixu_desc}\r"
                                 f"扣排代码: p/P/排")
        logger.info(f"===发送扣排任务到群组===: {group_wxid}")
    except Exception as e:
        logger.error(f"发送扣排任务到群组{group_wxid}时出错: {e}")
@celery_app.task
def send_koupai_task_end(group_wxid: str, current_hour: int):
    """
    发送指定群组的结束扣排信息。
    """
    try:
        redis_conn = get_redis_connection(0)
        #获取群扣排相关信息
        group_config = get_group_config(redis_conn, group_wxid)
        limit_koupai = int(group_config["limit_koupai"])
        hosts_config = get_group_hosts_config(redis_conn, group_wxid, current_hour)
        tasks_members = get_group_task_members(redis_conn, group_wxid, current_hour)
        tasks_members_desc = "\r".join(
            f"{i+1}. {at_user(member)}({'手速' if koupai_type in ['p', 'P', '排'] else '其他'})"
            for i, (member, koupai_type) in enumerate(tasks_members)
        )
        ending = "  \\uD83C\\uDE35\r麦序已截止可买8插队" if len(tasks_members) >= limit_koupai else f"  \\uD83C\\uDE33: {limit_koupai - len(tasks_members)}\r30分钟内可补"
        hsot_desc = hosts_config["host_desc"]
        send_message(group_wxid, f"主持: {hsot_desc}\r"
                                 f"时间: {(current_hour+1)%24}-{((current_hour+2)%24)}\r"
                                 f"截止麦序:\r"
                                 f"{tasks_members_desc}\r"
                            
                                 f"{ending}")
        logger.info(f"===发送结束扣排任务到群组===: {group_wxid}")
    except Exception as e:
        logger.error(f"发送结束扣排任务到群组{group_wxid}时出错: {e}")  
@celery_app.task
def add_koupai_member(group_wxid: str, member_wxid: str, **kwargs):
    """
    添加指定群组的成员到开牌任务列表中。
    """
    try:
        redis_conn = get_redis_connection(0)
        current_hour = datetime.now().hour
        has_task = redis_conn.sismember(f"tasks:launch_tasks:tasks_list", f"{group_wxid}:{(current_hour+1)%24}")
        member_limit = int(redis_conn.hget(f"groups_config:{group_wxid}", "limit_koupai"))
        current_members = redis_conn.zcard(f"tasks:launch_tasks:{group_wxid}:{(current_hour+1)%24}")
        print(f"当前群[tasks:launch_tasks:{group_wxid}:{(current_hour+1)%24}]成员数: {current_members}, 人数上限: {member_limit}")
        if has_task and (current_members < member_limit):
            add_with_timestamp(redis_conn, group_wxid, f"{member_wxid}:p", current_hour = (current_hour+1)%24)
            # if current_members + 1 == member_limit:
            current_members = redis_conn.zcard(f"tasks:launch_tasks:{group_wxid}:{(current_hour+1)%24}")
            # return f"成员{member_wxid}已添加到扣排任务列表{group_wxid}"
            if current_members >= member_limit:
                hosts_config = get_group_hosts_config(redis_conn, group_wxid, current_hour)
                hsot_desc = hosts_config["host_desc"]
                tasks_members = get_group_task_members(redis_conn, group_wxid, current_hour)
                # tasks_members: [('wxid_2tkacjo984zq22', 'p'), ('wxid_dofg3jonqvre22', 'p')]
                
                print(f"当前tasks_members: {tasks_members}")
                tasks_members_desc = "\r".join(
                    f"{i+1}. {at_user(member)}({'手速' if koupai_type in ['p', 'P', '排'] else '其他'})"
                    for i, (member, koupai_type) in enumerate(tasks_members)
                )
                send_message(group_wxid, f"主持: {hsot_desc}\r"
                                        f"时间: {(current_hour+1)%24}-{((current_hour+2)%24)}\r"
                                        f"当前麦序:\r"
                                        f"{tasks_members_desc}\r"
                                        f"  \\uD83C\\uDE35\r"
                                        f"当前已满 可扣任务")
        
        
        
    except Exception as e:
        logger.error(f"添加成员{member_wxid}到开牌任务列表{group_wxid}时出错: {e}")

# @celery_app.task
# def set_koupai_tasks(koupai_data: dict):
#     """
#     设置指定群组的开牌时间。
#     """
#     # 将字典转换为KoupaiModel对象
#     koupai_model = KoupaiModel(**koupai_data)
    
#     # 解析时间字符串为分钟数
#     try:
#         start_minutes = int(koupai_model.start_time)
#     except ValueError:
#         return f"无效的时间格式: {koupai_model.start_time}"
    
#     # 检查时间是否在有效范围内
#     if not (0 <= start_minutes <= 59):
#         return f"时间必须在0到59之间: {koupai_model.start_time}"
    
#     # 存储开牌时间
#     redis_conn = get_redis_connection(0)
#     # 使用mapping参数将整个模型存储为哈希表，将None值转换为空字符串
#     koupai_dict = {k: v if v is not None else "" for k, v in koupai_model.dict().items()}
#     redis_conn.hset(f"koupai:{koupai_model.group_wxid}", mapping=koupai_dict)
#     print(f"开牌时间已设置为: {start_minutes}分")
#     return f"开牌时间已设置为: {start_minutes}分"

