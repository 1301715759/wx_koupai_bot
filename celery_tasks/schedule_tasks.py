import json
import redis
from db.repository import group_repo
from utils.send_utils_sync import send_message
from celery_app import celery_app
from celery import group
import time
from common.koupai import KoupaiModel
from datetime import datetime, timedelta
import logging
from cache.redis_pool import get_redis_connection

logger = logging.getLogger(__name__)

def get_next_hour_group(groups_wxid: list, current_hour: int) -> list:
    """检查符合下一个小时扣排任务的群组"""
    valid_groups = []
    task_hour = (current_hour + 1) % 24
    redis_conn = get_redis_connection(0)
    for group_wxid in groups_wxid:
        field = f"tasks:hosts_tasks:{group_wxid}:{task_hour}"
        print(f"field: {field}")
        check_hour = redis_conn.keys(field)
        if not check_hour:
            continue
        valid_groups.append(group_wxid)
    return valid_groups

def get_next_minute_group(groups_wxid: list, current_minute: int, field: str) -> list:
    """检查符合下一个分钟扣排任务的群组"""
    valid_groups = []
    task_minute = (current_minute + 1) % 60
    redis_conn = get_redis_connection(0)
    for group_wxid in groups_wxid:
        key = f"groups_config:{group_wxid}"
        print(f"field: {key}")
        check_minute = (redis_conn.hget(key, field) == str(task_minute))
        if not check_minute:
            continue
        valid_groups.append(group_wxid)
    return valid_groups


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
    groups_keys = list(redis_conn.smembers("groups_config:koupai_groups"))
    valid_groups = []
    # 检查下一个小时的开牌任务
    try:
        valid_groups = get_next_hour_group(groups_keys, current_hour)
        print(f"valid_groups 下一个小时的开牌任务: {valid_groups}")

        # 检查下一个分钟的开牌任务
        valid_groups_start = get_next_minute_group(valid_groups, current_minute, "start_koupai")
        valid_groups_end = get_next_minute_group(valid_groups, current_minute, "end_koupai")
        # print(f"valid_groups 下一个分钟的开牌任务: {valid_groups}")
    except Exception as e:
        logger.error(f"检查开牌任务时出错: {e}")
    if valid_groups_start:
        logger.info(f"群组{valid_groups}符合下一个分钟的扣排任务")
        for group_wxid in valid_groups:
            task_start = group(
                send_koupai_task_start.s(group_wxid, current_hour).set(
                task_id=f"start:tasks:hosts_tasks:{group_wxid}:{(current_hour+1)%24}",
                queue="celery"
                )
            )
        launch_time = now.replace(minute=current_minute+1, second=0, microsecond=0)
        print(f"下一分钟到了就开始执行扣排任务: {launch_time}")
        # 立即执行
        utc_time = datetime.utcfromtimestamp(launch_time.timestamp())
        task_start.apply_async(eta=utc_time)
    if valid_groups_end:
        logger.info(f"群组{valid_groups_end}符合下一个分钟的结束扣排任务")
        for group_wxid in valid_groups_end:
            task_end = group(
                send_koupai_task_end.s(group_wxid, current_hour).set(
                task_id=f"end:tasks:hosts_tasks:{group_wxid}:{(current_hour+1)%24}",
                queue="celery"
                )
            )
        #下一分钟到了就开始执行
        launch_time = now.replace(minute=current_minute+1, second=0, microsecond=0)
        print(f"下一分钟到了就开始执行: {launch_time}")
        # 立即执行
        utc_time = datetime.utcfromtimestamp(launch_time.timestamp())
        
        task_end.apply_async(eta=utc_time)




@celery_app.task
def send_koupai_task_start(group_wxid: str, current_hour: int):
    """
    发送指定群组的扣排信息。
    """
    try:
        redis_conn = get_redis_connection(0)

        group_config = redis_conn.hvals(f"groups_config:{group_wxid}")
        maixu_desc = group_config[6]
        verify_mode = group_config[5]
        hsot_desc = redis_conn.hget(f"tasks:hosts_tasks:{group_wxid}:{(current_hour+1)%24}", "host_desc")
        now_date = datetime.now().strftime('%m-%d')
        send_message(group_wxid, f"主持: {hsot_desc}\r"
                                 f"时间: {current_hour+1}-{current_hour+2}\r"
                                 f"日期: {now_date}\r"
                                 f"{maixu_desc}\r"
                                 f"扣排代码: {verify_mode}")
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
        group_config = redis_conn.hvals(f"groups_config:{group_wxid}")
        limit_koupai = int(group_config[4])
        host_task = redis_conn.hvals(f"tasks:hosts_tasks:{group_wxid}:{(current_hour+1)%24}")
        current_koupai_sum = int(host_task[4])
        current_koupai_info = host_task[5]
        ending = "\\uD83C\\uDE35麦序已截止" if current_koupai_sum >= limit_koupai else f"\\uD83C\\uDE33: {limit_koupai - current_koupai_sum}\r30分钟内可补"
        hsot_desc = redis_conn.hget(f"tasks:hosts_tasks:{group_wxid}:{(current_hour+1)%24}", "host_desc")
        send_message(group_wxid, f"主持: {hsot_desc}\r"
                                 f"时间: {current_hour+1}-{current_hour+2}\r"
                                 f"截止麦序:\r"
                                 f"\r"
                                 f"{ending}")
        logger.info(f"===发送结束扣排任务到群组===: {group_wxid}")
    except Exception as e:
        logger.error(f"发送结束扣排任务到群组{group_wxid}时出错: {e}")  


# # 一般传入命令为 设置扣排时间26
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

