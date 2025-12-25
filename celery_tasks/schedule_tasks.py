import json
import redis
from db.repository import group_repo
from utils.emoji_map import emoji_map
from utils.send_utils_sync import send_message, at_user, get_member_nick
from celery_app import celery_app
from celery import group
from celery.result import AsyncResult
import time
from common.koupai import KoupaiModel
from datetime import datetime, timedelta
import logging
from cache.redis_pool import get_redis_connection
from celery_tasks.tasks_crud import *
import json
from contextlib import contextmanager


logger = logging.getLogger(__name__)
@contextmanager
def task_lock(redis_conn, task_key, timeout=30):
    """
    分布式任务锁
    """
    lock_key = f"task_lock:{task_key}"
    
    # 尝试获取锁
    acquired = redis_conn.set(lock_key, 'locked', nx=True, ex=timeout)
    print(f"尝试获取锁 {lock_key}，结果: {acquired}")
    try:
        if acquired:
            yield True  # 获得锁，可以执行
        else:
            yield False  # 未获得锁，跳过执行
    finally:
        # 执行完成后释放锁（如果是我们获得的）
        if acquired:
            redis_conn.delete(lock_key)

def process_valid_groups(current_hour: int, **kwargs):
    """处理符合下一个分钟扣排任务的群组"""
    valid_groups_start = kwargs.get("valid_groups_start", [])
    valid_groups_end = kwargs.get("valid_groups_end", [])
    valid_groups_end_renwu = kwargs.get("valid_groups_end_renwu", [])
    print(f"valid_groups_start: {valid_groups_start}")
    print(f"valid_groups_end: {valid_groups_end}")
    print(f"valid_groups_end_renwu: {valid_groups_end_renwu}")
    tasks = []
    redis_conn = get_redis_connection()
    for group_wxid in valid_groups_start:
        task_id = f"start:tasks:hosts_tasks:{group_wxid}:{(current_hour+1)%24}"
        # 检查是否有未完成的对应任务
        with task_lock(redis_conn, task_id) as lock_acquired:
            if not lock_acquired:
                continue
        # 先判断是否有未完成的对应任务，有的话直接跳过这个任务创建
        # if AsyncResult(task_id).state == "PENDING":
        #     print(f"开始任务 {task_id} 已存在，状态为 {AsyncResult(task_id).state}")
        #     continue
        task_start = send_koupai_task_start.s(group_wxid, current_hour).set(
                task_id=task_id,
                queue="celery"
            )
        tasks.append(task_start)
    for group_wxid in valid_groups_end:
        task_id = f"end:tasks:hosts_tasks:{group_wxid}:{(current_hour+1)%24}"
        with task_lock(redis_conn, task_id) as lock_acquired:
            if not lock_acquired:
                continue
        task_end = send_koupai_task_end.s(group_wxid, current_hour, "end_koupai").set(
                task_id=task_id,
                queue="celery"
            )
        
        tasks.append(task_end)
    for group_wxid in valid_groups_end_renwu:
        task_id = f"renwu:tasks:hosts_tasks:{group_wxid}:{(current_hour+1)%24}"
        with task_lock(redis_conn, task_id) as lock_acquired:
            if not lock_acquired:
                continue
        task_renwu = send_koupai_task_end.s(group_wxid, current_hour, "end_renwu").set(
                task_id=task_id,
                queue="celery"
            )
        tasks.append(task_renwu)
    task_group = group(tasks)
    logger.info(f"创建 group，包含 {len(tasks)} 个任务")
    return task_group


@celery_app.task
def scheduled_task( koupai_type: str = "all", update_group:str = None,):
    """
    定时任务，每分钟检查一次群组的扣牌时间。
    """
    logger.info("每分钟定时任务开始")
    
    #获取当前分钟、小时
    now = datetime.now()
    current_minute = now.minute
    current_hour = now.hour
    # 检查所有群组的扣牌时间
    redis_conn = get_redis_connection(0)
    # 先获取所有扣排任务群组
    if update_group:
        groups_keys = [update_group]
    else:
        groups_keys = list(redis_conn.smembers("groups_config:koupai_groups"))
    valid_groups = []
    print(f"groups_keys 所有扣牌任务群组: {groups_keys}")
    # 检查下一个小时的开牌任务
    try:
        valid_groups = get_next_hour_group(redis_conn, groups_keys, current_hour)
        print(f"valid_groups 下一个小时的扣牌任务: {valid_groups}")

        # 检查下一个分钟的开牌任务
        valid_groups_start = []
        valid_groups_end = []
        valid_groups_end_renwu = []
        # 保证互斥
        if koupai_type != "end":
            valid_groups_start = get_next_minute_group(redis_conn, valid_groups, current_minute, "start_koupai") 
        if koupai_type != "start":
            valid_groups_end = get_next_minute_group(redis_conn, valid_groups, current_minute, "end_koupai") 
            valid_groups_end_renwu = get_next_minute_group(redis_conn, valid_groups, current_minute, "end_renwu") 
        task_group = process_valid_groups(current_hour, valid_groups_start=valid_groups_start, valid_groups_end=valid_groups_end, valid_groups_end_renwu=valid_groups_end_renwu)
        launch_time = now.replace(minute=current_minute, second=0, microsecond=0)
        print(f"下一分钟到了就开始执行扣排任务: {launch_time}")
        # 立即执行
        utc_time = datetime.utcfromtimestamp(launch_time.timestamp())
        task_group.apply_async(eta=utc_time)
    except Exception as e:
        logger.error(f"检查扣牌任务时出错: {e}")
    


@celery_app.task
def send_koupai_task_start(group_wxid: str, current_hour: int):
    """
    发送指定群组的扣排信息。
    """
    try:
        print(f"发送扣排任务到群组{group_wxid}")
        redis_conn = get_redis_connection(0)
        #在任务列表单里添加扣排任务id
        redis_conn.sadd(f"tasks:launch_tasks:koupai_tasks_list", f"{group_wxid}:{(current_hour+1)%24}")
        #获取群扣排相关信息
        group_config = get_group_config(redis_conn, group_wxid)
        maixu_desc, verify_mode = group_config["maixu_desc"], group_config["verify_mode"]
        hosts_config = get_group_hosts_config(redis_conn, group_wxid, current_hour)
        hsot_desc = hosts_config["host_desc"]
        # 固定排成员
        fixed_hosts = json.loads(hosts_config["fixed_hosts"])
        # 先往扣排队列里添加固定排成员
        for fixed_host in fixed_hosts:
            # 因为固定排成员在最前面，因此基础分数设为1000
            add_with_timestamp(redis_conn, group_wxid, f"{fixed_host}:固定排", base_score=1000, current_hour=(current_hour+1)%24)
        
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
def send_koupai_task_end(group_wxid: str, current_hour: int, task_type: str = "end_koupai"):
    """
    发送指定群组的结束扣排信息。
    """
    try:
        print(f"===发送结束扣排任务到群组{group_wxid}==={task_type}===")
        redis_conn = get_redis_connection(0)
        #获取群扣排相关信息
        group_config = get_group_config(redis_conn, group_wxid)
        end_koupai_time = group_config["end_koupai"]
        end_renwu_time = group_config["end_renwu"]

        #移扣排队列中对应id
        redis_conn.srem(f"tasks:launch_tasks:koupai_tasks_list", f"{group_wxid}:{(current_hour+1)%24}")
        # 当扣排截止时间和任务截止时间相同
        if end_koupai_time == end_renwu_time:
            # 并且task_type为end_koupai时，移除任务列表
            if task_type == "end_koupai":
                redis_conn.srem(f"tasks:launch_tasks:renwu_tasks_list", f"{group_wxid}:{(current_hour+1)%24}")
            # 并且task_type为end_renwu时，不移除任务列表，并且不执行后续代码，因为已经发过结束任务了
            if task_type == "end_renwu":
                print(f"不执行后续代码，{task_type}")
                return
        elif task_type == "end_renwu":
            # 当扣排截止时间和任务截止时间不同时并且传入类型为end_renwu时，移除任务列表
            redis_conn.srem(f"tasks:launch_tasks:renwu_tasks_list", f"{group_wxid}:{(current_hour+1)%24}")
        print(f"发送结束任务到群组{group_wxid}")
        limit_koupai = int(group_config["limit_koupai"])
        hosts_config = get_group_hosts_config(redis_conn, group_wxid, current_hour)
        # 普通排成员
        tasks_members = get_group_task_members(redis_conn, group_wxid, current_hour, limit_koupai)
        mai8tasks_members = get_group_task_members(redis_conn, f"{group_wxid}:mai8", current_hour, limit=1)
        mai9tasks_members = get_group_task_members(redis_conn, f"{group_wxid}:mai9", current_hour, limit=1)
        # print(f"mai8tasks_members: {mai8tasks_members}")
        # print(f"mai9tasks_members: {mai9tasks_members}")

        tasks_members.extend(mai8tasks_members)
        tasks_members.extend(mai9tasks_members)
        tasks_members_desc = "\r".join(
            f"{i+1}. {at_user(member)}({'手速' if koupai_type in ['p', 'P', '排'] else koupai_type})"
            for i, (member, koupai_type, score) in enumerate(tasks_members)
        )
        type_desc = "麦序" if task_type == "end_koupai" else "任务"
        ending = f"  {emoji_map.get('full', '')}\r{type_desc}已截止" if len(tasks_members) >= limit_koupai else f"  {emoji_map.get('empty', '')}: {limit_koupai - len(tasks_members)}\r30分钟内可补"
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
def add_koupai_member(group_wxid: str, member_wxid: str, msg_content: str = "p", **kwargs):
    """
    添加指定群组的成员到扣牌任务列表中。
    """
    try:
        redis_conn = get_redis_connection(0)
        current_hour = datetime.now().hour
        has_task = redis_conn.sismember(f"tasks:launch_tasks:koupai_tasks_list", f"{group_wxid}:{(current_hour+1)%24}")
        has_renwu = redis_conn.sismember(f"tasks:launch_tasks:renwu_tasks_list", f"{group_wxid}:{(current_hour+1)%24}")
        member_limit = int(redis_conn.hget(f"groups_config:{group_wxid}", "limit_koupai"))
        current_members = redis_conn.zcard(f"tasks:launch_tasks:{group_wxid}:{(current_hour+1)%24}")
        print(f"当前群[tasks:launch_tasks:{group_wxid}:{(current_hour+1)%24}]成员数: {current_members}, 人数上限: {member_limit}")
        if has_task and (current_members < member_limit) or (not (has_renwu or has_task) and msg_content == "补"):
            add_with_timestamp(redis_conn, group_wxid, f"{member_wxid}:{msg_content}", current_hour = (current_hour+1)%24, base_score = 0)
            # if current_members + 1 == member_limit:
            current_members = redis_conn.zcard(f"tasks:launch_tasks:{group_wxid}:{(current_hour+1)%24}")
            # return f"成员{member_wxid}已添加到扣排任务列表{group_wxid}"
            # 当采用 手速 扣排达到人数上限的时候，删除扣排阶段队列，添加任务阶段队列
            if current_members >= member_limit:
                # 当有任扣排任务队列的时候再添加，否则就是 识别到 补
                if has_task:
                    #移扣排队列中对应id
                    redis_conn.srem(f"tasks:launch_tasks:koupai_tasks_list", f"{group_wxid}:{(current_hour+1)%24}")
                    print(f"尝试添加任务")
                    #在任务列表单里添加任务id
                    redis_conn.sadd(f"tasks:launch_tasks:renwu_tasks_list", f"{group_wxid}:{(current_hour+1)%24}")
                hosts_config = get_group_hosts_config(redis_conn, group_wxid, current_hour)
                hsot_desc = hosts_config["host_desc"]
                tasks_members = get_group_task_members(redis_conn, group_wxid, current_hour, member_limit)
                # tasks_members: [('wxid_2tkacjo984zq22', 'p'), ('wxid_dofg3jonqvre22', 'p')]
                mai8tasks_members = get_group_task_members(redis_conn, f"{group_wxid}:mai8", current_hour, limit=1)
                mai9tasks_members = get_group_task_members(redis_conn, f"{group_wxid}:mai9", current_hour, limit=1)
                tasks_members.extend(mai8tasks_members)
                tasks_members.extend(mai9tasks_members)
                print(f"当前tasks_members: {tasks_members}")
                tasks_members_desc = "\r".join(
                    f"{i+1}. {at_user(member)}({'手速' if koupai_type in ['p', 'P', '排'] else koupai_type})"
                    for i, (member, koupai_type, score) in enumerate(tasks_members)
                )
                ending = f"\r当前已满 可扣任务" if has_task else ""
                send_message(group_wxid, f"主持: {hsot_desc}\r"
                                        f"时间: {(current_hour+1)%24}-{((current_hour+2)%24)}\r"
                                        f"当前麦序:\r"
                                        f"{tasks_members_desc}\r"
                                        f"  \\uD83C\\uDE35"
                                        f"{ending}")
    except Exception as e:
        logger.error(f"添加成员{member_wxid}到扣牌任务列表{group_wxid}时出错: {e}")
@celery_app.task
def update_koupai_member(group_wxid: str, member_wxid: str, msg_content: str, **kwargs):
    """
    更新扣排任务群中的成员。
    """
    try:
        redis_conn = get_redis_connection(0)
        current_hour = datetime.now().hour
        has_renwu = redis_conn.sismember(f"tasks:launch_tasks:renwu_tasks_list", f"{group_wxid}:{(current_hour+1)%24}")
        if has_renwu:
            limit_koupai = int(redis_conn.hget(f"groups_config:{group_wxid}", "limit_koupai"))
            base_score = get_renwu_dict(get_renwu_list(redis_conn, group_wxid)).get(msg_content, 0)
            before_update_list = get_group_task_members(redis_conn, group_wxid, current_hour, limit_koupai,)
            add_with_timestamp(redis_conn, group_wxid, f"{member_wxid}", current_hour = (current_hour+1)%24, base_score = base_score, msg_content = msg_content, limit_koupai = limit_koupai)
            after_update_list = get_group_task_members(redis_conn, group_wxid, current_hour, limit_koupai)
            print(f"before_update_last: {before_update_list}")
            print(f"after_update_list: {after_update_list}")
            # 插入买89
            mai8tasks_members = get_group_task_members(redis_conn, f"{group_wxid}:mai8", current_hour, limit=1)
            mai9tasks_members = get_group_task_members(redis_conn, f"{group_wxid}:mai9", current_hour, limit=1)
            tasks_members.extend(mai8tasks_members)
            tasks_members.extend(mai9tasks_members)

            tasks_members_desc = "\r".join(
                f"{i+1}. @{get_member_nick(group_wxid, member)}({'手速' if koupai_type in ['p', 'P', '排'] else f'{koupai_type}'})"
                for i, (member, koupai_type, score) in enumerate(after_update_list)
            )
            # 当出现不相等时说明有before中最后一位成员被挤出去了。
            if before_update_list != after_update_list:
                send_message(group_wxid, f"当前麦序:\r"
                                        f"{tasks_members_desc}\r"
                                        f" {emoji_map.get('full', '')}\r"
                                        f"当前已满 可扣任务")
                time.sleep(1) #延迟一秒后@被挤出去的成员。
                #当最后一位成员不在after_update_list中时，说明最后一位成员被挤出去了。
                if before_update_list[-1][0] not in [member[0] for member in after_update_list]:
                    send_message(group_wxid, f"{get_member_nick(member_wxid)} {msg_content} 顶 {at_user(before_update_list[-1][0])}")
            
    except Exception as e:
        logger.error(f"更新成员{member_wxid}在扣排任务列表{group_wxid}时出错: {e}")
@celery_app.task()
def add_mai89_member(group_wxid: str, member_wxid: str, msg_content: str, **kwargs):
    """
    添加指定群组的买89成员。只能有一个，且不影响原来的麦序成员。
    """
    try:
        print(f"添加买89成员{member_wxid}到群组{group_wxid}")
        redis_conn = get_redis_connection(0)
        current_hour = datetime.now().hour
        has_renwu = redis_conn.sismember(f"tasks:launch_tasks:renwu_tasks_list", f"{group_wxid}:{(current_hour+1)%24}")
        if has_renwu:
            part = msg_content.replace("买8", "").strip() if msg_content.startswith("买8") else msg_content.replace("买9", "").strip()
            base_score = get_renwu_dict(get_renwu_list(redis_conn, group_wxid)).get(part, 0)
            if base_score == 0:
                send_message(group_wxid, f"{get_member_nick(group_wxid, member_wxid)} 输入不符合任务设置。请检查输入")
                return
            mai89group_wxid = f"{group_wxid}:mai8" if msg_content.startswith("买8") else f"{group_wxid}:mai9"
            before_update_list = get_group_task_members(redis_conn, mai89group_wxid, current_hour, limit = 1)
            add_with_timestamp(redis_conn, mai89group_wxid, member_wxid, current_hour = (current_hour+1)%24, base_score = base_score, msg_content = msg_content, limit_koupai = 1)
            after_update_list = get_group_task_members(redis_conn, mai89group_wxid, current_hour, limit = 1)
            if before_update_list != after_update_list and len(before_update_list) != 0:
                send_message(group_wxid, f"{at_user(member_wxid)} {msg_content} 顶 {at_user(before_update_list[-1][0])}")
            # 更新买89后打印当前麦序
            group_config = get_group_config(redis_conn, group_wxid)
            hosts_config = get_group_hosts_config(redis_conn, group_wxid, current_hour)
            limit_koupai = int(group_config.get("limit_koupai", 0))
            tasks_members = get_group_task_members(redis_conn, group_wxid, current_hour, limit_koupai)
            # 插入买89
            mai8tasks_members = get_group_task_members(redis_conn, f"{group_wxid}:mai8", current_hour, limit=1)
            mai9tasks_members = get_group_task_members(redis_conn, f"{group_wxid}:mai9", current_hour, limit=1)
            tasks_members.extend(mai8tasks_members)
            tasks_members.extend(mai9tasks_members)
            maixu_desc = "\r".join(
                f"{i+1}. @{get_member_nick(group_wxid, member)}({'手速' if koupai_type in ['p', 'P', '排'] else f'{koupai_type}'})"
                for i, (member, koupai_type, score) in enumerate(tasks_members)
            )
            
            send_message(group_wxid, f"主持:{hosts_config.get('host_desc', '')}\r"
                                    f"时间:{current_hour+1}-{current_hour+2}\r"
                                    f"{maixu_desc}\r"
                                    f" {emoji_map.get('full', '')}\r"
                                    f"当前已满 可扣任务")

    except Exception as e:
        logger.error(f"添加成员{member_wxid}到买89任务列表{group_wxid}时出错: {e}")
@celery_app.task
def delete_koupai_member(group_wxid: str, member_wxid: str, **kwargs):
    """
    删除指定群组的成员从开牌任务列表中。
    """
    try:
        redis_conn = get_redis_connection(0)
        current_hour = datetime.now().hour
        has_renwu = redis_conn.sismember(f"tasks:launch_tasks:renwu_tasks_list", f"{group_wxid}:{(current_hour+1)%24}")
        if not has_renwu:
            limit_koupai = int(redis_conn.hget(f"groups_config:{group_wxid}", "limit_koupai"))
            remaining_members = delete_member(redis_conn, group_wxid, member_wxid, (current_hour+1)%24, limit_koupai)
            print(f"剩余: {remaining_members}")
            if remaining_members > 0:
                send_message(group_wxid, f"{get_member_nick(group_wxid, member_wxid)}你已取排成功\r"
                                        f"当前{emoji_map.get('empty', '')}: {remaining_members}"
                                            )
        if has_renwu:
            send_message(group_wxid, f"主持已设置任务期间不可取")
    except Exception as e:
        logger.error(f"删除成员{member_wxid}从扣牌任务列表{group_wxid}时出错: {e}")
@celery_app.task
def transfer_koupai_member(group_wxid: str, sender_wxid: str, to_wxid: str, msg_content: str, **kwargs): 
    """
    转麦序
    """
    try:
        if sender_wxid == to_wxid:
            send_message(group_wxid, f"{at_user(sender_wxid)}\r不能转给自己")
            return
        redis_conn = get_redis_connection(0)
        current_hour = datetime.now().hour
        current_maixu = get_group_task_members(redis_conn, group_wxid, current_hour,)
        for (member, koupai_type, score) in current_maixu:
            # 如果存在sender_wxid，说明可转麦序
            if member == sender_wxid:
                member = to_wxid
                koupai_type = msg_content
                delete_member(redis_conn, group_wxid, sender_wxid, (current_hour+1)%24, limit_koupai=0)
                add_with_timestamp(redis_conn, group_wxid, f"{to_wxid}", current_hour = (current_hour+1)%24, msg_content = msg_content, extend_score = score)
                send_message(group_wxid, f"{at_user(sender_wxid)}已转麦序给{at_user(to_wxid)}")
                break
        else:
            send_message(group_wxid, f"{at_user(sender_wxid)}\r不在当前麦序中")
    except Exception as e:
        logger.error(f"转麦序时出错: {e}")

@celery_app.task
def get_current_maixu(group_wxid: str, **kwargs):
    """
    查询当前麦序。
    """
    try:
        redis_conn = get_redis_connection(0)
        current_hour = datetime.now().hour
        current_maixu = get_group_task_members(redis_conn, group_wxid, current_hour,0)
        mai8tasks_members = get_group_task_members(redis_conn, f"{group_wxid}:mai8", current_hour, limit=1)
        mai9tasks_members = get_group_task_members(redis_conn, f"{group_wxid}:mai9", current_hour, limit=1)
        current_maixu.extend(mai8tasks_members)
        current_maixu.extend(mai9tasks_members)
        current_desc = "\r".join(
                f"{i+1}. {get_member_nick(group_wxid, member)}({'手速' if koupai_type in ['p', 'P', '排'] else f'{koupai_type}'})"
                for i, (member, koupai_type, score) in enumerate(current_maixu)
            )
        if current_maixu:
            send_message(group_wxid, f"--当前麦序：\r{current_desc}")
        else:
            send_message(group_wxid, "当前没有设置麦序")
    except Exception as e:
        logger.error(f"查询当前麦序时出错: {e}")


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