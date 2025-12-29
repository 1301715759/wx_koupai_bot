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
    valid_groups_schedule = kwargs.get("valid_groups_schedule", [])
    print(f"valid_groups_start: {valid_groups_start}")
    print(f"valid_groups_end: {valid_groups_end}")
    print(f"valid_groups_end_renwu: {valid_groups_end_renwu}")
    tasks = []
    redis_conn = get_redis_connection()
    # 处理开始扣排任务
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
    # 处理结束扣排任务
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
    # 处理任务结束
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
    # 处理小时打卡记录表
    for group_wxid in valid_groups_schedule:
        task_id = f"schedule:tasks:hosts_tasks:{group_wxid}:{current_hour}"
        with task_lock(redis_conn, task_id) as lock_acquired:
            if not lock_acquired:
                continue
        task_schedule = send_task_schedule.s(group_wxid, current_hour).set(
                task_id=task_id,
                queue="celery"
            )
        tasks.append(task_schedule)
    task_group = group(tasks)
    logger.info(f"创建 group，包含 {len(tasks)} 个任务")
    return task_group


@celery_app.task
def scheduled_task( koupai_type: str = "all", update_group:str = None, **kwargs):
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
    # 检查下一个小时的扣牌任务
    try:
        valid_groups = get_next_hour_group(redis_conn, groups_keys, current_hour)
        print(f"valid_groups 下一个小时的扣牌任务: {valid_groups}")

        # 检查下一个分钟的扣牌任务
        valid_groups_start = []
        valid_groups_end = []
        valid_groups_end_renwu = []
        valid_groups_schedule = []
        # 保证互斥
        if koupai_type != "end":
            valid_groups_start = get_next_minute_group(redis_conn, valid_groups, current_minute, "start_koupai") 
        if koupai_type != "start":
            valid_groups_end = get_next_minute_group(redis_conn, valid_groups, current_minute, "end_koupai") 
            valid_groups_end_renwu = get_next_minute_group(redis_conn, valid_groups, current_minute, "end_renwu") 
        # 当前为默认all并且是第1分钟时，检查符合发送打卡记录表的群组
        if koupai_type == "all" and (kwargs.get("schedule_minute") or current_minute == 1):
            valid_groups_schedule = get_next_schedule_group(redis_conn, groups_keys, current_hour)
        task_group = process_valid_groups(current_hour, valid_groups_start=valid_groups_start, valid_groups_end=valid_groups_end, valid_groups_end_renwu=valid_groups_end_renwu, valid_groups_schedule=valid_groups_schedule)
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
            add_with_timestamp(redis_conn, group_wxid, f"{fixed_host}",msg_content=="固定排", base_score=1000, current_hour=(current_hour+1)%24)
        
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
        tasks_members = get_group_task_members(redis_conn, group_wxid, current_hour)

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
        group_config = get_group_config(redis_conn, group_wxid)
        has_task = redis_conn.sismember(f"tasks:launch_tasks:koupai_tasks_list", f"{group_wxid}:{(current_hour+1)%24}")
        has_renwu = redis_conn.sismember(f"tasks:launch_tasks:renwu_tasks_list", f"{group_wxid}:{(current_hour+1)%24}")
        member_limit = int(group_config["limit_koupai"])
        # 获取分数为正值的成员数量
        print("==============")
        current_members = redis_conn.zcount(f"tasks:launch_tasks:{group_wxid}:{(current_hour+1)%24}", 0, float('inf'))
        print(f"当前群[tasks:launch_tasks:{group_wxid}:{(current_hour+1)%24}]成员数: {current_members}, 人数上限: {member_limit}")
        if has_task and (current_members < member_limit) or ( msg_content == "补"):
            base_score = 0
            # 先获取是否有固定手速排人数和固定手速排任务
            fixed_p_num = int(group_config["fixed_p_num"])
            fixed_renwu_desc = group_config["fixed_renwu_desc"]
            # 如果固定手速排人数大于0，并且有task，那么需要设置base_score
            if fixed_p_num > 0 and has_task and msg_content != "补":
                # 获取固定手速排人数（即member带固定手速的人恩叔)
                # 先获取获取群组的扣排麦序信息
                koupai_members = get_group_task_members(redis_conn, group_wxid, current_hour)
                fixed_num = 0
                for member, koupai_type, score in koupai_members:
                    if koupai_type == "固定手速":
                        fixed_num += 1
                # 当获取到的固定手速排人数大于固定手速排人数时，固定手速人数未达标。需要设置base_score
                if fixed_p_num > fixed_num:
                    # 设置base_score为500，低于固定排
                    base_score = 500
                    msg_content = "固定手速"
                    # 如果有固定手速排任务， 那么base_score为 字典中对应的分数
                    if fixed_renwu_desc != "":
                        base_score = get_renwu_dict(get_renwu_list(redis_conn, group_wxid)).get(fixed_renwu_desc, 0)
            add_with_timestamp(redis_conn, group_wxid, f"{member_wxid}", msg_content=msg_content, current_hour = (current_hour+1)%24, base_score = base_score)

            # 获取正分的成员（因为负分为买89，不参与扣排人数限制）
            current_members = redis_conn.zcount(f"tasks:launch_tasks:{group_wxid}:{(current_hour+1)%24}", 0, float('inf'))
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
                tasks_members = get_group_task_members(redis_conn, group_wxid, current_hour)
                # tasks_members: [('wxid_2tkacjo984zq22', 'p'), ('wxid_dofg3jonqvre22', 'p')]
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
            # 添加成员并返回是否有踢出成员
            exit_member =add_with_timestamp(redis_conn, group_wxid, f"{member_wxid}", current_hour = (current_hour+1)%24, base_score = base_score, msg_content = msg_content, limit_koupai = limit_koupai)
            tasks_members = get_group_task_members(redis_conn, group_wxid, current_hour)
            print(f"tasks_members: {tasks_members}")


            tasks_members_desc = "\r".join(
                f"{i+1}. @{get_member_nick(group_wxid, member)}({'手速' if koupai_type in ['p', 'P', '排'] else f'{koupai_type}'})"
                for i, (member, koupai_type, score) in enumerate(tasks_members)
            )
            # 当出现exit_member时，说明有成员被挤出去了。
            if not exit_member or limit_koupai == len(tasks_members):
                send_message(group_wxid, f"当前麦序:\r"
                                        f"{tasks_members_desc}\r"
                                        f" {emoji_map.get('full', '')}\r"
                                        f"当前已满 可扣任务")
                time.sleep(1) #延迟一秒后@被挤出去的成员。
                # 如果被挤出去的成员不是当前成员，才@被挤出去的成员
                if exit_member != member_wxid and not exit_member:
                    send_message(group_wxid, f"{get_member_nick(member_wxid)} {msg_content} 顶 {at_user(exit_member)}")
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
            mai_type = f"mai8" if msg_content.startswith("买8") else f"mai9"
            # 如果是买9，base_score要额外-500，因为我们规定买9要在买8下面
            if mai_type == "mai8":
                base_score = base_score - 200
            if mai_type == "mai9":
                base_score = base_score - 1000
            group_config = get_group_config(redis_conn, group_wxid)
            hosts_config = get_group_hosts_config(redis_conn, group_wxid, current_hour)
            limit_koupai = int(group_config.get("limit_koupai", 0))
            # 添加并获取被t出去的成员
            exit_member = add_with_timestamp(redis_conn, group_wxid, member_wxid, current_hour = (current_hour+1)%24, base_score = base_score, msg_content = msg_content, limit_koupai = limit_koupai, mai_type = mai_type)
            
            if exit_member:
                send_message(group_wxid, f"{at_user(member_wxid)} {msg_content} 顶 {at_user(exit_member)}")
            # 更新买89后打印当前麦序
            tasks_members = get_group_task_members(redis_conn, group_wxid, current_hour)
            hosts_config = get_group_config(redis_conn, group_wxid).get("hosts_config", {})
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
    删除指定群组的成员从扣牌任务列表中。
    消息内容一般为 取 的时候触发
    """
    try:
        redis_conn = get_redis_connection(0)
        current_hour = datetime.now().hour
        current_minute = datetime.now().minute
        group_config = get_group_config(redis_conn, group_wxid)

        # 获取 任务是否可取、手速是否可取、取排时间（为0默认为任意时间可取）、扣排人数
        renwu_qu = int(group_config.get("renwu_qu", 0))
        p_qu = int(group_config.get("p_qu", 0))
        qu_time = int(group_config.get("qu_time", 0))
        limit_koupai = int(group_config.get("limit_koupai", 0))
        # 获取扣排队列（包括买89）中所有成员
        task_members = get_group_task_members(redis_conn, group_wxid, current_hour)

        # 获取成员的扣排类型
        member_type = next((koupai_type for member, koupai_type, score in task_members if member == member_wxid), None)
        print(f"成员{member_wxid}的扣排类型: {member_type}")
        group_wxid_this = ""
        if not member_type:
            send_message(group_wxid, f"{get_member_nick(group_wxid, member_wxid)} 不在扣牌列表中")
            return
        if member_type in ['p', 'P', '排'] and p_qu == 0:
            send_message(group_wxid, f"群已设置手速不可取")
            return
        if member_type in group_config.get("renwu_desc", []) and renwu_qu == 0:
            send_message(group_wxid, f"群已设置取任务不可取")
            return
        if qu_time != 0 and current_minute > qu_time:
            send_message(group_wxid, f"当前不是取排时间")
            return
        remaining_members = delete_member(redis_conn, group_wxid, member_wxid, (current_hour+1)%24, limit_koupai)
        print(f"剩余: {remaining_members}")
        if remaining_members > 0:
            send_message(group_wxid, f"{get_member_nick(group_wxid, member_wxid)}你已取排成功\r"
                                    f"当前{emoji_map.get('empty', '')}: {remaining_members}"
                                        )
    except Exception as e:
        logger.error(f"删除成员{member_wxid}从扣牌任务列表{group_wxid}时出错: {e}")
@celery_app.task
def delete_koupai_members(group_wxid: str, current_hour: int, delete_count: str = "all", **kwargs):
    """
    从扣牌列表中删除指定数量的群组成员。
    消息内容一般为 设置麦序作废人数/本档作废/上档作废 的时候触发
    """
    try:
        redis_conn = get_redis_connection(0)
        if delete_count == "all":
            delete_count = 0
        delete_members(redis_conn, group_wxid, current_hour, delete_count)
    except Exception as e:
        logger.error(f"从扣牌列表中删除指定数量的群组成员时出错: {e}")
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
def check_koupai_member_limit(group_wxid: str, limit_koupai: int):
    """
    检查扣排人数是否超过了设置人数，超过了则删除最小的正分扣排人员
    """
    redis_conn = get_redis_connection(0)
    current_hour = datetime.now().hour
    # 获取群配置
    group_config = get_group_config(redis_conn, group_wxid)
    # 获取正分扣排人数
    koupai_count = redis_conn.zcount(f"tasks:launch_tasks:{group_wxid}:{(current_hour+1)%24}", 0, float('inf'))
    if koupai_count > limit_koupai:
        print(f"当前扣排人数: {koupai_count}, 限制人数: {limit_koupai}")
        # 获取分数最小的正分扣排人员
        postive_min_members = redis_conn.zrangebyscore(f"tasks:launch_tasks:{group_wxid}:{(current_hour+1)%24}", 0, float('inf'), start=0, num=koupai_count - limit_koupai)
        # 删除这些人员
        redis_conn.zrem(f"tasks:launch_tasks:{group_wxid}:{(current_hour+1)%24}", *postive_min_members)
        


    
@celery_app.task
def get_current_maixu(group_wxid: str, **kwargs):
    """
    查询当前麦序。
    """
    try:
        redis_conn = get_redis_connection(0)
        current_hour = datetime.now().hour
        current_maixu = get_group_task_members(redis_conn, group_wxid, current_hour)
        current_desc = "\r".join(
                f"{i+1}. @{get_member_nick(group_wxid, member)}({'手速' if koupai_type in ['p', 'P', '排'] else f'{koupai_type}'})"
                for i, (member, koupai_type, score) in enumerate(current_maixu)
            )
        if current_maixu:
            send_message(group_wxid, f"当前麦序：\r{current_desc}")
        else:
            send_message(group_wxid, "当前没有设置麦序")
    except Exception as e:
        logger.error(f"查询当前麦序时出错: {e}")
@celery_app.task
def add_bb_member(group_wxid: str, member_wxid: str, msg_content: str, **kwargs):
    """
    添加报备
    """
    try:
        redis_conn = get_redis_connection(0)
        current_hour = datetime.now().hour
        current_minute = datetime.now().minute
        current_date = datetime.now().strftime("%Y-%m-%d")
        # 获取报备相关内容
        group_config = get_group_config(redis_conn, group_wxid)
        bb_time = int(group_config.get("bb_time", 15))
        bb_limit = int(group_config.get("bb_limit", 2))
        bb_in_hour = int(group_config.get("bb_in_hour", "0"))
        bb_timeout_desc = group_config.get("bb_timeout_desc", "您已超时。")
        # 获取指定群id下的报备列表
        bb_list = list(redis_conn.scan_iter(f"history:{current_date}:bb:{group_wxid}:{current_hour}:*"))
        print(f"当前小时报备列表: {bb_list}")
        # 获取还没回来的成员数量
        bb_sum = 0
        if bb_list:
            for bb_key in bb_list:
                is_back = redis_conn.hget(bb_key, "is_back")
                if is_back == "0":
                    bb_sum += 1
        if bb_sum >= bb_limit:
            send_message(group_wxid, f"已超过报备人数\r当前设置报备人数:{bb_limit}人")
            return
        # 获取报备列表中当前小时成员的报备的次数
        bb_count = len(list(redis_conn.scan_iter(f"history:{current_date}:bb:{group_wxid}:{current_hour}:{member_wxid}:*")))
        # 检查是否超过了一小时内的报备次数
        if bb_in_hour <= bb_count:
            send_message(group_wxid, f"已超过每小时报备次数\r当前设置小时报备次数:{bb_in_hour}次")
            return
        # 加入报备列表
        # 计算过期时间（datetime格式）
        time_out = datetime.now() + timedelta(minutes=bb_time)
        key = f"history:{current_date}:bb:{group_wxid}:{current_hour}:{member_wxid}:{current_minute}"
        redis_conn.hset(key, mapping={
                    "group_wxid": group_wxid, "member_wxid": member_wxid, "msg_content": msg_content,
                    "create_time": datetime.now().strftime("%H:%M"),"back_time": "",
                    "is_timeout": "0", "is_back": "0"})
        # 发送报备成功消息
        send_message(group_wxid, f"{at_user(member_wxid)}{bb_time}分钟之内回来，回厅再发一个“回”")
        # 将过期时间转为utc时间
        time_out_utc = time_out.utcfromtimestamp(time_out.timestamp())
        print(f"过期时间(UTC): {time_out_utc}")
        # 将timeout任务添加到celery队列，到过期时间时候执行，并且指定task_id，方便我们控制
        send_timeout_message.apply_async(args=[group_wxid, member_wxid, bb_timeout_desc, key], 
                                        eta=time_out_utc, task_id=f"send_timeout_{group_wxid}_{member_wxid}")
    except Exception as e:
        logger.error(f"添加报备时出错: {e}")

@celery_app.task
def send_timeout_message(group_wxid: str, member_wxid: str, bb_timeout_desc: str, key: str, **kwargs):
    """
    发送报备超时消息
    """
    try:
        redis_conn = get_redis_connection(0)
        # 标记为超时
        redis_conn.hset(key, mapping={
                    "is_timeout": "1"})

        send_message(group_wxid, f"{at_user(member_wxid)}{bb_timeout_desc}")
    except Exception as e:
        logger.error(f"发送报备超时消息时出错: {e}")
   
@celery_app.task
def delete_bb_member(group_wxid: str, member_wxid: str):
    """
    删除报备
    """
    try:
        redis_conn = get_redis_connection(0)
        # 先删除超时任务
        result = AsyncResult(f"send_timeout_{group_wxid}_{member_wxid}")
        result.revoke(terminate=True)

        current_hour = datetime.now().hour
        current_minute = datetime.now().minute
        current_date = datetime.now().strftime("%Y-%m-%d")
        # 获取列表
        bb_list = list(redis_conn.scan_iter(f"history:{current_date}:bb:{group_wxid}:{current_hour}:{member_wxid}:*"))
        # 如果没有，那再尝试获取上一小时的（因为时间可能跨小时了，注意0点）
        if not bb_list:
            # 如果当前小时是0点，上一小时就是23点
            if current_hour == 0:
                current_hour = 23
                current_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                current_hour -= 1
            bb_list = list(redis_conn.scan_iter(f"history:{current_date}:bb:{group_wxid}:{current_hour}:{member_wxid}:*"))
        if not bb_list:
            send_message(group_wxid, f"{at_user(member_wxid)}当前没有报备记录。")
            return
        # 遍历列表，找到距离当前时间最近的项，即依据分钟排序
        # 找出list中分钟最大的项，即最近的项
        last_key = max(bb_list, key=lambda x: int(x.split(":")[-1]))
        # 将其is_back设置为1
        redis_conn.hset(last_key, "is_back", "1")
        # 设置back_time为当前时间
        redis_conn.hset(last_key, "back_time", datetime.now().strftime("%H:%M"))
        bb_back_desc = get_group_bb_back_desc(redis_conn, group_wxid)
        send_message(group_wxid, f"{at_user(member_wxid)}{bb_back_desc}")
    except Exception as e:
        logger.error(f"删除报备时出错: {e}")

@celery_app.task
def send_task_schedule(group_wxid: str, current_hour: int):
    """
    发送任务打卡记录
    """
    try:
        redis_conn = get_redis_connection(0)
        date = datetime.now().strftime("%m-%d")
        last_hour = (current_hour - 1) % 24
        last_hour_config = redis_conn.hgetall(f"tasks:hosts_tasks_config:{group_wxid}:{last_hour}")
        if not last_hour_config:
            return f"{group_wxid}上小时没有场次。"
        # 获取上小时的场次描述
        last_hour_group_desc = last_hour_config.get("host_desc", "")
        # 获取上小时的场次时间
        last_hour_start_schedule = int(last_hour_config.get("start_schedule", ""))
        last_hour_end_schedule = int(last_hour_config.get("end_schedule", ""))
        tasks_members = {}
        # 获取上场的扣排信息
        print(f"上小时场次时间: {last_hour_start_schedule} - {last_hour_end_schedule}")
        for time in range(last_hour_start_schedule, last_hour_end_schedule):
            # 获取扣排信息
            lianpai_desc = redis_conn.zrange(f"tasks:launch_tasks:{group_wxid}:{time}", 0, -1)
            print(f"检查{time}的扣排信息: {lianpai_desc}")
            if lianpai_desc:
                for lianpai in lianpai_desc:
                    # wxid_2tkacjo984zq22:10.0 这样的类型，分割出wxid和扣排类型
                    member_wxid, koupai_type = lianpai.split(":")
                    # 合并相同wxid的扣排类型为数组
                    if member_wxid in tasks_members:
                        tasks_members[member_wxid].append(koupai_type)
                    else:
                        tasks_members[member_wxid] = [koupai_type]
        # print(f"{last_hour_group_desc} 上场的扣排信息: {json.dumps(tasks_members, ensure_ascii=False)}")
        tasks_desc = "——麦序明细————"
        print(f"{last_hour_group_desc} 上场的扣排信息: {tasks_members}")
        for member_wxid, koupai_types in tasks_members.items():
            if "作废" in koupai_types:
                continue
            # 转化为 @昵称[扣排次数] 扣排详情
            nick_name = get_member_nick(group_wxid, member_wxid) or member_wxid
            print(f"{nick_name} 扣排类型: {koupai_types}")
            tasks_desc += f"\r@{nick_name} [{len(koupai_types)}]  {'+'.join(koupai_types)}"
        send_message(group_wxid, f"{emoji_map.get('schedule')} 打卡记录表\r"
                                f"主持: {last_hour_group_desc}\r"
                                f"日期: {date}\r"
                                f"时间: {last_hour_start_schedule}-{last_hour_end_schedule}\r"
                                f"{tasks_desc}\r"
                                "【互动】\r"
                                "【全麦】\r"
                                "【冠厅】\r"
                                "【带走】\r"
                                "【收光】\r"
                                "【歌单】\r"
                                "【黑麦】"
                                )
    except Exception as e:
        logger.error(f"发送任务打卡记录时出错: {e}")
