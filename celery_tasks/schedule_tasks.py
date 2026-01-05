import json
import redis
from db.repository import group_repo
from utils.emoji_map import emoji_map
from utils.send_utils_sync import send_message, at_user, get_member_nick
from celery_app import celery_app
from celery import group
from celery.result import AsyncResult
import time
from datetime import datetime, timedelta
import logging
from cache.redis_pool import get_redis_connection
from celery_tasks.tasks_crud import *
from celery_tasks.initialize_tasks import initialize_tasks
import json
from contextlib import contextmanager
import asyncio

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

        # 每日0点执行重新初始化所有任务
        if current_hour == 0 and current_minute == 0 or kwargs.get("init_tasks"):
            asyncio.run(initialize_tasks.load_from_database())

        # 每日0点59分执行 
        if current_hour == 0 and current_minute == 59 or kwargs.get("save_history"):
            # 存储今日麦序记录到历史记录
            # 并且清空redis内的launch_tasks缓存
            save_task_schedule_day_history.delay(cleanup=True)

        # 保证互斥
        if koupai_type != "end":
            valid_groups_start = get_next_minute_group(redis_conn, valid_groups, current_minute, "start_koupai") 
        if koupai_type != "start":
            valid_groups_end = get_next_minute_group(redis_conn, valid_groups, current_minute, "end_koupai") 
            valid_groups_end_renwu = get_next_minute_group(redis_conn, valid_groups, current_minute, "end_renwu") 
        # 当前为默认all并且是第1分钟时，检查符合发送打卡记录表的群组
        if koupai_type == "all" and (kwargs.get("schedule_minute") or current_minute == 1):
            valid_groups_schedule = get_next_schedule_group(redis_conn, groups_keys, current_hour)
            print(f"valid_groups_schedule 符合发送打卡记录表的群组: {valid_groups_schedule}")


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
            add_with_timestamp(redis_conn, group_wxid, f"{fixed_host}",msg_content="固定排", base_score=1000, current_hour=(current_hour+1)%24)
        
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

                # 重新获取task_members，因为在发送结束任务时，可能会有成员变更，并且要带上“带走”的成员
                tasks_members = get_group_task_members(redis_conn, group_wxid, current_hour, with_daizou=True)
                # 更新redis中的成员任务累积
                update_group_member_task(redis_conn, group_wxid, tasks_members)
                return
        elif task_type == "end_renwu":
            # 当扣排截止时间和任务截止时间不同时并且传入类型为end_renwu时，移除任务列表
            redis_conn.srem(f"tasks:launch_tasks:renwu_tasks_list", f"{group_wxid}:{(current_hour+1)%24}")
            # 重新获取task_members，因为在发送结束任务时，可能会有成员变更，并且要带上“带走”的成员
            tasks_members = get_group_task_members(redis_conn, group_wxid, current_hour, with_daizou=True)
            # 更新redis中的成员任务累积
            update_group_member_task(redis_conn, group_wxid, tasks_members)
        print(f"发送结束任务到群组{group_wxid}")
        limit_koupai = int(group_config["limit_koupai"])
        hosts_config = get_group_hosts_config(redis_conn, group_wxid, current_hour)
        # 普通排成员
        tasks_members = get_group_task_members(redis_conn, group_wxid, current_hour)
        print(f"tasks_members: {tasks_members}")
        tasks_members_desc = "\r".join(
            f"{i+1}. {at_user(member)}({'手速' if koupai_type in ['p', 'P', '排'] else koupai_type})"
            for i, (member, koupai_type, score, state, _, _, _) in enumerate(tasks_members)
        )
        type_desc = "麦序" if task_type == "end_koupai" else "任务"
        ending = f"  {emoji_map.get('full', '')}\r{type_desc}已截止" if len(tasks_members) >= limit_koupai else f"  {emoji_map.get('empty', '')}: {limit_koupai - len(tasks_members)}\r30分钟内可补"
        hsot_desc = hosts_config["host_desc"]
        send_message(group_wxid, f"主持: {hsot_desc}\r"
                                 f"时间: {(current_hour+1)%24}-{((current_hour+2)%24)}\r"
                                 f"截止麦序:\r"
                                 f"{tasks_members_desc}\r"

                                 f"{ending}")

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
                for member, koupai_type, score, state, _, _, _ in koupai_members:
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
            if msg_content == "补":
                koupai_type = "补"
            elif msg_content == "固定手速":
                koupai_type = "固定手速"
            else:
                koupai_type = "手速"
            add_with_timestamp(redis_conn, group_wxid, f"{member_wxid}", msg_content=koupai_type, current_hour = (current_hour+1)%24, base_score = base_score)

            # 获取正分的成员（因为负分为买89，不参与扣排人数限制）
            current_date = datetime.now().strftime("%Y-%m-%d")
            current_members = redis_conn.zcount(f"tasks:launch_tasks:{current_date}:{group_wxid}:{(current_hour+1)%24}", 0, float('inf'))
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
                    f"{i+1}. {at_user(member)}({koupai_type})"
                    for i, (member, koupai_type, score, state, _, _, _) in enumerate(tasks_members)
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
                f"{i+1}. @{get_member_nick(group_wxid, member)}({koupai_type})"
                for i, (member, koupai_type, score, state, _, _, _) in enumerate(tasks_members)
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
            # 当成员已经在task_members中时，不允许重复添加
            tasks_members = get_group_task_members(redis_conn, group_wxid, current_hour)
            if any(member == member_wxid for member, _, _, _, _, _, _ in tasks_members):
                send_message(group_wxid, f"{get_member_nick(group_wxid, member_wxid)} 已经在扣排任务列表中")
                return
            part = msg_content.replace("买8", "").strip() if msg_content.startswith("买8") else msg_content.replace("买9", "").strip()
            base_score = get_renwu_dict(get_renwu_list(redis_conn, group_wxid)).get(part, 0)
            if base_score == 0:
                send_message(group_wxid, f"{get_member_nick(group_wxid, member_wxid)} 输入不符合任务设置。请检查输入")
                return
            mai_type = f"p8" if msg_content.startswith("买8") else f"p9"
            mai_content = f"{mai_type} {part}"
            # 如果是买9，base_score要额外-500，因为我们规定买9要在买8下面
            if mai_type == "p8":
                base_score = base_score - 200
            if mai_type == "p9":
                base_score = base_score - 1000
            group_config = get_group_config(redis_conn, group_wxid)
            hosts_config = get_group_hosts_config(redis_conn, group_wxid, current_hour)
            limit_koupai = int(group_config.get("limit_koupai", 0))
            # 添加并获取被t出去的成员
            exit_member = add_with_timestamp(redis_conn, group_wxid, member_wxid, current_hour = (current_hour+1)%24, base_score = base_score, msg_content = mai_content, limit_koupai = limit_koupai, mai_type = mai_type)
            
            if exit_member:
                send_message(group_wxid, f"{at_user(member_wxid)} {mai_content} 顶 {at_user(exit_member)}")
            # 更新买89后打印当前麦序
            tasks_members = get_group_task_members(redis_conn, group_wxid, current_hour)
            hosts_config = get_group_config(redis_conn, group_wxid).get("hosts_config", {})
            maixu_desc = "\r".join(
                f"{i+1}. @{get_member_nick(group_wxid, member)}({koupai_type})"
                for i, (member, koupai_type, score, state, _, _, _) in enumerate(tasks_members)
            )
            
            send_message(group_wxid, f"主持:{hosts_config.get('host_desc', '')}\r"
                                     f"时间:{current_hour+1}-{current_hour+2}\r"
                                     f"{maixu_desc}\r"
                                     f" {emoji_map.get('full', '')}\r"
                                     f"当前已满 可扣任务")

    except Exception as e:
        logger.error(f"添加成员{member_wxid}到买89任务列表{group_wxid}时出错: {e}")

@celery_app.task
def add_daizou_member(group_wxid:str, member_wxid:str):
    """
    添加“带走”指定群组的成员到待扣任务列表中。
    该成员必须在扣排任务列表中。
    """
    try:
        redis_conn = get_redis_connection(0)
        current_hour = datetime.now().hour
        task_members = get_group_task_members(redis_conn, group_wxid, current_hour, with_daizou = False)
        # 获取当前成员的扣排类型
        koupai_type = next((koupai_type for member, koupai_type, score, state, _, _, _ in task_members if member == member_wxid), None)
        if not koupai_type:
            send_message(group_wxid, f"{get_member_nick(group_wxid, member_wxid)} 不在当前扣牌列表中")
            return
        # 将 "带走" 拼接到扣排类型后面
        koupai_type = f"{koupai_type}:带走"
        add_with_timestamp(redis_conn, group_wxid, member_wxid, current_hour = (current_hour+1)%24, base_score = -1500, msg_content = koupai_type, limit_koupai = 0)
    except Exception as e:
        logger.error(f"添加成员{member_wxid}到待扣任务列表{group_wxid}时出错: {e}")

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
        member_type = next((koupai_type for member, koupai_type, score, state, _, _, _ in task_members if member == member_wxid), None)
        print(f"成员{member_wxid}的扣排类型: {member_type}")
        group_wxid_this = ""
        if not member_type:
            send_message(group_wxid, f"{get_member_nick(group_wxid, member_wxid)} 不在扣牌列表中")
            return
        if member_type in ['p', 'P', '排', "手速"] and p_qu == 0:
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
        # 如果现在是取排时间内，则不需要执行后续操作
        if redis_conn.sismember(f"tasks:launch_tasks:renwu_tasks_list", f"{group_wxid}:{(current_hour+1)%24}"):
            return
        # 取的时候要更新任务累积（减少）
        # 先尝试获取当前成员的任务累积
        # 当member_type可以转化为float类型的时候才可以执行更新任务累积
        try:
            print(f"member_type!!!: {member_type}")
            # 当member_type为p8或p9时，需要特殊处理（移除掉p8或p9）
            if member_type.startswith("p8") or member_type.startswith("p9"):
                member_type = member_type.replace("p8", "").replace("p9", "")
            float(member_type)
            current_score = redis_conn.hget(f"member_task:{group_wxid}:{member_wxid}", "accumulate_score")
            if current_score is None:
                current_score = 0
            else:
                current_score = float(current_score)
            # 更新任务累积（减少）
            update_member_task(redis_conn, group_wxid, member_wxid, accumulate_score = current_score - float(member_type))
        except ValueError:
            
            return
        
        # 获取当前成员的扣排信息
    except Exception as e:
        logger.error(f"删除成员{member_wxid}从扣牌任务列表{group_wxid}时出错: {e}")
@celery_app.task
def delete_koupai_members(group_wxid: str, current_hour: int, delete_count: str = "all", current_date: str = None, **kwargs):
    """
    从扣牌列表中删除指定数量的群组成员。
    消息内容一般为 设置麦序作废人数/本档作废/上档作废 的时候触发
    """
    try:
        redis_conn = get_redis_connection(0)
        if delete_count == "all":
            delete_count = 0
        delete_members(redis_conn, group_wxid, current_hour, delete_count, current_date)
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
        for (member, koupai_type, score, state, _, _, _) in current_maixu:
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
    current_date = datetime.now().strftime("%Y-%m-%d")
    # 获取群配置
    group_config = get_group_config(redis_conn, group_wxid)
    key = f"tasks:launch_tasks:{group_wxid}:{current_hour}:{current_date}"
    # 获取正分扣排人数
    koupai_count = redis_conn.zcount(key, 0, float('inf'))
    if koupai_count > limit_koupai:
        print(f"当前扣排人数: {koupai_count}, 限制人数: {limit_koupai}")
        # 获取分数最小的正分扣排人员
        postive_min_members = redis_conn.zrangebyscore(key, 0, float('inf'), start=0, num=koupai_count - limit_koupai)
        # 删除这些人员
        redis_conn.zrem(key, *postive_min_members)
        


    
@celery_app.task
def get_current_maixu(group_wxid: str, **kwargs):
    """
    查询当前麦序。
    """
    try:
        redis_conn = get_redis_connection(0)
        current_hour = datetime.now().hour
        current_maixu = get_group_task_members(redis_conn, group_wxid, current_hour)
        print(f"current_maixu: {current_maixu}")
        current_desc = "\r".join(
                f"{i+1}. @{get_member_nick(group_wxid, member)}({koupai_type})"
                for i, (member, koupai_type, score, state, _, _, _) in enumerate(current_maixu)
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
        bb_list = list(redis_conn.scan_iter(f"history:bb:{current_date}:{group_wxid}:{current_hour}:*"))
        print(f"当前小时报备列表: {bb_list}")
        # 获取还没回来的成员数量
        bb_sum = 0
        if bb_list:
            for bb_key in bb_list:
                is_timeout = redis_conn.hget(bb_key, "is_timeout")
                is_back = redis_conn.hget(bb_key, "is_back")
                if is_timeout == "0" and is_back == "0":
                    bb_sum += 1
        if bb_sum >= bb_limit:
            send_message(group_wxid, f"已超过报备人数\r当前设置报备人数:{bb_limit}人")
            return
        # 获取报备列表中当前小时成员的报备的次数
        bb_count = len(list(redis_conn.scan_iter(f"history:bb:{current_date}:{group_wxid}:{current_hour}:{member_wxid}:*")))
    
        # 检查是否超过了一小时内的报备次数
        if bb_in_hour <= bb_count:
            send_message(group_wxid, f"已超过每小时报备次数\r当前设置小时报备次数:{bb_in_hour}次")
            return
        # 加入报备列表
        # 计算过期时间（datetime格式）
        time_out = datetime.now() + timedelta(minutes=bb_time)
        create_time = datetime.now()

        key = f"history:bb:{current_date}:{group_wxid}:{current_hour}:{member_wxid}:{current_minute}"
        redis_conn.hset(key, mapping={
                    "group_wxid": group_wxid, "member_wxid": member_wxid, "msg_content": msg_content,
                    "create_time": str(create_time),"back_time": "",
                    "is_timeout": "0", "is_back": "0"})
        # 添加群组成员报备（如果存在则更新）
        asyncio.run(group_repo.add_group_member_bb(group_wxid=group_wxid, member_wxid=member_wxid, msg_content=msg_content, create_time=create_time,is_timeout=0))
        
        # 发送报备成功消息
        send_message(group_wxid, f"{at_user(member_wxid)}{bb_time}分钟之内回来，回厅再发一个“回”")
        # 将过期时间转为utc时间
        time_out_utc = time_out.utcfromtimestamp(time_out.timestamp())
        print(f"过期时间(UTC): {time_out_utc} 本地时间: {time_out}")

        # 将timeout任务添加到celery队列，到过期时间时候执行，并且指定task_id，方便我们控制
        send_timeout_message.apply_async(args=[group_wxid, member_wxid, msg_content, create_time, bb_timeout_desc, key], 
                                        eta=time_out_utc)
    except Exception as e:
        logger.error(f"添加报备时出错: {e}")

@celery_app.task
def send_timeout_message(group_wxid: str, member_wxid: str, msg_content: str, create_time: float, bb_timeout_desc: str, key: str, **kwargs):
    """
    发送报备超时消息
    """
    try:
        redis_conn = get_redis_connection(0)
        # 如果没回来，才发送超时消息
        is_back = redis_conn.hget(key, "is_back")
        if is_back == "0":
            # 标记为超时
            redis_conn.hset(key, mapping={
                        "is_timeout": "1", "is_back": "0"})
            
            asyncio.run(group_repo.add_group_member_bb(group_wxid=group_wxid, member_wxid=member_wxid, msg_content=msg_content, create_time=create_time, is_timeout=1))
            send_message(group_wxid, f"{at_user(member_wxid)}{bb_timeout_desc}")    
    except Exception as e:
        logger.error(f"发送报备超时消息时出错: {e}")
   
@celery_app.task
def delete_bb_member(group_wxid: str, member_wxid: str):
    """
    删除报备
    """
    try:
        print(f"删除报备: {group_wxid} {member_wxid}")
        redis_conn = get_redis_connection(0)
        # 先删除超时任务
        # result = AsyncResult(f"send_timeout_{group_wxid}_{member_wxid}")
        # result.revoke(terminate=True)

        current_hour = datetime.now().hour
        current_minute = datetime.now().minute
        current_date = datetime.now().strftime("%Y-%m-%d")
        # 获取列表
        bb_list = list(redis_conn.scan_iter(f"history:bb:{current_date}:{group_wxid}:{current_hour}:{member_wxid}:*"))
        # 如果没有，那再尝试获取上一小时的（因为时间可能跨小时了，注意0点）
        if not bb_list:
            # 如果当前小时是0点，上一小时就是23点
            if current_hour == 0:
                current_hour = 23
                current_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                current_hour -= 1
            bb_list = list(redis_conn.scan_iter(f"history:bb:{current_date}:{group_wxid}:{current_hour}:{member_wxid}:*"))
        if not bb_list:
            send_message(group_wxid, f"{at_user(member_wxid)}当前没有报备记录。")
            return
        # 遍历列表，找到距离当前时间最近的项，即依据分钟排序
        # 找出list中分钟最大的项，即最近的项
        last_key = max(bb_list, key=lambda x: int(x.split(":")[-1]))
        # 将其is_back设置为1
        redis_conn.hset(last_key, "is_back", "1")
        # 设置back_time为当前时间
        back_time = datetime.now()
        redis_conn.hset(last_key, "back_time", back_time.strftime("%H:%M"))

        bb_back_desc = redis_conn.hget(f"groups_config:{group_wxid}", "bb_back_desc")

        # 获取create_time
        create_time = datetime.strptime(redis_conn.hget(last_key, "create_time"), "%Y-%m-%d %H:%M:%S.%f")
        # 获取msg_content
        msg_content = redis_conn.hget(last_key, "msg_content")
        asyncio.run(group_repo.add_group_member_bb(group_wxid=group_wxid, member_wxid=member_wxid, msg_content=msg_content, create_time=create_time, back_time=back_time))
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
        date_last_hour = (datetime.now() - timedelta(hours=1)).strftime("%m-%d")
        date_last_hour_year = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d")
        last_hour = (current_hour - 1) % 24
        last_hour_config = redis_conn.hgetall(f"tasks:hosts_tasks_config:{group_wxid}:{last_hour}")
        if not last_hour_config:
            return f"{group_wxid}上小时没有场次。"
        # 复制上小时的场次到历史记录里
        # copy_to_history_task(redis_conn, group_wxid, date_last_hour_year)
        # 获取上小时的场次描述
        last_hour_group_desc = last_hour_config.get("host_desc", "")
        # 获取上小时的场次时间
        last_hour_start_schedule = int(last_hour_config.get("start_schedule", ""))
        last_hour_end_schedule = int(last_hour_config.get("end_schedule", ""))
        # 获取上场次的扣排麦序信息
        group_tasks_members = get_group_task_members(redis_conn, group_wxid, last_hour_start_schedule-1, last_hour_end_schedule, with_daizou=True)
        # 获取上场的扣排信息
        print(f"上小时场次时间: {last_hour_start_schedule} - {last_hour_end_schedule}")
        tasks_members = generate_task_members(group_tasks_members)
        
        # print(f"{last_hour_group_desc} 上场的扣排信息: {json.dumps(tasks_members, ensure_ascii=False)}")
        tasks_desc = "——麦序明细————"
        print(f"{last_hour_group_desc} 上场的扣排信息: {tasks_members}")
        for member_wxid, koupai_info in tasks_members.items():
            # 转化为 @昵称[扣排次数] 扣排详情(仅需要扣排类型)
            nick_name = get_member_nick(group_wxid, member_wxid) or member_wxid
            print(f"{nick_name} 扣排信息: {koupai_info}")
            tasks_desc += f"\r@{nick_name} [{len(koupai_info)}]  {'+'.join([item[0] for item in koupai_info])}"
        send_message(group_wxid, f"{emoji_map.get('schedule')} 打卡记录表\r"
                                f"主持: {last_hour_group_desc}\r"
                                f"日期: {date_last_hour}\r"
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

@celery_app.task
def send_task_schedule_day(group_wxid: str, start_hour: int = None, end_hour: int = None, date: str = None):
    """
    发送今日麦序记录（可选时间范围）
    
    """
    try:
        print(f"发送今日麦序记录（可选时间范围）: {group_wxid} {start_hour} {end_hour} {date}")
        redis_conn = get_redis_connection(0)
        tasks_members = {}
        # 获取今日的场次时间
        start_hour_this = start_hour or 0
        end_hour_this = end_hour or 24
        tasks_members = {}
        if date:
            group_tasks_members = group_repo.get_group_members_tasks_by_date(group_wxid, date, start_hour_this, end_hour_this)
        else:
            group_tasks_members = get_group_task_members(redis_conn, group_wxid, start_hour_this-1, end_hour_this, with_daizou=True)  

        # 生成字典，key为成员id，value为扣排列表
        tasks_members = generate_task_members(group_tasks_members)
        # 生成的列表 仅需要@昵称：（次数）
        task_desc = "\r".join([f"@{get_member_nick(group_wxid, member_wxid)} [{len(koupai_info)}]" for member_wxid, koupai_info in tasks_members.items()])

        date_desc = f"日期: {date}" if date != None else "今日"
        header = f"{date_desc}{start_hour}-{end_hour}场麦序统计" if (start_hour != None and end_hour != None) else f"{date_desc}麦序统计"
        send_message(group_wxid, f"{header}\r"
                                 f"{task_desc}"
                    
                    )

    except Exception as e:
        logger.error(f"发送今日麦序记录时出错: {e}")

@celery_app.task
def save_task_schedule_day_history(cleanup: bool = True):
    """
    存储今日麦序记录到历史记录
    并且清空redis内的launch_tasks缓存
    """
    try:
        redis_conn = get_redis_connection(0)
        # 存储今日麦序记录到历史记录，所以使用前一天日期
        date = (datetime.now() - timedelta(hours=2)).strftime("%Y-%m-%d")
        # 获取launch_tasks的所有key
        launch_tasks_keys_yesterday = redis_conn.scan_iter(f"tasks:launch_tasks:{date}:*")
        
        tasks_members = []
        for key in launch_tasks_keys_yesterday:
            print(f"key: {key}")
            # 拆分hour
            hour = key.split(":")[-1]
            # 拆分group_wxid(即倒数第二位)
            group_wxid = key.split(":")[-2]
            # 获取所有member
            tasks = redis_conn.zrange(key, 0, -1)
            # 格式化tasks_members  (group_wxid, member_wxid, koupai_type, koupai_desc, hour, date)
            tasks_members.extend(
                (
                    group_wxid,
                    member.split(":")[0], 
                    member.split(":")[1],
                    member.split(":")[2] if len(member.split(":")) > 2 else "",
                    hour,
                    date
                ) for member in tasks
            )
        
        # 批量添加到数据库
        if tasks_members:
            print(f"tasks_members: {tasks_members}")
            asyncio.run(group_repo.add_group_members_tasks(tasks_members))
            logger.info(f"已添加 {len(tasks_members)} 条群成员扣排记录到数据库")
        else:
            logger.info("暂无群成员扣排记录，无需添加")
        
        if cleanup:
            # 清空所有7天前的launch_tasks缓存
            date_7_days_ago = datetime.now() - timedelta(days=7)
            expired_date = date_7_days_ago.strftime("%Y-%m-%d")
            # 获取所有launch_tasks缓存的键值
            launch_tasks_all_keys = redis_conn.scan_iter("tasks:launch_tasks:*")
            # 获取所有bb缓存的键值
            bb_all_keys = redis_conn.scan_iter("history:bb:*")

            all_keys = list(launch_tasks_all_keys) + list(bb_all_keys)
            keys_to_delete = []

            for key in all_keys:
                parts = key.split(":")
                # 拆分date
                if len(parts) > 3:
                    key_date = parts[2]
                    print(f"key_date: {key_date}")
                else:
                    continue
                # 转换为datetime对象
                key_date_dt = datetime.strptime(key_date, "%Y-%m-%d")
                # 如果日期早于7天前，加入删除列表
                if key_date_dt < date_7_days_ago:
                    keys_to_delete.append(key)
            if keys_to_delete:
                redis_conn.delete(*keys_to_delete)
                logger.info(f"已清空 {len(keys_to_delete)} 个 launch_tasks 缓存")
        else:
            logger.info("launch_tasks 缓存为空，无需清空")
    except Exception as e:
        logger.error(f"清空redis内的launch_tasks缓存时出错: {e}")