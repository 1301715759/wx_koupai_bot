from celery import Celery
import os
from datetime import datetime
from pydantic import BaseModel
from datetime import timedelta, timezone
from celery.schedules import crontab
import redis
class TaskResult(BaseModel):
    task_id: str
    group_wxid: str
    status: str
    result: dict = None

# 使用Redis作为broker和result backend
celery_app = Celery(
    'task_scheduler',
    broker='redis://127.0.0.1:6379/0',      # Redis broker
    backend='redis://127.0.0.1:6379/1',     # Redis结果存储  我们不需要存储结果
    include=['celery_tasks.schedule_tasks']
)
# 配置Celery
celery_app.conf.update(
    task_serializer='json',     # 序列化方式为json
    accept_content=['json'],    # 接收参数为json
    result_serializer='json',   # 输出序列化方式为json
    timezone='Asia/Shanghai',
    worker_pool='eventlet',     #使用eventlet线程池
    worker_concurrency=10,     # eventlet可以设置更高的并发
    enable_utc=False,           # 禁用UTC时间，使用本地时间
    
)
celery_app.conf.beat_schedule = {
    'check-hosts-schedule-every-1-minutes': {
        'task': 'celery_tasks.schedule_tasks.scheduled_task',
        'schedule': crontab(minute='*'),  # 每分钟的第二秒执行一次
        
    },
        'cleanup-results-hourly': {
        'task': 'celery_app.cleanup_expired_results',
        'schedule': 3600.0,  # 每小时执行一次
        
    },
}

@celery_app.task
def cleanup_expired_results():
    """
    清理过期的任务结果
    """
    import redis
    
    redis_conn = redis.Redis(
        host='127.0.0.1',
        port=6379,
        db=1,
        decode_responses=True
    )
    
    # Celery 任务结果键的格式
    pattern = "celery-task-meta-*"
    
    deleted_count = 0
    expired_before = datetime.now(timezone.utc) - timedelta(hours=1)  # 1小时前
    print(f"清理过期任务结果，过期时间: {expired_before.isoformat()}")
    for key in redis_conn.scan_iter(match=pattern, count=1000):
        try:
            # 获取任务元数据
            task_meta = redis_conn.get(key)
            if task_meta:
                import json
                meta = json.loads(task_meta)
                
                # 检查任务完成时间
                if 'date_done' in meta and meta['date_done']:
                    done_time = datetime.fromisoformat(
                        meta['date_done']
                    )
                    
                    # 如果超过1小时，删除
                    if done_time < expired_before:
                        redis_conn.delete(key)
                        deleted_count += 1
                        
        except Exception as e:
            print(f"清理键 {key} 失败: {e}")
            continue
    print(f"清理过期任务结果完成，删除 {deleted_count} 条记录")
    return {
        'deleted_count': deleted_count,
        'timestamp': datetime.now().isoformat()
    }



celery_app.control.purge()