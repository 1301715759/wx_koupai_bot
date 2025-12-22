from celery import Celery
import os
from datetime import datetime
from pydantic import BaseModel
from datetime import timedelta
from celery.schedules import crontab
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
}
