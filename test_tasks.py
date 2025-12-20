from celery_tasks.schedule_tasks import set_koupai_tasks
from common.koupai import KoupaiModel
import json

# 将KoupaiModel对象转换为字典传递给任务
res = set_koupai_tasks.delay(KoupaiModel(group_wxid="42973360766@chatroom", start_time=30).dict())
print(f"开牌时间设置任务1 ID: {res.id}")

res2 = set_koupai_tasks.delay(KoupaiModel(group_wxid="32973360766@chatroom1", start_time=41, limit=100).dict())
print(f"开牌时间设置任务2 ID: {res2.id}")

res3 = set_koupai_tasks.delay(KoupaiModel(group_wxid="52973360766@chatroom3", start_time=42, end_time=50).dict())
print(f"开牌时间设置任务3 ID: {res3.id}")
