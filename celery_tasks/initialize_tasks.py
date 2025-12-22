import redis
import sys
from pathlib import Path
import json
import logging
# 将项目根目录加入 sys.path，确保能找到 db 模块
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# 现在可以安全导入 db 模块
from db.database import db_manager
from db.repository import group_repo, command_repo
from cache.redis_pool import get_redis_connection
import asyncio
from celery_tasks.schedule_tasks import scheduled_task



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InitializeTasks:
    def __init__(self):
        self.redis_client = get_redis_connection(0)
        self.db_manager = db_manager
        self.GROUPS_CONFIG_KEY = "groups_config"
        self.HOSTS_TASK_CONFIG_KEY = "tasks:hosts_tasks_config"
        logger.info(f"初始化任务类{__class__.__name__}")
    async def load_from_database(self):
        """从数据库加载所有组配置和主机信息"""
        try:
            # 加载所有组配置
            groups_configs = await group_repo.get_all_active_groups_info()
            print(f"从数据库加载的组配置: {groups_configs}")
            # 加载所有主持信息
            hosts_schedules = await group_repo.get_all_hosts()
            
            # print(f"从数据库加载的组配置: {groups_configs}")
            # groups_configs 
            # 字段顺序：group_id, start_koupai, end_koupai, end_renwu, limit_koupai, verify_mode
            # hosts_schedules 
            # 字段顺序：group_wxid, start_hour, end_hour, host_desc, lianpai_desc

            # 将元组转换为字典列表，方便后续处理
            
            formatted_configs = []
            for config in groups_configs:
                formatted_configs.append({
                    "group_wxid": config[0],
                    "start_koupai": config[1],
                    "end_koupai": config[2],
                    "end_renwu": config[3],
                    "limit_koupai": config[4],
                    "verify_mode": config[5],
                    "maixu_desc": config[6],
                    "welcome_msg": config[7],
                    "exit_msg": config[8],
                    "renwu_desc": config[9],
                })
            formatted_hosts = []
            for host in hosts_schedules:
                formatted_hosts.append({
                    "group_wxid": host[0],
                    "start_hour": host[1],
                    "host_desc": host[3],
                    "lianpai_desc": host[4],
                    "current_koupai_sum": 0,
                    "current_koupai_info": "",
                })
            tasks_groups = [host["group_wxid"] for host in formatted_hosts]
            logger.info(f"格式化后的任务群组: {tasks_groups}")
            # print(f"格式化后的组配置: {formatted_hosts}")
            for task in formatted_configs:
                self.redis_client.hset(f"{self.GROUPS_CONFIG_KEY}:{task['group_wxid']}", mapping=task)
            for host in formatted_hosts:
                self.redis_client.hset(f"{self.HOSTS_TASK_CONFIG_KEY}:{host['group_wxid']}:{host['start_hour']}", mapping=host)
            # 存储任务群组到集合
            for group_wxid in tasks_groups:
                self.redis_client.sadd(f"{self.GROUPS_CONFIG_KEY}:koupai_groups", group_wxid)

            # 初始化完redis存储后，立即执行检查任务到任务列表是否存在
            scheduled_task.delay()
        except Exception as e:
            print(f"从数据库加载组配置失败: {e}")
    
    async def update_groups_config(self, group_wxid: str, config: dict):
        """更新指定群组的配置"""
        self.redis_client.hset(f"{self.GROUPS_CONFIG_KEY}:{group_wxid}", mapping=config)
        print(f"已更新群组 {group_wxid} 的配置{config}")
    async def add_koupai_groups(self, group_wxid: str):
        """添加指定群组到开牌群组集合"""
        self.redis_client.sadd(f"{self.GROUPS_CONFIG_KEY}:koupai_groups", group_wxid)
        print(f"已添加群组 {group_wxid} 到开扣群组集合")
    async def update_groups_tasks(self, group_wxid: str, host_schedules: tuple):
        """更新指定群组的所有任务"""
        #添加前先清除旧任务
        self.clear_groups_tasks(group_wxid, clear_config=False)
        formatted_hosts = []
        for host in host_schedules:
            formatted_hosts.append({
                "group_wxid": group_wxid,
                "start_hour": host[1],
                "host_desc": host[0],
                "lianpai_desc": "",
                "current_koupai_sum": 0,
                "current_koupai_info": "",
            })
        for host in formatted_hosts:
            self.redis_client.hset(f"{self.HOSTS_TASK_CONFIG_KEY}:{host['group_wxid']}:{host['start_hour']}", mapping=host)
        print(f"已添加群组 {group_wxid} 的所有任务")
    async def clear_groups_tasks(self, group_wxid: str, clear_config: bool = False, clear_tasks: bool = True):
        """清除指定群组的所有任务"""
        # 1. 扫描出所有符合前缀的 key
        config_keys = [k for k in self.redis_client.scan_iter(match=f"{self.GROUPS_CONFIG_KEY}:{group_wxid}:*")] if clear_config else []
        task_keys   = [k for k in self.redis_client.scan_iter(match=f"{self.HOSTS_TASK_CONFIG_KEY}:{group_wxid}:*")] if clear_tasks else []
        all_keys    = config_keys + task_keys

        # 2. 批量删除
        if all_keys:
            self.redis_client.delete(*all_keys)
        logger.info(f"已清空 {len(all_keys)} 条任务相关 key")
        logger.info(f"群组 {group_id} 的所有任务已清除")

    async def clear_all_tasks(self):
        """清除所有任务"""
        """清空 Redis 中所有以 GROUPS_CONFIG_KEY / GROUPS_TASK_KEY 为前缀的哈希，而不是只删顶层 key"""
        # 1. 扫描出所有符合前缀的 key
        config_keys = [k for k in self.redis_client.scan_iter(match=f"{self.GROUPS_CONFIG_KEY}:*")]
        task_keys   = [k for k in self.redis_client.scan_iter(match=f"{self.HOSTS_TASK_CONFIG_KEY}:*")]
        all_keys    = config_keys + task_keys

        # 2. 批量删除
        if all_keys:
            self.redis_client.delete(*all_keys)
        logger.info(f"已清空 {len(all_keys)} 条任务相关 key")
        logger.info("所有任务已清除")
        
        
# 初始化任务
initialize_tasks = InitializeTasks()

# 只有在直接运行此脚本时才执行初始化
if __name__ == "__main__":
    asyncio.run(initialize_tasks.load_from_database())
