from pydantic import BaseModel
from typing import Optional
class KoupaiModel(BaseModel):
    # 群组微信ID
    group_wxid: str
    # 扣排时间，单位：分钟
    start_time: Optional[int|str] = ""
    # 关闭排时间，单位：分钟
    end_time: Optional[int|str] = ""
    # 限制人数
    limit: Optional[int|str] = ""
    # 当前人数
    current_count: Optional[int|str] = 0
    # 验证模式，可选值：
    verify_mode: str = "字符"
