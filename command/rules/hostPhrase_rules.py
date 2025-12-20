import re
from typing import List, Tuple, Optional, Union

def validate_time_slot_format(time_slot: str) -> Tuple[bool, Optional[str]]:
    """
    验证单个时间段字符串是否符合规范
    规范：数字1-数字2+其他非数字，且数字1 < 数字2
    参数:
        time_slot: 时间段字符串，如 "0-2a"
    返回:
        (是否有效, 错误信息或None)
    """
    # 使用正则表达式匹配格式
    # ^表示字符串开头，\d+表示至少一个数字，-是连字符，\d+表示至少一个数字，\D+表示至少一个非数字字符，$表示字符串结尾
    pattern = r'^(\d+)-(\d+)(\D+)$'
    match = re.match(pattern, time_slot)
    if not match:
        return False, f"格式错误: '{time_slot}' 不符合 '数字-数字+非数字' 格式"
    # 提取数字部分
    num1_str, num2_str, suffix = match.groups()
    # 确保数字是整数
    try:
        num1 = int(num1_str)
        num2 = int(num2_str)
    except ValueError:
        return False, f"数字格式错误: '{time_slot}' 中的时间不是有效的整数"
    # 验证数字1是否小于数字2
    if num1 >= num2:
        return False, f"数字顺序错误: '{time_slot}' 中第一个时间({num1})必须小于第二个时间({num2})"
    # 可以添加额外的验证，比如数字范围限制
    if num1 < 0 or num1 > 23 or num2 < 0 or num2 > 24:
        return False, f"数字范围错误: '{time_slot}' 每个对应时间范围应当为0-23的整数"
    return True, None

def validate_time_slots_array(time_slots: List[str]) -> Tuple[bool, Optional[str]]:
    """
    验证时间段数组是否符合规范
    参数:
        time_slots: 时间段字符串数组，如 ["0-2a", "2-4b"]
    返回:
        (是否有效, 错误信息或None)
    """
    if not time_slots:
        return False, "时间段数组不能为空"
    for time_slot in time_slots:
        is_valid, error_msg = validate_time_slot_format(time_slot)
        if not is_valid:
            return False, error_msg
    return True, None

def parse_time_slots(time_slots: List[str]) -> List[Tuple[str, int, int]]:
    """
    解析时间段字符串数组，返回数字范围和后缀
    参数:
        time_slots: 时间段字符串数组，如 ["0-2a", "2-4b"]
    返回:
        解析后的时间段列表，每个元素为 (非数字,数字1, 数字2)
    """
    # 先按起始数字从小到大排序
    time_slots_sorted = sorted(time_slots, key=lambda x: int(re.match(r'^(\d+)-', x).group(1)))
    parsed_slots = []
    last_end = -1          # 记录上一个区间的结束值
    for time_slot in time_slots_sorted:
        is_valid, error_msg = validate_time_slot_format(time_slot)
        if not is_valid:
            raise ValueError(error_msg)
        num1_str, num2_str, suffix = re.match(r'^(\d+)-(\d+)(\D+)$', time_slot).groups()
        num1 = int(num1_str)
        num2 = int(num2_str)
        # 检查时间交叉：当前起始必须大于等于上一个结束
        if num1 < last_end:
            raise ValueError(f"时间段存在重叠: '{time_slot}' 与之前的时间段出现重叠")
        for num in range(num1, num2):
            parsed_slots.append((suffix, num, num+1))
        last_end = num2
    return parsed_slots
