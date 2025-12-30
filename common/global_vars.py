# 全局变量
enable_groups = []
baned_list = []

def set_enable_groups(groups: list):
    """设置全局变量enable_groups"""
    global enable_groups
    enable_groups = groups
def set_baned_list(members: list):
    """设置全局变量baned_list"""
    global baned_list
    baned_list = members
def get_baned_list() -> list:
    """获取全局变量baned_list"""
    return baned_list
def get_enable_groups() -> list:
    """获取全局变量enable_groups"""
    return enable_groups
def add_enable_group(group_wxid: str):
    """添加全局变量enable_groups"""
    enable_groups.append(group_wxid)
def remove_enable_group(group_wxid: str):
    """移除全局变量enable_groups"""
    enable_groups.remove(group_wxid)
def add_baned_member(group_wxid: str, member_wxid: str):
    """添加全局变量baned_list"""
    baned_list.append((group_wxid, member_wxid))
def remove_baned_member(group_wxid: str, member_wxid: str):
    """移除全局变量baned_list"""
    baned_list.remove((group_wxid, member_wxid))