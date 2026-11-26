"""本地仿真的多区域事件接入与灾备中间件。

只依赖标准库。核心入口是 :class:`drelay.engine.Engine`，它在一个持久化
数据目录上提供发布、跨区保存、切换、损失批准与消费接续五类操作。
"""

from .engine import Engine
from .model import (
    ACTIVE,
    ANNOUNCED,
    LOCAL,
    PENDING,
    PROMOTING,
    REGIONAL,
)

__all__ = [
    "Engine",
    "ACTIVE",
    "ANNOUNCED",
    "LOCAL",
    "PENDING",
    "PROMOTING",
    "REGIONAL",
]
