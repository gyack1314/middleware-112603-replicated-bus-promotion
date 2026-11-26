"""状态模型与仅追加持久化。

设计原则：
- 接受事实与副本证据是两类状态。写入一旦被主站受理，就固定
  (epoch, position, digest)，副本到达与否只影响副本证据和确认级别。
- 所有状态都可由事件日志重放重建；日志每行一个 JSON 事件并 fsync。
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Any, Optional

# 流策略
REGIONAL = "regional"  # 较严格：确认至少两个指定区域保存同一内容
LOCAL = "local"        # 较宽松：主站保管后即可返回仅主站结果

# 流生命周期阶段
ACTIVE = "active"            # 仍由当前主站写
PROMOTING = "promoting"      # 切换中：写入冻结，等待恢复或损失批准
PROMOTED = "promoted"        # 新主已取得发布资格

# 缺口状态
PENDING = "pending"              # 暂时不能找回，等待证据或损失批准
ANNOUNCED = "announced_gap"      # 已批准损失并对外宣布的缺口
RECOVERED = "recovered"          # 切换期间由候选区域找回
RECOVERED_LATE = "recovered_late_after_gap"  # 缺口宣布后才找回（只作历史事实）

# 确认级别
LEVEL_CONFIRMED = "confirmed"          # 已满足该流策略的持久保证
LEVEL_PRIMARY_ONLY = "primary_only"    # 仅主站保管
LEVEL_WAITING = "waiting"              # 已受理但尚未达到保证
LEVEL_GAP = "gap"                      # 已宣布的损失缺口

# 写回执状态（业务端据此区分三类结果）
RECEIPT_CONFIRMED = "confirmed"
RECEIPT_ACCEPTED = "accepted"
RECEIPT_WAITING = "waiting"

KNOWN_POLICIES = frozenset({REGIONAL, LOCAL})
KNOWN_PHASES = frozenset({ACTIVE, PROMOTING, PROMOTED})


class EngineError(Exception):
    """所有可预期的操作拒绝都带稳定 reason，供值班端追查。"""

    def __init__(self, reason: str, detail: str = ""):
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


@dataclass
class EntryState:
    """一个位置上的接受事实与该位置的副本证据。"""

    position: int
    epoch: int
    digest: str
    request_id: Optional[str]
    written_by: str                 # 受理它的区域（当时的主站）
    replicas: set[str] = field(default_factory=set)
    source_epochs: set[int] = field(default_factory=set)
    gap: Optional[str] = None       # None 表示不是缺口；否则取 PENDING/ANNOUNCED/...
    gap_approved_at: Optional[int] = None
    recovered_at: Optional[int] = None
    # 缺口宣布之后才到达的证据：只保留为历史事实，不复活该位置
    late_evidence: list[dict[str, Any]] = field(default_factory=list)

    @property
    def is_announced_gap(self) -> bool:
        return self.gap == ANNOUNCED

    @property
    def is_pending_gap(self) -> bool:
        return self.gap == PENDING

    def evidence_view(self) -> dict[str, Any]:
        return {
            "position": self.position,
            "epoch": self.epoch,
            "digest": self.digest,
            "written_by": self.written_by,
            "replicas": sorted(self.replicas),
            "gap": self.gap,
            "recovered_at": self.recovered_at,
            "gap_approved_at": self.gap_approved_at,
            "late_evidence": list(self.late_evidence),
        }


@dataclass
class StreamState:
    stream: str
    tenant: str
    primary: str
    secondary: str
    policy: str
    phase: str = ACTIVE
    active_epoch: int = 1
    # position -> EntryState；只增，切换后新写继续向后编号，绝不重用
    entries: dict[int, EntryState] = field(default_factory=dict)
    next_position: int = 1
    # 切换控制面
    promotion_epoch: Optional[int] = None
    promotion_candidate: Optional[str] = None
    promotion_opened_at: Optional[int] = None
    # 已对外确认过的最大位置（跨区确认的事实，损失批准不能触及它）
    highest_confirmed: int = 0
    # request_id -> position，保证同一生产请求重送只恢复原结果
    idempotency: dict[str, int] = field(default_factory=dict)
    # consumer -> {"through": int, "epoch": int, "updated_at": int}
    consumers: dict[str, dict[str, Any]] = field(default_factory=dict)
    conflicts: list[dict[str, Any]] = field(default_factory=list)
    audit: list[dict[str, Any]] = field(default_factory=list)

    def entry(self, position: int) -> EntryState:
        return self.entries[position]

    def ordered_entries(self) -> list[EntryState]:
        return [self.entries[p] for p in sorted(self.entries)]


class Storage:
    """数据目录上的仅追加 JSONL 日志。

    控制面与每个流各自一个文件，写入后逐条 fsync。重启时先重放控制
    日志、再按文件名排序重放各流日志，文件内保持行序（同一秒内按
    行序提交）。流标识经过转义，文件名本身不含路径分隔符。
    """

    CONTROL = "control.log"

    def __init__(self, directory: str):
        self.directory = directory
        os.makedirs(directory, exist_ok=True)
        self._handles: dict[str, Any] = {}

    @staticmethod
    def log_name(stream: str) -> str:
        return "stream-" + re.sub(r"[^A-Za-z0-9_.-]", "_", stream) + ".log"

    def _path(self, name: str) -> str:
        return os.path.join(self.directory, name)

    def _handle(self, name: str):
        handle = self._handles.get(name)
        if handle is None:
            handle = open(self._path(name), "a", encoding="utf-8")
            self._handles[name] = handle
        return handle

    def append(self, name: str, event: dict[str, Any]) -> None:
        handle = self._handle(name)
        handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())

    def replay(self):
        """按确定性顺序产出 (stream_or_None, event)。

        控制日志必须先于各流日志（流事件需要先有登记事实）；其余文件
        按文件名排序，文件内保持行序（同一秒内按行序提交）。
        """
        names = sorted(
            n for n in os.listdir(self.directory) if n.endswith(".log")
        )
        ordered = ([self.CONTROL] if self.CONTROL in names else []) + [
            n for n in names if n != self.CONTROL
        ]
        for name in ordered:
            with open(self._path(name), encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    event = json.loads(line)
                    yield (None if name == self.CONTROL else event["stream"]), event

    def close(self) -> None:
        for handle in self._handles.values():
            handle.close()
        self._handles.clear()
