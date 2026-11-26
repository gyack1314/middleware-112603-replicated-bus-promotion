"""事件接入与灾备中间件引擎。

五条可持续使用的入口：
- :meth:`Engine.write`         发布（按租户流依次确定 position）
- :meth:`Engine.copied`        跨区保存证据（乱序安全）
- :meth:`Engine.promote`       切换（两阶段，可重复调用以等待恢复）
- :meth:`Engine.approve_loss`  逐个租户/流/位置的损失批准
- :meth:`Engine.read`          消费者带实际处理位置与发布代次重连

不变量：
1. 单一流的 position 全局只增，切换后新写使用新发布代次且不重用旧位置。
2. 接受事实（entry）与副本证据（replicas）分离；副本乱序不改变接受事实。
3. 一切“连续边界”都由前缀扫描得到，最大 position 到达不代表前面每条存在。
4. 同一生产请求（request id）重送只恢复原结果，不重新分配位置。
5. 已对外跨区确认的数据不能被损失批准丢弃。
6. 旧主恢复后只能补历史事实：写权被代次 fencing，迟来副本不能把同一
   位置填成两种结果（digest 不符即冲突隔离）。
"""

from __future__ import annotations

import re
from typing import Any, Optional

from .model import (
    ANNOUNCED,
    EngineError,
    EntryState,
    LEVEL_CONFIRMED,
    LEVEL_GAP,
    LEVEL_PRIMARY_ONLY,
    LEVEL_WAITING,
    LOCAL,
    PENDING,
    PROMOTED,
    PROMOTING,
    RECEIPT_ACCEPTED,
    RECEIPT_CONFIRMED,
    RECEIPT_WAITING,
    RECOVERED,
    REGIONAL,
    Storage,
    StreamState,
)

_IDENTIFIER = re.compile(r"^[A-Za-z0-9_.\-:]+$")


def _identifier(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise EngineError("invalid_argument", f"{field} 必须是非空标识符")
    return value


def _positive_int(value: Any, field: str) -> int:
    if type(value) is not int or value < 1:
        raise EngineError("invalid_argument", f"{field} 必须是正整数")
    return value


class Engine:
    """在一个持久化数据目录上维护全部租户流的状态。"""

    def __init__(self, storage: Storage):
        self.storage = storage
        self.streams: dict[str, StreamState] = {}
        self.sequence = 0
        last_seq = 0
        for _stream, event in storage.replay():
            self._apply(event)
            last_seq = max(last_seq, event.get("seq", 0))
        self.sequence = last_seq

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------

    def _stream_required(self, stream: str) -> StreamState:
        _identifier(stream, "stream")
        state = self.streams.get(stream)
        if state is None:
            raise EngineError("unknown_stream", f"流 {stream} 尚未登记")
        return state

    def _emit(self, event: dict[str, Any], replaying: bool) -> None:
        if not replaying:
            self.sequence += 1
            event = dict(event)
            event["seq"] = self.sequence
            self.storage.append(
                Storage.CONTROL if event["type"].startswith("stream_")
                else Storage.log_name(event["stream"]),
                event,
            )
        self._apply(event)

    def _audit(self, state: StreamState, at: int, action: str,
               detail: dict[str, Any]) -> None:
        state.audit.append({"at": at, "action": action, **detail})

    # ------------------------------------------------------------------
    # 事件应用（重放与实时共用同一份状态迁移）
    # ------------------------------------------------------------------

    def _apply(self, event: dict[str, Any]) -> None:
        """纯状态迁移：实时提交与日志重放走同一条路径，审计由事件派生。"""
        kind = event["type"]
        if kind == "stream_registered":
            state = StreamState(
                stream=event["stream"],
                tenant=event["tenant"],
                primary=event["primary"],
                secondary=event["secondary"],
                policy=event["policy"],
                active_epoch=1,
            )
            self.streams[event["stream"]] = state
            self._audit(state, event["at"], "registered", {})
            return

        state = self.streams[event["stream"]]
        at = event["at"]

        if kind == "write_accepted":
            entry = EntryState(
                position=event["position"],
                epoch=event["epoch"],
                digest=event["digest"],
                request_id=event.get("request_id"),
                written_by=event["region"],
                replicas={event["region"]},
                source_epochs={event["epoch"]},
            )
            state.entries[event["position"]] = entry
            state.next_position = max(state.next_position,
                                      event["position"] + 1)
            if event.get("request_id"):
                state.idempotency[event["request_id"]] = event["position"]
            if event["policy"] == LOCAL:
                self._refresh_highest_confirmed(state)
            self._audit(state, at, "write_accepted",
                        {"position": event["position"],
                         "epoch": event["epoch"],
                         "request_id": event.get("request_id")})

        elif kind == "replica_recorded":
            entry = state.entries[event["position"]]
            entry.replicas.add(event["region"])
            entry.source_epochs.add(event["source_epoch"])
            if entry.gap == PENDING:
                entry.gap = RECOVERED
                entry.recovered_at = at
                self._audit(state, at, "gap_recovered",
                            {"position": entry.position,
                             "region": event["region"]})
            elif entry.gap == ANNOUNCED:
                entry.late_evidence.append({
                    "at": at, "region": event["region"],
                    "source_epoch": event["source_epoch"],
                    "digest": event["digest"],
                })
                self._audit(state, at, "late_evidence_after_gap",
                            {"position": entry.position,
                             "region": event["region"]})
            if state.policy == REGIONAL:
                self._refresh_highest_confirmed(state)

        elif kind == "replica_conflict":
            state.conflicts.append({
                "at": at,
                "position": event["position"],
                "expected_digest": event["expected_digest"],
                "received_digest": event["received_digest"],
                "region": event["region"],
                "source_epoch": event["source_epoch"],
            })
            self._audit(state, at, "replica_conflict",
                        {"position": event["position"],
                         "region": event["region"]})

        elif kind == "promotion_opened":
            state.phase = PROMOTING
            state.promotion_epoch = event["new_epoch"]
            state.promotion_candidate = event["candidate"]
            state.promotion_opened_at = at
            for position in sorted(state.entries):
                entry = state.entries[position]
                if entry.gap is not None:
                    continue
                if event["candidate"] not in entry.replicas:
                    entry.gap = PENDING
            self._audit(state, at, "promotion_opened",
                        {"new_epoch": event["new_epoch"],
                         "candidate": event["candidate"]})

        elif kind == "promotion_completed":
            old_primary = state.primary
            state.phase = PROMOTED
            state.active_epoch = event["new_epoch"]
            state.primary = event["candidate"]
            # 旧主降级为备用区域，保持“两个指定区域”的跨区语义不塌缩
            if state.secondary == event["candidate"]:
                state.secondary = old_primary
            state.promotion_epoch = None
            state.promotion_candidate = None
            self._audit(state, at, "promotion_completed",
                        {"new_epoch": event["new_epoch"],
                         "new_primary": event["candidate"],
                         "new_secondary": state.secondary})

        elif kind == "loss_approved":
            for position in event["positions"]:
                entry = state.entries[position]
                entry.gap = ANNOUNCED
                entry.gap_approved_at = at
            self._audit(state, at, "loss_approved",
                        {"positions": list(event["positions"]),
                         "operator": event.get("operator", "oncall")})

        elif kind == "consumer_advanced":
            current = state.consumers.get(event["consumer"])
            through = event["through"]
            if current is None or through > current["through"]:
                state.consumers[event["consumer"]] = {
                    "through": through,
                    "epoch": event["epoch"],
                    "updated_at": at,
                }
            self._audit(state, at, "consumer_advanced",
                        {"consumer": event["consumer"],
                         "through": through})

        elif kind == "operation_rejected":
            self._audit(state, at, "rejected",
                        {"reason": event["reason"],
                         "detail": event.get("detail", ""),
                         "op": event.get("op", "")})

        else:  # pragma: no cover - 新增事件必须显式处理
            raise EngineError("unknown_event", kind)

    # ------------------------------------------------------------------
    # 连续证据边界（前缀扫描，绝不取最大 position）
    # ------------------------------------------------------------------

    def _designated_regions(self, state: StreamState) -> set[str]:
        return {state.primary, state.secondary}

    def _has_durable_evidence(self, state: StreamState,
                              entry: EntryState) -> bool:
        if state.policy == REGIONAL:
            return self._designated_regions(state).issubset(entry.replicas)
        return state.primary in entry.replicas

    def _refresh_highest_confirmed(self, state: StreamState) -> None:
        frontier = 0
        for position in sorted(state.entries):
            entry = state.entries[position]
            if entry.gap is not None:
                break
            if self._has_durable_evidence(state, entry):
                frontier = position
            else:
                break
        state.highest_confirmed = frontier

    def _candidate_frontier(self, state: StreamState,
                            candidate: Optional[str]) -> dict[str, int]:
        """各区域（以及候选区域）连续持有证据的边界。"""
        regions = sorted({state.primary, state.secondary,
                          *(candidate for candidate in (candidate,)
                            if candidate)})
        view: dict[str, int] = {}
        for region in regions:
            frontier = 0
            for position in sorted(state.entries):
                entry = state.entries[position]
                if entry.gap == ANNOUNCED:
                    break
                if region in entry.replicas:
                    frontier = position
                else:
                    break
            view[region] = frontier
        return view

    def _confirmed_frontier(self, state: StreamState) -> int:
        frontier = 0
        for position in sorted(state.entries):
            entry = state.entries[position]
            if entry.gap is not None:
                break
            if self._has_durable_evidence(state, entry):
                frontier = position
            else:
                break
        return frontier

    def _cross_region_frontier(self, state: StreamState) -> int:
        """曾在两个指定区域同时保存同一内容的连续边界。

        这是“曾经对外称跨区确认”的判定依据，与策略无关；local 流里
        恰好已复制的部分同样受保护，不能被损失批准丢弃。
        """
        frontier = 0
        designated = self._designated_regions(state)
        for position in sorted(state.entries):
            entry = state.entries[position]
            if entry.gap is not None:
                break
            if designated.issubset(entry.replicas):
                frontier = position
            else:
                break
        return frontier

    def _entry_level(self, state: StreamState,
                     entry: EntryState) -> str:
        """确认级别是拓扑事实：这个位置当前由哪些区域实际保管。

        - confirmed：两个指定区域都有同一内容（跨区确认）
        - primary_only：仅当前主站保管
        - waiting：主站保管事实之外的中间态（切换待决等）
        - gap：已宣布的损失缺口
        流策略决定写回执能否在 primary_only 时就对业务返回 accepted，
        但不改变这里的级别含义。
        """
        if entry.gap == ANNOUNCED:
            return LEVEL_GAP
        designated = self._designated_regions(state)
        if designated.issubset(entry.replicas):
            return LEVEL_CONFIRMED
        if state.primary in entry.replicas:
            return LEVEL_PRIMARY_ONLY
        return LEVEL_WAITING

    def _receipt(self, state: StreamState, entry: EntryState,
                 deduplicated: bool, at: int) -> dict[str, Any]:
        level = self._entry_level(state, entry)
        if level == LEVEL_CONFIRMED:
            # 双区持有同一内容，任何策略下都是满足要求的持久确认
            status = RECEIPT_CONFIRMED
        elif level == LEVEL_PRIMARY_ONLY:
            # 仅主站保管：宽松流可先受理，严格流只能等待
            status = (RECEIPT_ACCEPTED if state.policy == LOCAL
                      else RECEIPT_WAITING)
        else:
            status = RECEIPT_WAITING
        return {
            "ok": True,
            "stream": state.stream,
            "tenant": state.tenant,
            "position": entry.position,
            "epoch": entry.epoch,
            "digest": entry.digest,
            "request_id": entry.request_id,
            "level": level,
            "status": status,
            "deduplicated": deduplicated,
            "at": at,
        }

    # ------------------------------------------------------------------
    # 1) 流登记
    # ------------------------------------------------------------------

    def register_stream(self, at: int, stream: str, tenant: str,
                        primary: str, secondary: str,
                        policy: str) -> dict[str, Any]:
        _identifier(stream, "stream")
        _identifier(tenant, "tenant")
        _identifier(primary, "primary")
        _identifier(secondary, "secondary")
        if policy not in (REGIONAL, LOCAL):
            raise EngineError("invalid_argument",
                              f"policy 只能是 {REGIONAL} 或 {LOCAL}")
        if primary == secondary:
            raise EngineError("invalid_argument", "主备区域不能相同")
        if stream in self.streams:
            raise EngineError("stream_exists", f"流 {stream} 已登记")
        event = {
            "type": "stream_registered", "at": at, "stream": stream,
            "tenant": tenant, "primary": primary,
            "secondary": secondary, "policy": policy,
        }
        self._emit(event, replaying=False)
        return {"ok": True, "stream": stream, "tenant": tenant,
                "policy": policy, "primary": primary,
                "secondary": secondary, "epoch": 1, "at": at}

    # ------------------------------------------------------------------
    # 2) 发布
    # ------------------------------------------------------------------

    def write(self, at: int, stream: str, region: str, epoch: int,
              request_id: Optional[str], digest: str) -> dict[str, Any]:
        state = self._stream_required(stream)
        _identifier(region, "region")
        _positive_int(epoch, "epoch")
        _identifier(digest, "digest")
        if request_id is not None:
            _identifier(request_id, "id")

        if state.phase == PROMOTING:
            raise EngineError(
                "promotion_in_progress",
                f"流 {stream} 正在切换到 {state.promotion_candidate}，写入冻结",
            )

        # 同一生产请求重送：无论从哪个区域/代次重送，都只恢复原结果。
        if request_id is not None and request_id in state.idempotency:
            original = state.entries[state.idempotency[request_id]]
            receipt = self._receipt(state, original, True, at)
            receipt["original_status"] = receipt["status"]
            return receipt

        # 代次 fencing 优先：旧主即使区域曾经合法，旧代次也必须被拒绝，
        # 这样值班端能区分“旧主迟报”与“陌生区域冒写”。
        if epoch != state.active_epoch:
            raise EngineError(
                "stale_epoch",
                f"代次 {epoch} 已失效，当前发布代次 {state.active_epoch}",
            )
        if region != state.primary:
            raise EngineError(
                "not_primary",
                f"区域 {region} 不是流 {stream} 的当前主站（主站 {state.primary}）",
            )

        position = state.next_position
        event = {
            "type": "write_accepted", "at": at, "stream": stream,
            "region": region, "epoch": epoch, "position": position,
            "digest": digest, "request_id": request_id,
            "policy": state.policy,
        }
        self._emit(event, replaying=False)
        return self._receipt(state, state.entries[position], False, at)

    # ------------------------------------------------------------------
    # 3) 跨区保存证据
    # ------------------------------------------------------------------

    def copied(self, at: int, stream: str, region: str,
               source_epoch: int, position: int,
               digest: str) -> dict[str, Any]:
        state = self._stream_required(stream)
        _identifier(region, "region")
        _positive_int(source_epoch, "source_epoch")
        _positive_int(position, "position")
        _identifier(digest, "digest")

        entry = state.entries.get(position)
        if entry is None:
            raise EngineError("unknown_position",
                              f"流 {stream} 位置 {position} 没有接受事实")
        if entry.epoch != source_epoch:
            raise EngineError(
                "epoch_mismatch",
                f"位置 {position} 属于代次 {entry.epoch}，"
                f"证据声称源代次 {source_epoch}",
            )
        if entry.digest != digest:
            # 同一位置不允许两种结果：冲突隔离，证据不进入副本集合。
            event = {
                "type": "replica_conflict", "at": at, "stream": stream,
                "region": region, "source_epoch": source_epoch,
                "position": position, "expected_digest": entry.digest,
                "received_digest": digest,
            }
            self._emit(event, replaying=False)
            raise EngineError(
                "digest_conflict",
                f"位置 {position} 内容冲突：已受理 {entry.digest}，"
                f"证据 {digest}",
            )

        already = region in entry.replicas
        if not already:
            self._emit({
                "type": "replica_recorded", "at": at, "stream": stream,
                "region": region, "source_epoch": source_epoch,
                "position": position, "digest": digest,
            }, replaying=False)

        entry = state.entries[position]
        return {
            "ok": True, "stream": stream, "position": position,
            "epoch": entry.epoch, "region": region,
            "level": self._entry_level(state, entry),
            "duplicate": already,
            "confirmed_frontier": self._confirmed_frontier(state),
            "candidate_frontier": self._candidate_frontier(
                state, state.promotion_candidate),
            "at": at,
        }

    # ------------------------------------------------------------------
    # 4) 切换
    # ------------------------------------------------------------------

    def promote(self, at: int, stream: str, region: str,
                new_epoch: int) -> dict[str, Any]:
        state = self._stream_required(stream)
        _identifier(region, "region")
        _positive_int(new_epoch, "new_epoch")

        if state.phase == PROMOTED and state.primary == region \
                and state.active_epoch == new_epoch:
            return self._promotion_view(state, at, already_complete=True)

        if state.phase == PROMOTING:
            if region != state.promotion_candidate or \
                    new_epoch != state.promotion_epoch:
                raise EngineError(
                    "promotion_conflict",
                    f"流 {stream} 已有切换 "
                    f"{state.promotion_candidate}/代次{state.promotion_epoch}",
                )
            # 同一接管的重复调用：检查缺口是否已恢复/批准，满足则完成。
        else:
            if new_epoch <= state.active_epoch:
                raise EngineError(
                    "invalid_epoch",
                    f"新代次 {new_epoch} 必须大于当前代次 {state.active_epoch}",
                )
            self._emit({
                "type": "promotion_opened", "at": at, "stream": stream,
                "candidate": region, "new_epoch": new_epoch,
            }, replaying=False)

        pending = [e.position for e in state.ordered_entries()
                   if e.is_pending_gap]
        if not pending:
            self._emit({
                "type": "promotion_completed", "at": at,
                "stream": stream, "candidate": region,
                "new_epoch": new_epoch,
            }, replaying=False)
        return self._promotion_view(state, at)

    def _promotion_view(self, state: StreamState, at: int,
                        already_complete: bool = False) -> dict[str, Any]:
        pending = [e.position for e in state.ordered_entries()
                   if e.is_pending_gap]
        announced = [e.position for e in state.ordered_entries()
                     if e.is_announced_gap]
        return {
            "ok": True,
            "stream": state.stream,
            "tenant": state.tenant,
            "phase": state.phase,
            "candidate": state.promotion_candidate or state.primary,
            "new_epoch": state.promotion_epoch or state.active_epoch,
            "active_epoch": state.active_epoch,
            "complete": state.phase == PROMOTED,
            "already_complete": already_complete,
            "last_strong_frontier": state.highest_confirmed,
            "pending_gaps": pending,
            "announced_gaps": announced,
            "candidate_frontier": self._candidate_frontier(
                state, state.promotion_candidate),
            "at": at,
        }

    # ------------------------------------------------------------------
    # 5) 损失批准（逐租户、逐流、逐位置，可追查）
    # ------------------------------------------------------------------

    def approve_loss(self, at: int, stream: str, span: list[int],
                     operator: str = "oncall") -> dict[str, Any]:
        state = self._stream_required(stream)
        if (not isinstance(span, (list, tuple)) or len(span) != 2
                or any(type(n) is not int for n in span)):
            raise EngineError("invalid_argument", "span 必须是 [起, 止]")
        start, end = span
        if start < 1 or end < start:
            raise EngineError("invalid_argument", "span 范围非法")

        if state.phase != PROMOTING:
            raise EngineError(
                "not_promoting",
                "损失批准只能在切换期间、对仍待决的缺口作出",
            )

        positions = list(range(start, end + 1))
        missing = [p for p in positions if p not in state.entries]
        if missing:
            raise EngineError("unknown_position",
                              f"位置 {missing} 没有接受事实")

        # regional 流不得用损失清理未达约定的已受理写。
        if state.policy == REGIONAL:
            raise EngineError(
                "loss_forbidden_regional",
                "regional 流的已受理写必须先补齐，或由源端显式放弃尚未受理的写",
            )

        ineligible: list[dict[str, Any]] = []
        cross_region_frontier = self._cross_region_frontier(state)
        for position in positions:
            entry = state.entries[position]
            if not entry.is_pending_gap:
                ineligible.append({
                    "position": position,
                    "reason": "not_pending",
                    "gap": entry.gap,
                    "replicas": sorted(entry.replicas),
                })
            elif position <= cross_region_frontier:
                # 曾经对外称跨区确认的数据不能被批准丢弃。
                ineligible.append({
                    "position": position,
                    "reason": "was_cross_region_confirmed",
                })
        if ineligible:
            raise EngineError("span_not_approvable",
                              f"以下位置不能批准损失：{ineligible}")

        self._emit({
            "type": "loss_approved", "at": at, "stream": stream,
            "positions": positions, "operator": operator,
        }, replaying=False)
        return {
            "ok": True, "stream": state.stream,
            "tenant": state.tenant, "positions": positions,
            "operator": operator,
            "announced_gaps": [e.position for e in state.ordered_entries()
                               if e.is_announced_gap],
            "at": at,
        }

    # ------------------------------------------------------------------
    # 6) 消费接续
    # ------------------------------------------------------------------

    def read(self, at: int, stream: str, consumer: str, after: int,
             epoch_seen: int, max_items: int = 100) -> dict[str, Any]:
        state = self._stream_required(stream)
        _identifier(consumer, "consumer")
        if type(after) is not int or after < 0:
            raise EngineError("invalid_argument", "after 必须是非负整数")
        _positive_int(epoch_seen, "epoch")
        _positive_int(max_items, "max_items")

        # 已实际消费的事实由服务端保存，不能替消费端声称退回未处理。
        known = state.consumers.get(consumer)
        consumed_through = max(after, known["through"]) if known else after
        epoch_behind = epoch_seen < state.active_epoch

        entries: list[dict[str, Any]] = []
        announced_gaps: list[int] = []
        pending_gaps: list[int] = []
        for entry in state.ordered_entries():
            # 缺口边界包含 after 本身：客户端带着 after 重连时，必须能
            # 得知自己手里该位置的数据已在新主被宣布缺失。
            if entry.position < after:
                continue
            if entry.gap == ANNOUNCED:
                announced_gaps.append(entry.position)
                continue
            if entry.gap == PENDING:
                pending_gaps.append(entry.position)
                continue
            # 实际处理过的正常条目不重复发布
            if entry.position <= consumed_through:
                continue
            # 切换完成前，候选区域没有证据的位置不能由新主发布。
            if state.phase == PROMOTING and \
                    state.promotion_candidate not in entry.replicas:
                continue
            if len(entries) >= max_items:
                break
            entries.append({
                "position": entry.position,
                "epoch": entry.epoch,
                "digest": entry.digest,
                "level": self._entry_level(state, entry),
                "written_by": entry.written_by,
            })

        next_after = max(
            [e["position"] for e in entries]
            + announced_gaps + pending_gaps
            + [consumed_through, after],
        )
        return {
            "ok": True,
            "stream": state.stream,
            "tenant": state.tenant,
            "consumer": consumer,
            "phase": state.phase,
            "active_epoch": state.active_epoch,
            "epoch_behind": epoch_behind,
            "resume_from": consumed_through,
            "entries": entries,
            "announced_gaps": announced_gaps,
            "pending_gaps": pending_gaps,
            "next_after": next_after,
            "at": at,
        }

    def ack(self, at: int, stream: str, consumer: str,
            through: int) -> dict[str, Any]:
        state = self._stream_required(stream)
        _identifier(consumer, "consumer")
        _positive_int(through, "through")
        if through not in state.entries:
            raise EngineError("unknown_position",
                              f"位置 {through} 没有接受事实")
        event = {
            "type": "consumer_advanced", "at": at, "stream": stream,
            "consumer": consumer, "through": through,
            "epoch": state.entries[through].epoch,
        }
        self._emit(event, replaying=False)
        return {
            "ok": True, "stream": state.stream, "consumer": consumer,
            "through": state.consumers[consumer]["through"],
            "at": at,
        }

    # ------------------------------------------------------------------
    # 7) 值班端追查视图
    # ------------------------------------------------------------------

    def inspect(self, stream: str) -> dict[str, Any]:
        state = self._stream_required(stream)
        self._refresh_highest_confirmed(state)
        return {
            "stream": state.stream,
            "tenant": state.tenant,
            "policy": state.policy,
            "phase": state.phase,
            "primary": state.primary,
            "secondary": state.secondary,
            "active_epoch": state.active_epoch,
            "promotion": {
                "candidate": state.promotion_candidate,
                "epoch": state.promotion_epoch,
                "opened_at": state.promotion_opened_at,
            },
            "next_position": state.next_position,
            "last_strong_frontier": state.highest_confirmed,
            "confirmed_frontier": self._confirmed_frontier(state),
            "cross_region_frontier": self._cross_region_frontier(state),
            "candidate_frontier": self._candidate_frontier(
                state, state.promotion_candidate),
            "entries": [
                {
                    **e.evidence_view(),
                    "level": self._entry_level(state, e),
                } for e in state.ordered_entries()
            ],
            "announced_gaps": [e.position for e in state.ordered_entries()
                               if e.is_announced_gap],
            "pending_gaps": [e.position for e in state.ordered_entries()
                             if e.is_pending_gap],
            "consumers": {
                name: dict(progress)
                for name, progress in state.consumers.items()
            },
            "conflicts": list(state.conflicts),
            "audit": list(state.audit),
        }

    def list_streams(self) -> list[dict[str, Any]]:
        return [
            {
                "stream": s.stream, "tenant": s.tenant,
                "policy": s.policy, "phase": s.phase,
                "active_epoch": s.active_epoch,
                "primary": s.primary, "secondary": s.secondary,
            }
            for s in self.streams.values()
        ]

    # ------------------------------------------------------------------
    # 演练记录分发
    # ------------------------------------------------------------------

    def handle(self, at: int, kind: str,
               record: dict[str, Any]) -> dict[str, Any]:
        """按 README 的消息契约分发一条参与方消息。"""
        if kind == "stream":
            return self.register_stream(
                at, record["stream"], record["tenant"],
                record["primary"], record["secondary"], record["policy"])
        if kind == "write":
            return self.write(
                at, record["stream"], record["region"], record["epoch"],
                record.get("id"), record["digest"])
        if kind == "copied":
            return self.copied(
                at, record["stream"], record["region"],
                record["source_epoch"], record["position"],
                record["digest"])
        if kind == "promote":
            return self.promote(
                at, record["stream"], record["region"],
                record["new_epoch"])
        if kind == "loss":
            return self.approve_loss(at, record["stream"], record["span"])
        if kind == "read":
            return self.read(
                at, record["stream"], record["consumer"],
                record["after"], record["epoch"])
        if kind == "ack":
            return self.ack(
                at, record["stream"], record["consumer"],
                record["through"])
        if kind == "inspect":
            return {"ok": True, **self.inspect(record["stream"])}
        if kind == "restart":
            return self.restart()
        raise EngineError("unknown_message", kind)

    def reject(self, at: int, stream: Optional[str], op: str,
               error: EngineError) -> dict[str, Any]:
        """把拒绝也落审计日志，保证值班端能追查每次失败尝试。"""
        result = {"ok": False, "reason": error.reason,
                  "detail": error.detail, "op": op, "at": at}
        if stream is not None and stream in self.streams:
            self._emit({
                "type": "operation_rejected", "at": at, "stream": stream,
                "op": op, "reason": error.reason,
                "detail": error.detail,
            }, replaying=False)
            result["stream"] = stream
        return result

    def restart(self) -> dict[str, Any]:
        """关闭句柄并从日志重建全部状态，验证仅追加日志足够恢复。"""
        self.storage.close()
        engine = Engine(self.storage)
        self.streams = engine.streams
        self.sequence = engine.sequence
        return {"ok": True, "restarted": True,
                "streams": len(self.streams),
                "last_seq": self.sequence}

    def close(self) -> None:
        self.storage.close()
