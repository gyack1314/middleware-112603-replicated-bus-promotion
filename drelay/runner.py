"""把演练 JSONL 记录喂给引擎，逐条产出可追查的回执。

同一场景跑在独立的临时/指定数据目录上，便于对照每个秒点的状态。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

from .engine import Engine
from .model import EngineError, Storage


def load_message_types(examples_dir: Path) -> set[str]:
    registry = examples_dir / "message_types.json"
    if registry.exists():
        return set(json.loads(registry.read_text(encoding="utf-8")))
    return {"stream", "write", "copied", "promote", "loss",
            "read", "ack", "restart", "inspect"}


def run_scenario(path: Path, data_dir: str,
                 supported: Optional[set[str]] = None) -> list[dict[str, Any]]:
    """运行单个 JSONL 场景，返回带行号与秒点的结果序列。"""
    if supported is None:
        supported = load_message_types(path.parent)
    storage = Storage(data_dir)
    engine = Engine(storage)
    results: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as source:
        for line_number, raw in enumerate(source, 1):
            row = json.loads(raw)
            at, kind, record = row["at"], row["kind"], row["record"]
            if kind not in supported:
                raise ValueError(f"{path.name} 第 {line_number} 行：未登记消息 {kind}")
            try:
                result = engine.handle(at, kind, record)
            except EngineError as error:
                result = engine.reject(
                    at, record.get("stream"), kind, error)
            results.append({
                "line": line_number, "at": at, "kind": kind,
                "record": record, "result": result,
            })
    engine.close()
    return results


def render(results: list[dict[str, Any]]) -> str:
    lines = []
    for item in results:
        head = f"#{item['line']} t={item['at']} {item['kind']}"
        result = item["result"]
        if result.get("ok"):
            lines.append(f"{head} -> {json.dumps(result, ensure_ascii=False)}")
        else:
            lines.append(
                f"{head} -> 拒绝[{result['reason']}] {result['detail']}")
    return "\n".join(lines)
