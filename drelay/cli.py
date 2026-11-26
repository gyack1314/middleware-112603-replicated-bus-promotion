"""值班端命令行入口：发布、跨区保存、切换、损失批准、消费与追查。

所有输出为单个 JSON 对象；被拒绝的操作 ok=false 并带稳定 reason，
拒绝事实同样写入审计日志。示例：

    python3 -m drelay.cli --data-dir ./data register \
        --stream s --tenant lab --primary a --secondary b --policy regional
    python3 -m drelay.cli --data-dir ./data inspect --stream s
    python3 -m drelay.cli --data-dir ./data run examples/replication-hole.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

from .engine import Engine
from .model import EngineError, Storage
from .runner import load_message_types, render, run_scenario


def _print(payload) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _build(args: argparse.Namespace) -> tuple[Engine, Storage]:
    storage = Storage(args.data_dir)
    return Engine(storage), storage


def _execute(args: argparse.Namespace, operation) -> None:
    engine, storage = _build(args)
    try:
        payload = operation(engine)
        _print(payload)
    except EngineError as error:
        _print({"ok": False, "reason": error.reason,
                "detail": error.detail})
        sys.exit(2)
    finally:
        engine.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="drelay", description="本地仿真的多区域事件接入与灾备中间件")
    parser.add_argument("--data-dir", required=True, help="持久化数据目录")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("register", help="登记租户流")
    p.add_argument("--stream", required=True)
    p.add_argument("--tenant", required=True)
    p.add_argument("--primary", required=True)
    p.add_argument("--secondary", required=True)
    p.add_argument("--policy", required=True, choices=["regional", "local"])
    p.add_argument("--at", type=int, default=0)
    p.set_defaults(func=lambda a, e: e.register_stream(
        a.at, a.stream, a.tenant, a.primary, a.secondary, a.policy))

    p = sub.add_parser("write", help="在当前主站发布")
    p.add_argument("--stream", required=True)
    p.add_argument("--region", required=True)
    p.add_argument("--epoch", type=int, required=True)
    p.add_argument("--id", dest="request_id", default=None)
    p.add_argument("--digest", required=True)
    p.add_argument("--at", type=int, default=0)
    p.set_defaults(func=lambda a, e: e.write(
        a.at, a.stream, a.region, a.epoch, a.request_id, a.digest))

    p = sub.add_parser("copied", help="登记跨区副本证据")
    p.add_argument("--stream", required=True)
    p.add_argument("--region", required=True)
    p.add_argument("--source-epoch", type=int, required=True)
    p.add_argument("--position", type=int, required=True)
    p.add_argument("--digest", required=True)
    p.add_argument("--at", type=int, default=0)
    p.set_defaults(func=lambda a, e: e.copied(
        a.at, a.stream, a.region, a.source_epoch, a.position, a.digest))

    p = sub.add_parser("promote", help="候选区域尝试接管（可重复调用）")
    p.add_argument("--stream", required=True)
    p.add_argument("--region", required=True)
    p.add_argument("--new-epoch", type=int, required=True)
    p.add_argument("--at", type=int, default=0)
    p.set_defaults(func=lambda a, e: e.promote(
        a.at, a.stream, a.region, a.new_epoch))

    p = sub.add_parser("loss", help="对 local 流的待决缺口批准损失")
    p.add_argument("--stream", required=True)
    p.add_argument("--start", type=int, required=True)
    p.add_argument("--end", type=int, required=True)
    p.add_argument("--operator", default="oncall")
    p.add_argument("--at", type=int, default=0)
    p.set_defaults(func=lambda a, e: e.approve_loss(
        a.at, a.stream, [a.start, a.end], a.operator))

    p = sub.add_parser("read", help="消费者带处理位置与发布代次重连")
    p.add_argument("--stream", required=True)
    p.add_argument("--consumer", required=True)
    p.add_argument("--after", type=int, required=True)
    p.add_argument("--epoch", type=int, required=True)
    p.add_argument("--at", type=int, default=0)
    p.set_defaults(func=lambda a, e: e.read(
        a.at, a.stream, a.consumer, a.after, a.epoch))

    p = sub.add_parser("ack", help="消费者确认实际处理 through")
    p.add_argument("--stream", required=True)
    p.add_argument("--consumer", required=True)
    p.add_argument("--through", type=int, required=True)
    p.add_argument("--at", type=int, default=0)
    p.set_defaults(func=lambda a, e: e.ack(
        a.at, a.stream, a.consumer, a.through))

    p = sub.add_parser("inspect", help="按流追查新旧代次与缺口")
    p.add_argument("--stream", required=True)
    p.set_defaults(func=lambda a, e: e.inspect(a.stream))

    p = sub.add_parser("streams", help="列出全部租户流")
    p.set_defaults(func=lambda a, e: {"streams": e.list_streams()})

    p = sub.add_parser("restart", help="从日志重建状态")
    p.set_defaults(func=lambda a, e: e.restart())

    p = sub.add_parser("run", help="运行一份演练 JSONL（独立数据目录）")
    p.add_argument("scenario", type=Path)
    p.add_argument("--keep-data", default=None,
                   help="保留数据目录的路径；默认使用临时目录")
    p.set_defaults(func=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "run":
        data_dir = args.keep_data or tempfile.mkdtemp(prefix="drelay-")
        supported = load_message_types(args.scenario.parent)
        results = run_scenario(args.scenario, data_dir, supported)
        print(render(results))
        print(f"\n数据目录：{data_dir}")
        return 0

    def operation(engine: Engine):
        return args.func(args, engine)

    try:
        _execute(args, operation)
    except EngineError:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
