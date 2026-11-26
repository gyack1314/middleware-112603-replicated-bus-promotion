"""中间件状态机测试：先钉死两个给定演练场景，再覆盖每条不变量。"""

import json
import tempfile
import unittest
from pathlib import Path

from drelay.engine import Engine
from drelay.model import (
    ACTIVE,
    ANNOUNCED,
    EngineError,
    PENDING,
    PROMOTED,
    PROMOTING,
    RECOVERED,
    RECOVERED_LATE,
    Storage,
)
from drelay.runner import run_scenario

EXAMPLES = Path(__file__).resolve().parent.parent.parent / "examples"


class EngineCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.engine = Engine(Storage(self.tmp.name))

    def tearDown(self):
        self.engine.close()
        self.tmp.cleanup()

    def register(self, policy="regional", stream="s",
                 primary="a", secondary="b", tenant="lab", at=0):
        return self.engine.register_stream(
            at, stream, tenant, primary, secondary, policy)

    # ------------------------------------------------------------------
    # 给定演练一：复制空洞 + regional 损失禁止 + 恢复后完成切换
    # ------------------------------------------------------------------

    def test_replication_hole_scenario(self):
        e = self.engine
        self.register(policy="regional")
        r1 = e.write(1, "s", "a", 1, "one", "x")
        r2 = e.write(2, "s", "a", 1, "two", "y")
        # regional 流在仅主站保管时只能等待
        self.assertEqual(r1["status"], "waiting")
        self.assertEqual(r1["position"], 1)
        self.assertEqual(r2["position"], 2)

        # 乱序：位置 2 的副本先到，连续证据边界仍停在 0
        c = e.copied(3, "s", "b", 1, 2, "y")
        self.assertEqual(c["confirmed_frontier"], 0)
        self.assertEqual(c["candidate_frontier"]["b"], 0)

        first_promote = e.promote(4, "s", "b", 2)
        self.assertEqual(first_promote["phase"], PROMOTING)
        self.assertEqual(first_promote["pending_gaps"], [1])
        self.assertFalse(first_promote["complete"])

        # regional 流的损失批准必须被拒绝
        with self.assertRaisesRegex(EngineError, "loss_forbidden_regional"):
            e.approve_loss(5, "s", [1, 1])

        # 迟来的副本补齐空洞
        e.copied(6, "s", "b", 1, 1, "x")
        self.assertEqual(self.engine.streams["s"].entries[1].gap,
                         RECOVERED)

        # 重复调用同一接管：此时证据连续，切换完成
        second = e.promote(8, "s", "b", 2)
        self.assertTrue(second["complete"])
        self.assertEqual(second["phase"], PROMOTED)
        self.assertEqual(second["active_epoch"], 2)

        read = e.read(9, "s", "client", 0, 1)
        self.assertEqual([item["position"] for item in read["entries"]],
                         [1, 2])
        self.assertEqual(read["announced_gaps"], [])
        self.assertTrue(read["epoch_behind"])

    # ------------------------------------------------------------------
    # 给定演练二：local 仅主站尾部、损失批准、旧主 fencing、消费事实
    # ------------------------------------------------------------------

    def test_accepted_tail_and_old_owner_scenario(self):
        e = self.engine
        self.register(policy="local")
        r1 = e.write(1, "s", "a", 1, "one", "x")
        self.assertEqual(r1["status"], "accepted")
        self.assertEqual(r1["level"], "primary_only")

        e.copied(2, "s", "b", 1, 1, "x")
        e.write(3, "s", "a", 1, "two", "y")

        read = e.read(4, "s", "c", 1, 1)
        self.assertEqual([item["position"] for item in read["entries"]], [2])
        e.ack(5, "s", "c", 2)

        promote = e.promote(6, "s", "b", 2)
        self.assertEqual(promote["pending_gaps"], [2])

        approved = e.approve_loss(7, "s", [2, 2])
        self.assertEqual(approved["positions"], [2])
        self.assertEqual(
            self.engine.streams["s"].entries[2].gap, ANNOUNCED)

        done = e.promote(8, "s", "b", 2)
        self.assertTrue(done["complete"])

        # 新写使用新代次，position 继续向后编号，不重用 2
        r3 = e.write(9, "s", "b", 2, "three", "z")
        self.assertEqual(r3["position"], 3)
        self.assertEqual(r3["epoch"], 2)

        # 旧主迟来的写：代次 fencing
        with self.assertRaisesRegex(EngineError, "stale_epoch"):
            e.write(10, "s", "a", 1, "late", "v")

        # 消费者重连：缺口被宣布，已处理事实不回退
        reconnect = e.read(11, "s", "c", 2, 1)
        self.assertEqual(reconnect["announced_gaps"], [2])
        self.assertEqual(
            [item["position"] for item in reconnect["entries"]], [3])
        self.assertEqual(reconnect["resume_from"], 2)

    # ------------------------------------------------------------------
    # 确认级别与连续边界
    # ------------------------------------------------------------------

    def test_regional_confirmation_requires_both_regions(self):
        e = self.engine
        self.register(policy="regional")
        e.write(1, "s", "a", 1, "m1", "d1")
        e.write(2, "s", "a", 1, "m2", "d2")
        e.copied(3, "s", "b", 1, 1, "d1")
        view = e.inspect("s")
        self.assertEqual(view["confirmed_frontier"], 1)
        self.assertEqual(view["entries"][0]["level"], "confirmed")
        # 位置 2 只有主站副本：拓扑级别 primary_only，regional 回执只能 waiting
        self.assertEqual(view["entries"][1]["level"], "primary_only")
        # 空洞后面的位置即使双区有证据，也不能越过边界
        e.copied(4, "s", "b", 1, 2, "d2")
        self.assertEqual(e.inspect("s")["confirmed_frontier"], 2)

    def test_max_position_does_not_imply_prefix(self):
        e = self.engine
        self.register(policy="regional")
        for pos, digest in enumerate(("d1", "d2", "d3"), start=1):
            e.write(pos, "s", "a", 1, f"m{pos}", digest)
        e.copied(4, "s", "b", 1, 2, "d2")
        e.copied(5, "s", "b", 1, 3, "d3")
        view = e.inspect("s")
        self.assertEqual(view["candidate_frontier"]["b"], 0)
        self.assertEqual(view["confirmed_frontier"], 0)

    def test_local_primary_only_receipt_then_topology_upgrade(self):
        e = self.engine
        self.register(policy="local")
        receipt = e.write(1, "s", "a", 1, "m", "d")
        self.assertEqual(receipt["status"], "accepted")
        self.assertEqual(receipt["level"], "primary_only")
        e.copied(2, "s", "b", 1, 1, "d")
        view = e.inspect("s")
        # 副本到达后，该位置的拓扑级别升级为跨区确认，可追查
        self.assertEqual(view["entries"][0]["level"], "confirmed")
        self.assertEqual(view["cross_region_frontier"], 1)

    # ------------------------------------------------------------------
    # 幂等与 fencing
    # ------------------------------------------------------------------

    def test_duplicate_production_request_recovers_original_result(self):
        e = self.engine
        self.register(policy="regional")
        first = e.write(1, "s", "a", 1, "req-1", "d")
        second = e.write(2, "s", "a", 1, "req-1", "d")
        self.assertTrue(second["deduplicated"])
        self.assertEqual(second["position"], first["position"])
        self.assertEqual(self.engine.streams["s"].next_position, 2)
        # 切换后用新代次重送同一请求，仍只恢复原结果
        e.copied(3, "s", "b", 1, 1, "d")
        e.promote(4, "s", "b", 2)
        replay = e.write(5, "s", "b", 2, "req-1", "d")
        self.assertTrue(replay["deduplicated"])
        self.assertEqual(replay["position"], 1)
        self.assertEqual(replay["epoch"], 1)
        self.assertEqual(self.engine.streams["s"].next_position, 2)

    def test_non_primary_and_wrong_region_are_fenced(self):
        e = self.engine
        self.register(policy="regional")
        with self.assertRaisesRegex(EngineError, "not_primary"):
            e.write(1, "s", "b", 1, "x", "d")
        with self.assertRaisesRegex(EngineError, "stale_epoch"):
            e.write(1, "s", "a", 9, "x", "d")

    def test_writes_freeze_during_promotion(self):
        e = self.engine
        self.register(policy="local")
        e.write(1, "s", "a", 1, "m", "d")
        e.promote(2, "s", "b", 2)
        with self.assertRaisesRegex(EngineError, "promotion_in_progress"):
            e.write(3, "s", "a", 1, "n", "e")

    # ------------------------------------------------------------------
    # 切换与损失批准的边界
    # ------------------------------------------------------------------

    def test_loss_never_discards_cross_region_confirmed_data(self):
        e = self.engine
        self.register(policy="local")
        e.write(1, "s", "a", 1, "m1", "d1")
        e.write(2, "s", "a", 1, "m2", "d2")
        # 位置 1 已双区保存：即使它在切换时不缺失，也构造保护检查
        e.copied(2, "s", "b", 1, 1, "d1")
        e.promote(3, "s", "b", 2)
        # 位置 1 不缺，不能被当作损失 span 批准
        with self.assertRaisesRegex(EngineError, "span_not_approvable"):
            e.approve_loss(4, "s", [1, 2])

    def test_partial_span_rejection_leaves_state_unchanged(self):
        e = self.engine
        self.register(policy="local")
        e.write(1, "s", "a", 1, "m1", "d1")
        e.write(2, "s", "a", 1, "m2", "d2")
        e.promote(3, "s", "b", 2)
        # 两个位置都是待决缺口
        self.assertEqual(
            [p for p in (1, 2)
             if self.engine.streams["s"].entries[p].is_pending_gap],
            [1, 2])
        # 先恢复位置 1，使混合 span 中出现不可批准项
        e.copied(4, "s", "b", 1, 1, "d1")
        with self.assertRaisesRegex(EngineError, "span_not_approvable"):
            e.approve_loss(5, "s", [1, 2])
        # 整笔拒绝：位置 1 保持已恢复，位置 2 仍待决，没有缺口被宣布
        self.assertEqual(
            self.engine.streams["s"].entries[1].gap, RECOVERED)
        self.assertEqual(
            self.engine.streams["s"].entries[2].gap, PENDING)
        # 只批准真正待决的位置
        approved = e.approve_loss(6, "s", [2, 2])
        self.assertEqual(approved["positions"], [2])
        self.assertIn("loss_approved",
                      [a["action"] for a in e.inspect("s")["audit"]])

    def test_promotion_is_idempotent_and_rejects_competing_attempt(self):
        e = self.engine
        self.register(policy="local")
        e.write(1, "s", "a", 1, "m", "d")
        e.promote(2, "s", "b", 2)
        with self.assertRaisesRegex(EngineError, "promotion_conflict"):
            e.promote(3, "s", "c", 3)
        # 同一候选/代次重复调用不产生新事件
        again = e.promote(4, "s", "b", 2)
        self.assertEqual(again["new_epoch"], 2)
        self.assertEqual(again["phase"], PROMOTING)
        # 完成后再调用，返回已完成视图
        e.approve_loss(5, "s", [1, 1])
        e.promote(6, "s", "b", 2)
        finished = e.promote(7, "s", "b", 2)
        self.assertTrue(finished["already_complete"])

    def test_new_epoch_must_advance(self):
        e = self.engine
        self.register(policy="local")
        with self.assertRaisesRegex(EngineError, "invalid_epoch"):
            e.promote(1, "s", "b", 1)

    def test_promotion_keeps_two_designated_regions(self):
        e = self.engine
        self.register(policy="regional")
        e.write(1, "s", "a", 1, "m", "d")
        e.copied(2, "s", "b", 1, 1, "d")
        e.promote(3, "s", "b", 2)
        state = self.engine.streams["s"]
        self.assertEqual(state.primary, "b")
        self.assertEqual(state.secondary, "a")
        # 新主上的新写仍需两个指定区域确认才达到 regional 保证
        receipt = e.write(4, "s", "b", 2, "n", "e")
        self.assertEqual(receipt["status"], "waiting")
        e.copied(5, "s", "a", 2, 2, "e")
        self.assertEqual(e.inspect("s")["confirmed_frontier"], 2)

    # ------------------------------------------------------------------
    # 旧主恢复：迟来证据与冲突隔离
    # ------------------------------------------------------------------

    def test_late_evidence_after_announced_gap_is_history_only(self):
        e = self.engine
        # 指定第三区域 c 作为旧主恢复后补送历史事实的来源
        self.register(policy="local")
        e.write(1, "s", "a", 1, "m", "d")
        e.promote(2, "s", "b", 2)
        e.approve_loss(3, "s", [1, 1])
        e.promote(4, "s", "b", 2)
        # 旧主恢复后由其灾备侧 c 补回历史事实；缺口宣布不复活
        e.copied(5, "s", "c", 1, 1, "d")
        entry = self.engine.streams["s"].entries[1]
        self.assertEqual(entry.gap, ANNOUNCED)
        self.assertEqual(len(entry.late_evidence), 1)
        self.assertEqual(entry.late_evidence[0]["region"], "c")
        # 副本集合保留历史事实，但消费视图里它仍是已宣布缺口
        self.assertIn("c", entry.replicas)
        read = e.read(6, "s", "consumer", 0, 1)
        self.assertEqual(read["announced_gaps"], [1])
        self.assertEqual(read["entries"], [])

    def test_duplicate_old_owner_report_remains_idempotent(self):
        e = self.engine
        self.register(policy="local")
        e.write(1, "s", "a", 1, "m", "d")
        e.copied(2, "s", "b", 1, 1, "d")
        e.promote(3, "s", "b", 2)
        e.promote(4, "s", "b", 2)
        # 旧主重复上报它本来就有的副本：幂等，不产生冲突也不产生新证据
        result = e.copied(5, "s", "a", 1, 1, "d")
        self.assertTrue(result["duplicate"])
        self.assertEqual(
            self.engine.streams["s"].entries[1].late_evidence, [])

    def test_digest_conflict_is_quarantined_not_double_written(self):
        e = self.engine
        self.register(policy="regional")
        e.write(1, "s", "a", 1, "m", "good")
        with self.assertRaisesRegex(EngineError, "digest_conflict"):
            e.copied(2, "s", "b", 1, 1, "evil")
        view = e.inspect("s")
        self.assertEqual(len(view["conflicts"]), 1)
        self.assertEqual(view["conflicts"][0]["received_digest"], "evil")
        self.assertNotIn("b", view["entries"][0]["replicas"])
        # 正确证据仍可正常登记
        e.copied(3, "s", "b", 1, 1, "good")
        self.assertIn("b", e.inspect("s")["entries"][0]["replicas"])

    def test_unknown_position_and_epoch_mismatch_rejected(self):
        e = self.engine
        self.register(policy="regional")
        e.write(1, "s", "a", 1, "m", "d")
        with self.assertRaisesRegex(EngineError, "unknown_position"):
            e.copied(2, "s", "b", 1, 9, "d")
        with self.assertRaisesRegex(EngineError, "epoch_mismatch"):
            e.copied(3, "s", "b", 2, 1, "d")

    # ------------------------------------------------------------------
    # 消费接续
    # ------------------------------------------------------------------

    def test_consumer_progress_is_remembered_across_reconnects(self):
        e = self.engine
        self.register(policy="local")
        for i in range(1, 4):
            e.write(i, "s", "a", 1, f"m{i}", f"d{i}")
        e.ack(4, "s", "c", 2)
        # 客户端即便带着更小的 after 重连，服务端仍以实际消费事实为准
        read = e.read(5, "s", "c", 0, 1)
        self.assertEqual(read["resume_from"], 2)
        positions = [item["position"] for item in read["entries"]]
        self.assertEqual(positions, [3])

    def test_pending_gaps_are_signaled_not_silently_skipped(self):
        e = self.engine
        self.register(policy="local")
        e.write(1, "s", "a", 1, "m1", "d1")
        e.write(2, "s", "a", 1, "m2", "d2")
        e.copied(2, "s", "b", 1, 1, "d1")
        e.promote(3, "s", "b", 2)
        read = e.read(4, "s", "c", 0, 1)
        self.assertEqual(read["pending_gaps"], [2])
        self.assertEqual(
            [item["position"] for item in read["entries"]], [1])

    def test_gaps_do_not_consume_entry_page_budget(self):
        e = self.engine
        self.register(policy="local")
        for pos in range(1, 5):
            e.write(pos, "s", "a", 1, f"m{pos}", f"d{pos}")
        e.promote(5, "s", "b", 2)
        # 位置 1、2 批准为缺口，3、4 待决后恢复
        e.approve_loss(6, "s", [1, 2])
        e.copied(7, "s", "b", 1, 3, "d3")
        e.copied(8, "s", "b", 1, 4, "d4")
        e.promote(9, "s", "b", 2)
        read = e.read(10, "s", "c", 0, 1, max_items=2)
        self.assertEqual(read["announced_gaps"], [1, 2])
        self.assertEqual(
            [item["position"] for item in read["entries"]], [3, 4])

    def test_sequence_continues_after_restart(self):
        e = self.engine
        self.register(policy="local")
        e.write(1, "s", "a", 1, "m1", "d1")
        before = e.sequence
        result = e.restart()
        self.assertEqual(result["last_seq"], before)
        e.write(2, "s", "b" if False else "a", 1, "m2", "d2")
        self.assertEqual(e.sequence, before + 1)
        # 再重启一次，状态仍一致
        e.restart()
        self.assertEqual(e.streams["s"].next_position, 3)

    # ------------------------------------------------------------------
    # 租户隔离
    # ------------------------------------------------------------------

    def test_independent_tenants_are_not_frozen_together(self):
        e = self.engine
        self.register(policy="local", stream="s1", tenant="t1")
        self.register(policy="local", stream="s2", tenant="t2")
        e.write(1, "s1", "a", 1, "m", "d")
        e.promote(2, "s1", "b", 2)
        # s1 冻结，s2 仍可正常写
        receipt = e.write(3, "s2", "a", 1, "n", "e")
        self.assertEqual(receipt["status"], "accepted")
        self.assertEqual(e.inspect("s1")["phase"], PROMOTING)
        self.assertEqual(e.inspect("s2")["phase"], ACTIVE)

    # ------------------------------------------------------------------
    # 持久化与重放
    # ------------------------------------------------------------------

    def test_restart_rebuilds_full_state_from_log(self):
        e = self.engine
        self.register(policy="regional")
        e.write(1, "s", "a", 1, "m1", "d1")
        e.write(2, "s", "a", 1, "m2", "d2")
        e.copied(3, "s", "b", 1, 2, "d2")
        e.promote(4, "s", "b", 2)
        try:
            e.approve_loss(5, "s", [1, 1])
        except EngineError as error:
            # 与 runner 一致：拒绝也落审计日志
            e.reject(5, "s", "loss", error)
        before = e.inspect("s")
        e.restart()
        after = e.inspect("s")
        self.assertEqual(
            json.dumps(before, sort_keys=True),
            json.dumps(after, sort_keys=True))
        # 重启后仍能继续工作，且拒绝事实在审计里
        actions = [a["action"] for a in after["audit"]]
        self.assertIn("rejected", actions)
        self.assertEqual(e.streams["s"].phase, PROMOTING)

    def test_running_example_files_end_to_end(self):
        with tempfile.TemporaryDirectory() as data_dir:
            results = run_scenario(
                EXAMPLES / "replication-hole.jsonl", data_dir)
        kinds = [(r["kind"], r["result"].get("ok")) for r in results]
        self.assertIn(("loss", False), kinds)
        self.assertTrue(kinds[-1][1])  # restart 成功
        with tempfile.TemporaryDirectory() as data_dir:
            results = run_scenario(
                EXAMPLES / "accepted-tail-and-old-owner.jsonl", data_dir)
        late_write = [r for r in results
                      if r["kind"] == "write"
                      and r["record"].get("id") == "late"]
        self.assertEqual(len(late_write), 1)
        self.assertEqual(late_write[0]["result"]["reason"], "stale_epoch")
        read = [r for r in results if r["kind"] == "read"][-1]["result"]
        self.assertEqual(read["announced_gaps"], [2])
        self.assertEqual(
            [item["position"] for item in read["entries"]], [3])


if __name__ == "__main__":
    unittest.main()
