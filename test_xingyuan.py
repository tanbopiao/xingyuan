"""行辕算子单测：标签匹配/水位筛选/fallback 切换/审计外送/高风险门/透传不判真值。"""
import importlib.util
import os
import unittest

_s = importlib.util.spec_from_file_location(
    "xingyuan_core", os.path.join(os.path.dirname(os.path.abspath(__file__)), "xingyuan.py"))
_x = importlib.util.module_from_spec(_s)
_s.loader.exec_module(_x)

POOL = [
    {"id": "n1", "tags": ["HR评估", "多模态"], "quota": 100, "health": "ok"},
    {"id": "n2", "tags": ["HR评估"], "quota": 50, "health": "ok"},
    {"id": "n3", "tags": ["代码推演"], "quota": 80, "health": "ok"},
    {"id": "n4", "tags": ["HR评估"], "quota": 0, "health": "ok"},        # 配额耗尽
    {"id": "n5", "tags": ["HR评估"], "quota": 30, "health": "offline"},  # 离线
]


class T(unittest.TestCase):
    def setUp(self):
        self.audit = []
        self.xy = _x.Xingyuan(audit_sink=self.audit.append)

    def test_describe(self):
        d = self.xy.describe()
        self.assertEqual(d["name"], "xingyuan")
        self.assertEqual(d["stage"], "m1")
        self.assertEqual(d["srs"], "SRS-ROUTING-OPERATOR-001")

    def test_tag_match_stable(self):
        """AC01：相同任务标签稳定匹配交集最大的专长节点；并列取配额高者。"""
        r1 = self.xy.route("HR评估", POOL)
        r2 = self.xy.route(["HR评估"], POOL)
        self.assertEqual(r1["route_status"], "SUCCESS")
        self.assertEqual(r1["target_node_id"], "n1")   # 交集最大（2 标签）
        self.assertEqual(r1["target_node_id"], r2["target_node_id"])  # 稳定
        r3 = self.xy.route("代码推演", POOL)
        self.assertEqual(r3["target_node_id"], "n3")

    def test_watermark_filter(self):
        """SRS 逻辑2：配额耗尽/离线节点被水位筛选剔除；中枢水位可覆盖本地值。"""
        r = self.xy.route("HR评估", POOL)
        self.assertEqual(r["target_node_id"], "n1")   # 交集最大（2 标签）
        self.assertNotIn("n4", r["tried"])            # 配额耗尽被剔除
        self.assertNotIn("n5", r["tried"])            # 离线被剔除
        # 中枢探针水位：n1 已耗尽 → 落到 n2（C02：水位从外部拉取）
        r = self.xy.route("HR评估", POOL, watermark={"n1": {"quota": 0}})
        self.assertEqual(r["target_node_id"], "n2")

    def test_fallback_chain(self):
        """AC02：primary 故障沿 fallback 链切换，状态记 FALLBACK，不阻塞上层。"""
        def broken(nid, payload):
            raise RuntimeError("node down")
        pool = [{"id": "a", "tags": ["t"], "quota": 9, "health": "ok"}]
        r = self.xy.route("t", pool, fallback_chain=["b"],
                          transport=broken, payload={})
        self.assertEqual(r["route_status"], "NO_AVAILABLE_NODE")
        self.assertEqual(r["tried"], ["a", "b"])  # 链上备选被尝试
        # 备选可用时：一次切换即达
        def flip(nid, payload):
            if nid == "a":
                raise RuntimeError("a down")
        r2 = self.xy.route("t", pool, fallback_chain=["b"],
                           transport=flip, payload={})
        self.assertEqual(r2["route_status"], "FALLBACK")
        self.assertEqual(r2["target_node_id"], "b")

    def test_audit_emit(self):
        """C03：每次路由动作外送审计记录，含 trace_id 可溯源。"""
        r = self.xy.route("HR评估", POOL)
        self.assertEqual(len(self.audit), 1)
        self.assertTrue(self.audit[0]["trace_id"].startswith("xy-"))
        self.assertEqual(self.audit[0]["trace_id"], r["trace_id"])

    def test_risk_gate(self):
        """C05：高风险任务未经裁决门放行即拒绝承命；放行则照常路由。"""
        r = self.xy.route("涉密任务", POOL, risk=True)
        self.assertEqual(r["route_status"], "GATE_REQUIRED")
        self.assertEqual(r["target_node_id"], None)
        r2 = self.xy.route("涉密任务", POOL, risk=True, risk_gate=lambda t: True)
        self.assertEqual(r2["route_status"], "SUCCESS")
        r3 = self.xy.route("涉密任务", POOL, risk=True, risk_gate=lambda t: False)
        self.assertEqual(r3["route_status"], "GATE_REQUIRED")

    def test_forward_passthrough(self):
        """AC04：转发透传不判真值——结果原样返回，异常原样封存上报。"""
        ok, val = self.xy.forward("n1", lambda x: x * 2, 21)
        self.assertEqual((ok, val), (True, 42))
        bad, err = self.xy.forward("n1", lambda: 1 / 0)
        self.assertFalse(bad)
        self.assertIsInstance(err, ZeroDivisionError)

    def test_no_memory(self):
        """C02：连续路由不积累本地状态——两次回执互不影响。"""
        a = self.xy.route("HR评估", POOL)
        b = self.xy.route("HR评估", POOL)
        self.assertEqual(a["target_node_id"], b["target_node_id"])
        self.assertEqual(self.xy.describe().get("records", 0), 0)

    def test_no_available_node(self):
        """全池耗尽且备选链也被水位否定：返回 NO_AVAILABLE_NODE 如实上报。"""
        empty = [{"id": "x", "tags": ["t"], "quota": 0, "health": "ok"}]
        r = self.xy.route("t", empty, fallback_chain=["y"],
                          watermark={"y": {"quota": 0}})
        self.assertEqual(r["route_status"], "NO_AVAILABLE_NODE")
        self.assertNotIn("x", r["tried"])
        # 备选链健康时可承命（链上节点可不在当前池快照内）
        r2 = self.xy.route("t", empty, fallback_chain=["y"])
        self.assertEqual(r2["route_status"], "SUCCESS")
        self.assertEqual(r2["target_node_id"], "y")


if __name__ == "__main__":
    unittest.main()
