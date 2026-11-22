"""行辕算子 —— 道统算子谱通路组件（xingyuan）
道统出处：行辕承命，不擅自裁（构件定名册三十四员）。
工程映射：算力路由算子（SRS-ROUTING-OPERATOR-001 转正版，绑定真值
TR-CENTRAL-ROUTE-DIFF-001）——无状态任务寻址、标签匹配、微元水位
负载分发、fallback 故障切换。只读转发：不承载裁决、真值校验、记忆
逻辑（与中枢架构边界条款一致，熔断与放行归上层裁决内核）。
生长纪律：本地生长自由，推送远端需宗源发令。
"""
import uuid


class Xingyuan:
    """行辕：无状态路由器。

    约束落点（SRS constraint C01–C05）：
    - C01 只读转发：本算子不决定任务放行与否，只选路、转发、上报；
    - C02 无本地记忆：节点水位一律由外部传入（中枢统一水位探针输出），
      本体不保存配额状态；
    - C03 审计外送：每次路由动作生成 trace 记录，交由注入的审计出口，
      不可篡改由账本层保证；
    - C04 最小算力：同分节点按剩余配额权重优先低消耗侧（稳定排序）；
    - C05 高风险不上路：高风险任务须裁决门签发准许，否则拒绝承命。
    """

    def __init__(self, audit_sink=None):
        # audit_sink: callable(rec) —— 路由日志外送口；缺省不落任何本地状态
        self._audit_sink = audit_sink

    # ------------------------------------------------------------------
    def describe(self) -> dict:
        return {"name": "xingyuan", "stage": "m1",
                "role": "算力路由算子（无状态寻址/标签匹配/水位分发/fallback 切换）",
                "srs": "SRS-ROUTING-OPERATOR-001"}

    # ------------------------------------------------------------------
    def _emit(self, rec):
        rec.setdefault("trace_id", "xy-" + uuid.uuid4().hex)
        if self._audit_sink is not None:
            try:
                self._audit_sink(dict(rec))
            except Exception:
                pass  # 审计出口故障不阻断路由本身，事件由调用方复核
        return rec

    # ------------------------------------------------------------------
    def _probe_view(self, nid, model_pool):
        """归一水位探针视图：支持中枢输出简式数值（余量）与详式 dict 两态。"""
        wm = self._wm or {}
        probe = wm.get(nid, {})
        pool_by_id = self._pool_by_id
        n = pool_by_id.get(nid) or {}
        if isinstance(probe, (int, float)):
            return probe, "ok"
        quota = probe.get("quota", n.get("quota", 1))
        health = probe.get("health", n.get("health", "ok"))
        return quota, health

    def _candidates(self, task_tag, model_pool, watermark, fallback_chain):
        """标签匹配 + 微元水位筛选，输出稳定有序候选链。

        - 标签交集最大者优先（AC01 稳定匹配专长节点）；
        - 配额耗尽、离线、过载节点剔除（SRS 核心逻辑 2）；
        - 同分按剩余配额降权排序（C04 最小算力优先）；
        - fallback 链补在标签命中之后（AC02 备选链）。
        """
        self._wm = watermark or {}
        self._pool_by_id = {n.get("id"): n for n in (model_pool or [])}
        tags = set(task_tag) if isinstance(task_tag, (list, tuple, set)) else {task_tag}
        cands = []
        for n in model_pool or []:
            nid = n.get("id")
            if nid is None:
                continue
            quota, health = self._probe_view(nid, model_pool)
            if quota is not None and quota <= 0:
                continue
            if health in ("offline", "overload"):
                continue
            overlap = len(set(n.get("tags", [])) & tags)
            cands.append((-overlap, -(quota or 0), str(nid)))
        cands.sort()
        order = [c[2] for c in cands]
        for nid in fallback_chain or []:
            if nid in order:
                continue
            # 备选链节点可不在当前池内（SRS：降级备选序列独立给出）；
            # 若中枢水位给出明确否定（耗尽/离线/过载）则跳过，否则照链承命
            quota, health = self._probe_view(nid, model_pool)
            if quota is not None and quota <= 0:
                continue
            if health in ("offline", "overload"):
                continue
            order.append(nid)
        return order

    # ------------------------------------------------------------------
    def route(self, task_tag, model_pool, watermark=None, fallback_chain=None,
              risk=False, risk_gate=None, transport=None, payload=None):
        """选路并转发。返回路由回执 dict。

        - transport: callable(node_id, payload) —— 底层转发函数（可选）；
          抛异常视为节点故障，沿 fallback 链切换（SRS 核心逻辑 4）；
        - risk + risk_gate: 高风险任务须裁决门放行（C05），裁决缺失或
          拒绝即返回 GATE_REQUIRED，本算子不代行裁决（C01）。
        """
        if risk and (risk_gate is None or not risk_gate(task_tag)):
            return self._emit({"task_tag": task_tag, "target_node_id": None,
                               "route_status": "GATE_REQUIRED", "tried": []})
        order = self._candidates(task_tag, model_pool, watermark, fallback_chain)
        tried = []
        for nid in order:
            tried.append(nid)
            if transport is None:
                # 干路由（不实际转发）：只选路不触达，验收 AC03/AC01 用
                return self._emit({"task_tag": task_tag, "target_node_id": nid,
                                   "route_status": "SUCCESS", "tried": tried})
            try:
                transport(nid, payload)
                status = "SUCCESS" if len(tried) == 1 else "FALLBACK"
                return self._emit({"task_tag": task_tag, "target_node_id": nid,
                                   "route_status": status, "tried": tried})
            except Exception:
                continue  # 节点超时/异常：标记并沿备选链继续（不裁定不熔断）
        return self._emit({"task_tag": task_tag, "target_node_id": None,
                           "route_status": "NO_AVAILABLE_NODE", "tried": tried})

    # ------------------------------------------------------------------
    def forward(self, node_id, fn, *args, **kwargs):
        """转发透传：原样执行底层调用，不改语义、不校验结果真值（AC04）。

        返回 (ok, result_or_error)；异常原样封存为 error 交上层，本算子
        不判对错、不做重试语义（重试由中枢配置控制）。
        """
        try:
            result = fn(*args, **kwargs)
            self._emit({"task_tag": "forward", "target_node_id": node_id,
                        "route_status": "SUCCESS", "tried": [node_id]})
            return True, result
        except Exception as exc:  # noqa: BLE001 —— 透传原样上报，不吞不判
            self._emit({"task_tag": "forward", "target_node_id": node_id,
                        "route_status": "NODE_ERROR", "tried": [node_id],
                        "error": f"{type(exc).__name__}"})
            return False, exc
