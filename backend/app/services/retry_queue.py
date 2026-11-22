"""重放队列领域服务：管理失败请求重试，与 request_batch、call_log、api_monitor、govern_report 联动。

重放耗尽不再直接产生异常报告，而是生成「采集失败结案证据」进入治理：
- 耗尽时保留当时的失败证据快照（尝试次数、失败原因、关联调用日志），中断重试可核对；
- 迟到的成功采集回执按数据段关联结案证据，同一成功调用只算一次补齐；
- 已经实际补到的区间不再成为补采候选，补采候选查询兼容历史失败记录；
- 治理报告归档裁决后，剩余缺口同步回重放待办，支撑下一轮派发核对。
"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.store import store

TODAY = "2026-11-22"

# 结案证据结案状态
EVIDENCE_OPEN = "待补采"
EVIDENCE_FILLED = "已补齐"


def _to_int(value, default: int = 0) -> int:
    """种子数据里数值字段可能是占位字符串，解析失败时回退默认值。"""
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


class RetryQueueService(BaseModuleService):
    MODULE = "retry_queue"
    ENTITY = "重放任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务编号", "失败端点", "失败原因", "重试次数", "最大重试", "退避策略", "最后重试", "队列状态"]
    REQUIRED_FIELDS = ["任务编号", "失败端点", "失败原因"]
    STATUS_ORDER = ["待重试", "重试中", "已成功", "永久失败"]
    ACTION_RULES = {"加入队列": "重试中", "手动重试": "已成功", "标记失败": "永久失败"}
    NEGATIVE_ACTIONS = ["标记失败"]

    # ---------- 登记 ----------

    def create_entry(self, values: dict):
        """登记重放任务时支持携带关联数据段，供结案证据与回执关联使用。"""
        entry, missing = super().create_entry(values)
        if entry is not None and str(values.get("数据段") or "").strip():
            entry["数据段"] = str(values["数据段"]).strip()
        return entry, missing

    # ---------- 动作 ----------

    def run_action(self, entry_id: int, action: str):
        """重试失败按次计数，耗尽自动生成结案证据；永久失败进入治理并保留失败证据。"""
        if action == "重试失败":
            return self._record_failed_attempt(entry_id)
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if action == "加入队列":
            self._log_retry_start(entry)
        elif action == "手动重试":
            self._log_retry_success(entry)
            self._update_monitor_health(entry)
            message += "，已更新调用日志和监控健康上报"
        elif action == "标记失败":
            evidence = self._close_exhausted(entry, reason="人工标记失败")
            self._alert_monitor(entry)
            message += f"，已生成结案证据{evidence['证据编号']}并触发监控告警"
        return entry, message

    def _record_failed_attempt(self, entry_id: int):
        """记录一次重试失败；尝试次数耗尽时结案，失败证据进入治理。"""
        task = store.find(self.MODULE, entry_id)
        if task is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if task.get("status") not in ("待重试", "重试中"):
            return None, f"任务当前状态为{task.get('status')}，不可记录重试失败"
        attempts = _to_int(task.get("重试次数")) + 1
        max_retry = max(1, _to_int(task.get("最大重试"), default=3))
        task["重试次数"] = attempts
        task["最后重试"] = TODAY
        self._log_attempt(task, attempts)
        if attempts >= max_retry:
            task["status"] = "永久失败"
            task["pending"] = False
            task["abnormal"] = True
            task["队列状态"] = "重试耗尽"
            evidence = self._close_exhausted(task, reason="尝试次数耗尽")
            self._alert_monitor(task)
            return task, (
                f"重放任务第{attempts}次重试失败，尝试次数已耗尽，"
                f"已生成结案证据{evidence['证据编号']}并触发监控告警"
            )
        task["status"] = "待重试"
        task["pending"] = True
        task["队列状态"] = "等待下次重试"
        return task, f"重放任务第{attempts}/{max_retry}次重试失败，已退回待重试"

    # ---------- 结案证据 ----------

    @staticmethod
    def _segment_of(task: dict) -> str:
        """任务关联的数据段：优先显式数据段，否则回退失败端点。"""
        return str(task.get("数据段") or task.get("失败端点") or "").strip()

    def _close_exhausted(self, task: dict, reason: str) -> dict:
        """重放耗尽结案：保留当时的失败证据快照，进入治理等待回执或裁决。"""
        for evidence in store.rows("closure_evidence"):
            if evidence.get("任务ID") == task.get("id"):
                return evidence  # 同一任务的耗尽记录只结案一次
        tid = str(task["id"]).zfill(4)
        snapshot = [
            {
                "记录编号": log.get("记录编号"),
                "响应状态": log.get("响应状态"),
                "错误信息": log.get("错误信息"),
                "调用时间": log.get("调用时间"),
            }
            for log in store.rows("call_log")
            if str(log.get("请求参数", "")).startswith(f"retry:task={tid}")
        ]
        rows = store.rows("closure_evidence")
        evidence = {
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "证据编号": f"EVID-{tid}",
            "任务编号": task.get("任务编号", ""),
            "任务ID": task.get("id"),
            "数据段": self._segment_of(task),
            "失败端点": task.get("失败端点", ""),
            "失败原因": task.get("失败原因", ""),
            "结案原因": reason,
            "已尝试次数": _to_int(task.get("重试次数")),
            "最大重试": _to_int(task.get("最大重试"), default=3),
            "失败快照": snapshot,
            "结案状态": EVIDENCE_OPEN,
            "补齐回执": "",
            "登记时间": TODAY,
        }
        rows.append(evidence)
        return evidence

    # ---------- 成功采集回执 ----------

    def register_receipt(self, call_id: str, segment: str, endpoint: str = "") -> dict:
        """登记成功采集回执：按数据段关联结案证据，同一成功调用只算一次补齐。"""
        call_id = str(call_id or "").strip()
        segment = str(segment or "").strip()
        if not call_id or not segment:
            return {"ok": False, "duplicate": False,
                    "message": "回执缺少调用编号或数据段", "receipt": None, "filled_evidence": []}
        receipts = store.rows("collection_receipt")
        for receipt in receipts:
            if receipt.get("调用编号") == call_id:
                return {
                    "ok": True, "duplicate": True,
                    "message": f"回执{call_id}已登记过，同一成功调用只算一次补齐",
                    "receipt": receipt, "filled_evidence": [],
                }
        receipt = {
            "id": max((int(r.get("id", 0)) for r in receipts), default=0) + 1,
            "回执编号": f"RCPT-{len(receipts) + 1:04d}",
            "调用编号": call_id,
            "数据段": segment,
            "来源端点": str(endpoint or "").strip(),
            "回执结果": "成功",
            "到达时间": TODAY,
        }
        receipts.append(receipt)
        # 关联结案证据：该数据段实际补到，待补采证据核销
        filled = []
        for evidence in store.rows("closure_evidence"):
            if evidence.get("结案状态") == EVIDENCE_OPEN and evidence.get("数据段") == segment:
                evidence["结案状态"] = EVIDENCE_FILLED
                evidence["补齐回执"] = receipt["回执编号"]
                filled.append(evidence["证据编号"])
        receipt["核销证据"] = filled
        # 同段仍在队列里的任务一并成功结案，已补到的区间不再占用重放待办
        closed_tasks = []
        for task in store.rows(self.MODULE):
            if task.get("status") in ("待重试", "重试中") and self._segment_of(task) == segment:
                task["status"] = "已成功"
                task["pending"] = False
                task["abnormal"] = False
                task["队列状态"] = "回执补齐"
                closed_tasks.append(task.get("任务编号"))
        receipt["联动任务"] = closed_tasks
        if filled:
            message = f"回执{receipt['回执编号']}已登记，数据段{segment}核销结案证据{len(filled)}份"
        else:
            message = f"回执{receipt['回执编号']}已登记，数据段{segment}无待补采结案证据"
        return {"ok": True, "duplicate": False,
                "message": message, "receipt": receipt, "filled_evidence": filled}

    # ---------- 补采候选与下一轮派发 ----------

    @staticmethod
    def _receipt_segments() -> set:
        """已经实际补到的数据段（有成功回执）。"""
        return {r.get("数据段") for r in store.rows("collection_receipt")}

    def backfill_candidates(self) -> list[dict]:
        """补采候选：待补采结案证据 + 历史失败记录（无证据的永久失败），扣除已补到的段。"""
        filled = self._receipt_segments()
        candidates: list[dict] = []
        seen: set = set()
        for evidence in store.rows("closure_evidence"):
            segment = evidence.get("数据段")
            if evidence.get("结案状态") == EVIDENCE_OPEN and segment not in filled and segment not in seen:
                seen.add(segment)
                candidates.append({
                    "数据段": segment,
                    "失败端点": evidence.get("失败端点", ""),
                    "来源": "结案证据",
                    "证据编号": evidence.get("证据编号", ""),
                    "已尝试次数": evidence.get("已尝试次数", 0),
                })
        evidenced_tasks = {e.get("任务ID") for e in store.rows("closure_evidence")}
        for task in store.rows(self.MODULE):
            if task.get("status") != "永久失败" or task.get("id") in evidenced_tasks:
                continue
            segment = self._segment_of(task)
            if not segment or segment in filled or segment in seen:
                continue
            seen.add(segment)
            candidates.append({
                "数据段": segment,
                "失败端点": task.get("失败端点", ""),
                "来源": "历史失败记录",
                "证据编号": "",
                "任务ID": task.get("id"),
                "已尝试次数": _to_int(task.get("重试次数")),
            })
        return candidates

    def sync_backlog(self) -> list[dict]:
        """裁决结果同步重放待办：历史失败先补录结案证据，再为剩余缺口派发待重试任务。"""
        tasks = store.rows(self.MODULE)
        backlog: list[dict] = []
        for candidate in self.backfill_candidates():
            segment = candidate["数据段"]
            if not candidate.get("证据编号"):
                evidence = self._backfill_legacy_evidence(candidate)
                candidate["证据编号"] = evidence["证据编号"]
            open_task = next(
                (t for t in tasks
                 if t.get("status") in ("待重试", "重试中") and self._segment_of(t) == segment),
                None,
            )
            if open_task is None:
                open_task = {
                    "id": max((int(r.get("id", 0)) for r in tasks), default=0) + 1,
                    "任务编号": f"RETR-BF-{len(tasks) + 1:04d}",
                    "失败端点": candidate.get("失败端点", ""),
                    "失败原因": f"治理裁决补采:{candidate['证据编号']}",
                    "重试次数": 0,
                    "最大重试": 3,
                    "退避策略": "fixed:10s",
                    "最后重试": "",
                    "队列状态": "治理补采",
                    "数据段": segment,
                    "status": "待重试", "pending": True, "abnormal": False,
                }
                tasks.append(open_task)
            backlog.append(open_task)
        return backlog

    def _backfill_legacy_evidence(self, candidate: dict) -> dict:
        """为历史失败记录补录结案证据，让旧失败尝试纳入治理闭环后再派发。"""
        rows = store.rows("closure_evidence")
        task_id = candidate.get("任务ID")
        task = store.find(self.MODULE, task_id) if task_id is not None else None
        evidence = {
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "证据编号": f"EVID-LEG-{len(rows) + 1:04d}",
            "任务编号": (task or {}).get("任务编号", ""),
            "任务ID": task_id,
            "数据段": candidate["数据段"],
            "失败端点": candidate.get("失败端点", ""),
            "失败原因": "历史失败记录补录",
            "结案原因": "历史补录",
            "已尝试次数": candidate.get("已尝试次数", 0),
            "最大重试": _to_int((task or {}).get("最大重试"), default=0),
            "失败快照": [],
            "结案状态": EVIDENCE_OPEN,
            "补齐回执": "",
            "登记时间": TODAY,
        }
        rows.append(evidence)
        return evidence

    def plan_next_dispatch(self) -> dict:
        """下一轮重放派发核对：逐段检查是否仍缺采、旧失败尝试是否已处理、是否已补到。"""
        filled = self._receipt_segments()
        evidence_rows = store.rows("closure_evidence")
        tasks = store.rows(self.MODULE)
        segments: dict[str, None] = {}
        for evidence in evidence_rows:
            segments.setdefault(evidence.get("数据段"), None)
        for task in tasks:
            if task.get("status") == "永久失败":
                segments.setdefault(self._segment_of(task), None)
        plans: list[dict] = []
        for segment in segments:
            evidences = [e for e in evidence_rows if e.get("数据段") == segment]
            failed_tasks = [
                t for t in tasks
                if t.get("status") == "永久失败" and self._segment_of(t) == segment
            ]
            handled = bool(evidences) and all(
                any(e.get("任务ID") == t.get("id") for e in evidences) for t in failed_tasks
            )
            backfilled = segment in filled or any(
                e.get("结案状态") == EVIDENCE_FILLED for e in evidences
            )
            open_task = next(
                (t for t in tasks
                 if t.get("status") in ("待重试", "重试中") and self._segment_of(t) == segment),
                None,
            )
            still_missing = not backfilled
            plans.append({
                "数据段": segment,
                "仍缺采": still_missing,
                "已补到": backfilled,
                "旧失败已处理": handled,
                "待办任务": (open_task or {}).get("任务编号", ""),
                "可派发": still_missing and handled and open_task is None,
            })
        plans.sort(key=lambda item: item["数据段"])
        return {
            "plans": plans,
            "dispatch": [p["数据段"] for p in plans if p["可派发"]],
            "pending_segments": [p["数据段"] for p in plans if p["仍缺采"]],
        }

    def lost_request_stats(self) -> dict:
        """失联请求统计：与治理报告汇总同源，均取自实际待补范围。"""
        candidates = self.backfill_candidates()
        evidence_rows = store.rows("closure_evidence")
        return {
            "pending_count": len(candidates),
            "pending_segments": [c["数据段"] for c in candidates],
            "evidence_open": sum(1 for e in evidence_rows if e.get("结案状态") == EVIDENCE_OPEN),
            "evidence_filled": sum(1 for e in evidence_rows if e.get("结案状态") == EVIDENCE_FILLED),
            "receipts_total": len(store.rows("collection_receipt")),
        }

    # ---------- 联动写日志/监控 ----------

    def _log_retry_start(self, task: dict):
        log_rows = store.rows("call_log")
        tid = str(task["id"]).zfill(4)
        log_rows.append({
            "id": max((int(r.get("id", 0)) for r in log_rows), default=0) + 1,
            "记录编号": f"CALL-RETRY-{tid}",
            "接口端点": task.get("失败端点", ""),
            "请求参数": f"retry:task={tid}",
            "响应状态": "retrying",
            "响应耗时(ms)": 0,
            "返回行数": 0,
            "错误信息": task.get("失败原因", ""),
            "调用时间": TODAY,
            "status": "超时", "pending": True, "abnormal": True,
        })

    def _log_attempt(self, task: dict, attempt: int):
        """每次重试失败写一条调用日志，作为结案证据失败快照的来源。"""
        log_rows = store.rows("call_log")
        tid = str(task["id"]).zfill(4)
        log_rows.append({
            "id": max((int(r.get("id", 0)) for r in log_rows), default=0) + 1,
            "记录编号": f"CALL-RETRY-{tid}-{attempt}",
            "接口端点": task.get("失败端点", ""),
            "请求参数": f"retry:task={tid}",
            "响应状态": "timeout",
            "响应耗时(ms)": 0,
            "返回行数": 0,
            "错误信息": f"第{attempt}次重试失败:{task.get('失败原因', '')}",
            "调用时间": TODAY,
            "status": "超时", "pending": True, "abnormal": True,
        })

    def _log_retry_success(self, task: dict):
        tid = str(task["id"]).zfill(4)
        for log in store.rows("call_log"):
            if log.get("记录编号") == f"CALL-RETRY-{tid}":
                log["status"] = "成功"
                log["响应状态"] = "200"
                log["响应耗时(ms)"] = 150
                log["返回行数"] = 1
                break

    def _update_monitor_health(self, task: dict):
        """重试成功后联动刷新监控健康度。"""
        endpoint = task.get("失败端点", "")
        for mon in store.rows("api_monitor"):
            if mon.get("目标端点") == endpoint:
                mon["最近采样"] = TODAY
                if mon.get("status") == "已告警":
                    mon["status"] = "监控中"
                    mon["pending"] = True

    def _alert_monitor(self, task: dict):
        """重放耗尽触发监控告警；结案证据已另行生成，不再直接产生异常报告。"""
        endpoint = task.get("失败端点", "")
        for mon in store.rows("api_monitor"):
            if mon.get("目标端点") == endpoint:
                mon["status"] = "已告警"
                mon["pending"] = False
                mon["最近采样"] = TODAY

    def calc_recovery_rate(self) -> dict:
        """跨模块恢复率统计：分析重放队列成功/失败分布、结案证据及关联调用日志。"""
        retries = store.rows("retry_queue")
        total = len(retries)
        success = sum(1 for r in retries if r.get("status") == "已成功")
        perm_fail = sum(1 for r in retries if r.get("status") == "永久失败")
        # 关联调用日志
        logs_related = [
            l for l in store.rows("call_log")
            if "retry" in str(l.get("请求参数", ""))
        ]
        lost = self.lost_request_stats()
        return {
            "total_retries": total,
            "success": success,
            "permanent_fail": perm_fail,
            "recovery_rate": success / total if total else 0.0,
            "logged_events": len(logs_related),
            "closure_evidence": len(store.rows("closure_evidence")),
            "backfilled_segments": lost["evidence_filled"],
            "pending_backfill": lost["pending_count"],
        }
