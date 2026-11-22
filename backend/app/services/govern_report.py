"""治理报告领域服务：管理数据质量与合规报告，与 pipeline_audit、data_version、call_log、api_monitor、retry_queue 联动。

采集失败结案证据驱动的缺口登记：
- 生成报告时按结案证据登记剩余数据缺口，生成中断后重试可重新核对剩余范围；
- 归档前再次核对，封存期间迟到的成功回执覆盖的段从剩余缺口移除；
- 裁决结果同步重放待办，报告汇总与失联请求统计同源，对得上实际待补范围。
"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.retry_queue import RetryQueueService
from app.store import store

TODAY = "2026-11-22"


class GovernReportService(BaseModuleService):
    MODULE = "govern_report"
    ENTITY = "治理报告"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["报告编号", "评估范围", "合规指标", "当前得分", "改进事项", "评估周期", "负责人", "生成时间"]
    REQUIRED_FIELDS = ["报告编号", "评估范围", "合规指标"]
    STATUS_ORDER = ["待生成", "已生成", "已审核", "已归档"]
    ACTION_RULES = {"生成报告": "已生成", "提交审核": "已审核", "归档报告": "已归档"}
    NEGATIVE_ACTIONS = []
    # 流程守卫：生成中断可重试（待生成/已生成可重复生成），审核后进入封存期，归档后不可回退
    FLOW_GUARD = {
        "生成报告": ("待生成", "已生成"),
        "提交审核": ("已生成",),
        "归档报告": ("已审核",),
    }

    def run_action(self, entry_id: int, action: str):
        """生成时按结案证据登记剩余缺口；归档前重新核对并裁决同步重放待办。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        allowed = self.FLOW_GUARD.get(action)
        if allowed is not None and entry.get("status") not in allowed:
            return None, f"报告当前状态为{entry.get('status')}，不可执行{action}"
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if action == "生成报告":
            self._collect_cross_module_data(entry)
            gaps = self._register_remaining_gaps(entry)
            message += f"，已采集跨模块全景数据，按结案证据登记剩余缺口{len(gaps)}段"
        elif action == "提交审核":
            self._audit_report(entry)
            message += "，已记录审计并写入调用日志"
        elif action == "归档报告":
            self._finalize_adjudication(entry)
            self._snapshot_data_version(entry)
            message += "，已重新核对剩余缺口、裁决同步重放待办并创建数据版本快照"
        return entry, message

    def _collect_cross_module_data(self, report: dict):
        """生成报告时采集跨模块统计。"""
        total_calls = len(store.rows("call_log"))
        pending_retries = sum(1 for r in store.rows("retry_queue") if r.get("status") == "待重试")
        alerts = sum(1 for m in store.rows("api_monitor") if m.get("status") == "已告警")
        broken = sum(1 for rl in store.rows("rate_limit") if rl.get("status") == "已熔断")
        pipeline_errors = sum(1 for d in store.rows("data_delivery") if d.get("status") == "投递异常")
        # 基于统计计算综合得分
        health = 100.0
        health -= 5 * pending_retries
        health -= 10 * alerts
        health -= 15 * broken
        health -= 10 * pipeline_errors
        report["当前得分"] = max(0.0, health)
        report["改进事项"] = (
            f"待重试{pending_retries}项/告警{alerts}条/"
            f"熔断{broken}路/投递异常{pipeline_errors}路"
        )
        report["stats"] = {
            "total_calls": total_calls,
            "pending_retries": pending_retries,
            "active_alerts": alerts,
            "circuit_breaks": broken,
            "pipeline_errors": pipeline_errors,
        }

    def _register_remaining_gaps(self, report: dict) -> list[str]:
        """按结案证据登记剩余数据缺口；重复生成时重新核对，中断重试结果一致。"""
        candidates = RetryQueueService().backfill_candidates()
        segments = [c["数据段"] for c in candidates]
        report["剩余缺口"] = segments
        report["结案证据"] = [c["证据编号"] for c in candidates if c.get("证据编号")]
        stats = report.setdefault("stats", {})
        stats["lost_requests"] = len(segments)
        stats["backfilled_segments"] = sum(
            1 for e in store.rows("closure_evidence") if e.get("结案状态") == "已补齐"
        )
        report["改进事项"] = f"{report.get('改进事项', '')}/剩余缺口{len(segments)}段"
        return segments

    def _finalize_adjudication(self, report: dict):
        """归档前重新核对剩余范围：迟到回执覆盖的段移出缺口，裁决结果同步重放待办。"""
        retry = RetryQueueService()
        candidates = retry.backfill_candidates()
        segments = [c["数据段"] for c in candidates]
        backlog = retry.sync_backlog()
        report["剩余缺口"] = segments
        report["结案证据"] = [c["证据编号"] for c in candidates if c.get("证据编号")]
        report["重放待办"] = [t.get("任务编号") for t in backlog]
        report["裁决结果"] = (
            f"归档核对：剩余缺口{len(segments)}段"
            f"（{'、'.join(segments) if segments else '无'}），"
            f"已同步重放待办{len(backlog)}项"
        )
        stats = report.setdefault("stats", {})
        stats["lost_requests"] = len(segments)
        stats["backlog_synced"] = len(backlog)
        stats["backfilled_segments"] = sum(
            1 for e in store.rows("closure_evidence") if e.get("结案状态") == "已补齐"
        )
        report["改进事项"] = (
            f"剩余缺口{len(segments)}段待补采，"
            f"迟到回执已核销{stats['backfilled_segments']}段"
        )

    def _audit_report(self, report: dict):
        """审核报告时写审计日志和调用日志。"""
        rid = str(report["id"]).zfill(4)
        # 管道审计
        audit_rows = store.rows("pipeline_audit")
        audit_rows.append({
            "id": max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1,
            "记录编号": f"PIPE-GOV-{rid}",
            "管道名称": f"govern_report:{rid}",
            "操作人": report.get("负责人", "system"),
            "操作类型": "审核",
            "变更摘要": f"治理报告{rid}提交审核",
            "操作时间": TODAY,
            "来源节点": "govern_report",
            "审计结论": "待复核",
            "status": "待复核", "pending": True, "abnormal": False,
        })
        # 调用日志
        log_rows = store.rows("call_log")
        log_rows.append({
            "id": max((int(r.get("id", 0)) for r in log_rows), default=0) + 1,
            "记录编号": f"CALL-GOV-{rid}",
            "接口端点": "govern_report",
            "请求参数": report.get("评估范围", ""),
            "响应状态": "200",
            "响应耗时(ms)": 200,
            "返回行数": 1,
            "错误信息": "",
            "调用时间": TODAY,
            "status": "成功", "pending": False, "abnormal": False,
        })

    def _snapshot_data_version(self, report: dict):
        """归档时创建数据版本快照。"""
        ver_rows = store.rows("data_version")
        rid = str(report["id"]).zfill(4)
        ver_rows.append({
            "id": max((int(r.get("id", 0)) for r in ver_rows), default=0) + 1,
            "版本编号": f"DATA-GOV-{rid}",
            "数据来源": "govern_report",
            "行数统计": 1,
            "版本标签": "report_archived",
            "Schema摘要": report.get("合规指标", ""),
            "创建原因": f"治理报告{rid}已归档",
            "依赖版本": report.get("评估周期", ""),
            "创建时间": TODAY,
            "status": "可用", "pending": False, "abnormal": False,
        })

    def calc_govern_score(self) -> dict:
        """跨模块治理全景评分。"""
        lost = RetryQueueService().lost_request_stats()
        return {
            "reports_total": len(store.rows("govern_report")),
            "reports_pending": sum(1 for r in store.rows("govern_report") if r.get("status") == "待生成"),
            "audits_open": sum(1 for a in store.rows("pipeline_audit") if a.get("status") == "待复核"),
            "data_versions": len(store.rows("data_version")),
            "call_anomalies": sum(1 for c in store.rows("call_log") if c.get("abnormal")),
            "lost_requests": lost["pending_count"],
            "backfilled_segments": lost["evidence_filled"],
        }
