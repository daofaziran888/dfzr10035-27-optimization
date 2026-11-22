"""重放耗尽结案证据场景核对（直接运行：python3 tests/test_replay_closure.py）。

场景：准备两段采集数据并耗尽各自尝试次数 → 结案证据进入治理 →
报告生成登记两段缺口（中断重试重新核对）→ 封存期间收到第二段迟到成功回执 →
归档重新核对剩余段并裁决同步重放待办 → 核对下一轮实际派发。
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.govern_report import GovernReportService
from app.services.retry_queue import RetryQueueService
from app.store import store

SEG_A = "SEG-2026Q4-A"
SEG_B = "SEG-2026Q4-B"

retry = RetryQueueService()
govern = GovernReportService()


def check(condition: bool, label: str):
    if not condition:
        raise AssertionError(f"核对失败：{label}")
    print(f"  ✓ {label}")


def evidence_of(segment: str) -> dict | None:
    for ev in store.rows("closure_evidence"):
        if ev.get("数据段") == segment:
            return ev
    return None


print("1) 准备两段采集数据的重放任务（各自最大重试不同）")
task_a, missing = retry.create_entry({
    "任务编号": "RETR-SEG-A", "失败端点": "/api/orders/daily", "失败原因": "上游超时",
    "重试次数": 0, "最大重试": 2, "退避策略": "fixed:5s", "最后重试": "",
    "队列状态": "待调度", "数据段": SEG_A,
})
task_b, missing = retry.create_entry({
    "任务编号": "RETR-SEG-B", "失败端点": "/api/orders/hourly", "失败原因": "限流拒绝",
    "重试次数": 0, "最大重试": 3, "退避策略": "exp:3s", "最后重试": "",
    "队列状态": "待调度", "数据段": SEG_B,
})
check(task_a is not None and task_b is not None, "两段采集任务登记成功")
check(task_a.get("数据段") == SEG_A and task_b.get("数据段") == SEG_B, "任务携带关联数据段")

print("2) 耗尽各自尝试次数，生成结案证据")
for task, max_retry in ((task_a, 2), (task_b, 3)):
    entry = None
    for _ in range(max_retry):
        retry.run_action(task["id"], "加入队列")
        entry, _ = retry.run_action(task["id"], "重试失败")
    check(entry["status"] == "永久失败", f"{task['任务编号']} 第{max_retry}次失败后重试耗尽")
evidences = store.rows("closure_evidence")
check(len(evidences) == 2, "两段耗尽记录各生成一份结案证据")
ev_a, ev_b = evidence_of(SEG_A), evidence_of(SEG_B)
check(ev_a["结案状态"] == "待补采" and ev_b["结案状态"] == "待补采", "结案证据初始均为待补采")
check(ev_a["已尝试次数"] == 2 and ev_b["已尝试次数"] == 3, "结案证据保留各自耗尽时的尝试次数")
snap_a = [s for s in ev_a["失败快照"] if "重试失败" in str(s["错误信息"])]
snap_b = [s for s in ev_b["失败快照"] if "重试失败" in str(s["错误信息"])]
check(len(snap_a) == 2 and len(snap_b) == 3, "失败快照保留当时的全部失败尝试证据")
check(ev_a["数据段"] == SEG_A and ev_b["数据段"] == SEG_B, "失败证据能证明关联的数据段")
check(
    not any(str(r.get("报告编号", "")).startswith("GOVE-RETRY") for r in store.rows("govern_report")),
    "重放耗尽不再直接产生异常报告",
)

print("3) 治理登记报告并生成，登记两段剩余缺口；中断重试重新核对")
report, _ = govern.create_entry({
    "报告编号": "GOVE-COLL-0001", "评估范围": f"采集失败结案:{SEG_A},{SEG_B}",
    "合规指标": "数据采集完整性", "当前得分": 0, "改进事项": "",
    "评估周期": "2026-Q4", "负责人": "治理值班", "生成时间": "",
})
report, msg = govern.run_action(report["id"], "生成报告")
check(report["剩余缺口"] == [SEG_A, SEG_B], "报告按结案证据登记两段剩余缺口")
check(report["stats"]["lost_requests"] == 2, "报告汇总失联请求为2段")
report, _ = govern.run_action(report["id"], "生成报告")  # 生成中断后重试
check(report["剩余缺口"] == [SEG_A, SEG_B], "生成中断重试后重新核对剩余范围结果一致")
check(report["改进事项"].count("剩余缺口") == 1, "重复生成不重复叠加改进事项")
report, _ = govern.run_action(report["id"], "提交审核")
check(report["status"] == "已审核", "报告提交审核，进入封存期间")

print("4) 封存期间收到第二段迟到成功回执，同一成功调用只算一次补齐")
result = retry.register_receipt("CALL-LATE-9001", SEG_B, "/api/orders/hourly")
check(result["ok"] and not result["duplicate"], "第二段成功回执登记成功")
check(result["filled_evidence"] == [ev_b["证据编号"]], "回执关联核销第二段结案证据")
again = retry.register_receipt("CALL-LATE-9001", SEG_B, "/api/orders/hourly")
check(again["duplicate"] and again["filled_evidence"] == [], "同一成功调用重复登记被去重")
check(len(store.rows("collection_receipt")) == 1, "成功回执只登记一次")
check(ev_b["结案状态"] == "已补齐" and ev_b["补齐回执"] == "RCPT-0001", "第二段证据已补齐并登记回执编号")
check(ev_a["结案状态"] == "待补采", "第一段证据仍待补采")
check(
    ev_b["已尝试次数"] == 3 and len([s for s in ev_b["失败快照"] if "重试失败" in str(s["错误信息"])]) == 3,
    "补齐后结案证据仍保留当时的失败证据",
)

print("5) 归档报告：重新核对剩余段，裁决结果同步重放待办")
report, _ = govern.run_action(report["id"], "归档报告")
check(report["status"] == "已归档", "报告归档完成")
check(report["剩余缺口"] == [SEG_A], "迟到回执覆盖第二段，剩余缺口只剩第一段")
check(report["stats"]["lost_requests"] == 1, "归档后报告汇总失联请求为1段")
backlog = [t for t in store.rows("retry_queue") if t.get("队列状态") == "治理补采"]
check(len(backlog) == 1 and backlog[0].get("数据段") == SEG_A, "裁决只为第一段同步重放待办")
check(backlog[0]["status"] == "待重试", "补采任务进入待重试")
check(report["重放待办"] == [backlog[0]["任务编号"]], "报告登记裁决同步的重放待办")

print("6) 下一轮派发核对：第一段缺采且旧失败已处理，第二段已补到不再是候选")
plan = retry.plan_next_dispatch()
plans = {p["数据段"]: p for p in plan["plans"]}
check(plans[SEG_A]["仍缺采"] and plans[SEG_A]["旧失败已处理"], "第一段仍缺采且旧失败尝试已处理")
check(plans[SEG_A]["待办任务"] == backlog[0]["任务编号"] and not plans[SEG_A]["可派发"],
      "第一段已有待办，下一轮不重复派发")
check(plans[SEG_B]["已补到"] and not plans[SEG_B]["可派发"], "第二段已补到，不可继续成为补采候选")
check(SEG_B not in plan["pending_segments"] and plan["pending_segments"] == [SEG_A],
      "下一轮实际待补范围只有第一段")
candidates = retry.backfill_candidates()
check([c["数据段"] for c in candidates] == [SEG_A], "补采候选只剩第一段")

print("7) 报告汇总与失联请求统计对得上实际待补范围")
lost = retry.lost_request_stats()
check(lost["pending_count"] == report["stats"]["lost_requests"] == 1, "失联统计与报告汇总一致")
check(lost["pending_segments"] == report["剩余缺口"] == [SEG_A], "统计口径与实际待补范围一致")
check(lost["evidence_filled"] == 1 and lost["receipts_total"] == 1, "补齐证据与回执各计一次")

print("8) 补采兼容历史失败记录的查询与派发")
legacy = {
    "id": max(int(t["id"]) for t in store.rows("retry_queue")) + 1,
    "任务编号": "RETR-LEGACY-1", "失败端点": "/api/legacyFeed", "失败原因": "历史遗留失败",
    "重试次数": 3, "最大重试": 3, "退避策略": "fixed:30s", "最后重试": "2026-10-01",
    "队列状态": "历史遗留", "数据段": "SEG-LEGACY",
    "status": "永久失败", "pending": False, "abnormal": True,
}
store.rows("retry_queue").append(legacy)
legacy_cand = [c for c in retry.backfill_candidates() if c["数据段"] == "SEG-LEGACY"]
check(len(legacy_cand) == 1 and legacy_cand[0]["来源"] == "历史失败记录", "历史失败记录进入补采候选")
check(evidence_of("SEG-LEGACY") is None, "历史失败暂无结案证据")
retry.sync_backlog()
check(evidence_of("SEG-LEGACY") is not None, "裁决同步时为历史失败补录结案证据")
plans = {p["数据段"]: p for p in retry.plan_next_dispatch()["plans"]}
check(plans["SEG-LEGACY"]["旧失败已处理"] and plans["SEG-LEGACY"]["仍缺采"],
      "历史失败旧尝试已处理且仍缺采")
check(
    any(t.get("数据段") == "SEG-LEGACY" and t["status"] == "待重试" for t in store.rows("retry_queue")),
    "历史失败段已派发待重试任务",
)

print("9) 已归档报告不可回退重新生成")
entry, message = govern.run_action(report["id"], "生成报告")
check(entry is None and "不可执行" in message, "归档后重新生成被拒绝")

print("\n全部核对通过：结案证据、迟到回执、剩余缺口、裁决同步与下一轮派发均一致。")
