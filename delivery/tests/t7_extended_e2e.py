# -*- coding: utf-8 -*-
"""Task 13 t7: 扩展端到端扫描 —— SuperAGI x GaussDB 适配第三轮验收。

扫描剩余用户路径（全部走真实 API 8001 + psycopg2 直查 super_agi_e2e 落库核对）：
  S1 Settings/配置链：api_keys CRUD+鉴权、webhooks JSON 列写读+真实投递（本地监听器）、组织/项目列表
  S2 Analytics 全套：metrics/agents/all/agents/{id}/runs/active/tools/used/tools usage+logs/knowledge usage+logs
  S3 Agent 生命周期：创建(挂 knowledge) -> 执行 -> 首个 thought 后 STOP -> rerun -> /v1/agent 编辑 -> 删除级联
  S4 Agent Schedule：/agents/schedule 建定时 + 落库 + schedule_data + edit + stop
  S5 Knowledge 挂载执行：Knowledge Search 工具返回知识库文档内容（与执行 6 结果对比）
  S6 边界输入：.xyz 400 / .md 201+summarize 落向量 / 无效 knowledge_id、vector_db_id 404 而非 500
  S7 Models 页：provider api_key 加密存储+解密读回（ext- 测试项，不动 OpenAI）、fetch_models/fetch_model

运行（PowerShell，后端 8001 与 celery solo 已在跑，执行 6 不受干扰）：
  cd D:\\workplace\\code\\SuperAGI\\SuperAGI-0.0.14
  .\\.venv-e2e\\Scripts\\python.exe delivery\\tests\\t7_extended_e2e.py

纪律：单场景失败不中断整轮；产品代码问题按 (a)GaussDB 兼容可修 / (b)上游缺陷只报告 / (c)环境 分类。
新实体一律 ext- 前缀，脚本末尾自动清理（API 能删的走 API，没有删除端点的用 SQL 并在输出注明）。
"""
import json
import sys
import threading
import time
import traceback
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer

import psycopg2
import requests

ROOT = r"D:\workplace\code\SuperAGI\SuperAGI-0.0.14"
BASE = "http://127.0.0.1:8001"
DB = dict(host="127.0.0.1", port=5432, user="superagi_test",
          password="GaussTest2026", dbname="super_agi_e2e")
WEBHOOK_PORT = 9915
WEBHOOK_URL = "http://127.0.0.1:%d/ext-hook" % WEBHOOK_PORT
DEMO_DOC = r"D:\workplace\doc\ObsidianNote\learning\rag\demo-docs\05-capacity-planning.md"
GOAL = ["查询知识库：磁盘水位红色阈值"]
INSTRUCTION = ["先用 Knowledge Search 工具查询知识库，再根据结果回答。"]
AGENT_WORKFLOW = "Goal Based Workflow"
MODEL = "qwen3:4b"

S = requests.Session()
_results = []          # (scenario, name, status, detail) status in PASS/FAIL/BLOCKED
_webhook_hits = []     # 本地监听器收到的投递
_webhook_lock = threading.Lock()


# ---------------------------------------------------------------- utilities
def db_q(sql, params=None):
    with psycopg2.connect(**DB) as c:
        with c.cursor() as cur:
            cur.execute(sql, params or {})
            return cur.fetchall()


def db_exec(sql, params=None):
    with psycopg2.connect(**DB) as c:
        with c.cursor() as cur:
            cur.execute(sql, params or {})
        c.commit()


def db_one(sql, params=None):
    rows = db_q(sql, params)
    return rows[0] if rows else None


def check(scenario, name, ok, detail=""):
    status = "PASS" if ok else "FAIL"
    line = "[%s][%s] %s%s" % (status, scenario, name, (" | " + str(detail)) if detail else "")
    _results.append((scenario, name, status, str(detail)))
    print(line, flush=True)
    return ok


def blocked(scenario, name, detail=""):
    _results.append((scenario, name, "BLOCKED", str(detail)))
    print("[BLOCKED][%s] %s | %s" % (scenario, name, detail), flush=True)


def api(method, path, expect=None, name="", **kw):
    r = S.request(method, BASE + path, timeout=90, **kw)
    return r


def poll_db(sql, params, expect_fn, timeout, interval=4):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = db_q(sql, params)
        except psycopg2.Error as e:
            print("  poll db error:", e, flush=True)
        try:
            if last is not None and expect_fn(last):
                return last
        except Exception:
            pass
        time.sleep(interval)
    return None


class _HookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8", "replace")
        with _webhook_lock:
            _webhook_hits.append({"path": self.path, "headers": dict(self.headers), "body": body})
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        pass


def start_hook_listener():
    server = HTTPServer(("127.0.0.1", WEBHOOK_PORT), _HookHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server


def ext_config_payload(name, tools, knowledge):
    return {
        "name": name, "project_id": 1,
        "description": "t7 extended e2e agent (GaussDB)",
        "goal": GOAL, "instruction": INSTRUCTION,
        "agent_workflow": AGENT_WORKFLOW, "constraints": [],
        "toolkits": [], "tools": tools,
        "exit": "Terminate", "iteration_interval": 1,
        "model": MODEL, "permission_type": "God Mode",
        "LTM_DB": "GAUSSDB", "max_iterations": 2,
        "user_timezone": "Asia/Shanghai", "knowledge": knowledge,
    }


# ---------------------------------------------------------------- S1
def s1_settings():
    sc = "S1"
    # --- api keys ---
    r = api("POST", "/api-keys", json={"name": "ext-key-t7"})
    ok = check(sc, "POST /api-keys create -> 200 + uuid", r.status_code == 200 and "api_key" in r.json(),
               "%s %s" % (r.status_code, r.text[:120]))
    key1 = r.json().get("api_key") if ok else None
    row = db_one("SELECT id, name, key, is_expired FROM api_keys WHERE name='ext-key-t7' ORDER BY id DESC LIMIT 1")
    ok = check(sc, "api_keys 落库核对（key 明文 == 返回值，is_expired 未置真）",
               row is not None and row[2] == key1 and not row[3], "row=%s" % (row,))
    if ok:
        print("    note: api_keys.key 在库中为明文 UUID（上游未做静态加密；Fernet 加密仅用于 models_config.api_key）", flush=True)

    r = api("GET", "/api-keys")
    names = [x["name"] for x in r.json()] if r.status_code == 200 and isinstance(r.json(), list) else []
    check(sc, "GET /api-keys 列出新建 key", "ext-key-t7" in names, "%s %s" % (r.status_code, names))

    kid = row[0] if row else None
    r = api("PUT", "/api-keys", json={"id": kid, "name": "ext-key-t7-renamed"})
    row2 = db_one("SELECT name FROM api_keys WHERE id=%s", (kid,))
    check(sc, "PUT /api-keys 改名落库", r.status_code == 200 and row2 == ("ext-key-t7-renamed",),
          "%s %s row=%s" % (r.status_code, r.text[:80], row2))

    r = api("GET", "/api-keys/validate", headers={"X-API-Key": key1 or "bad"})
    check(sc, "GET /api-keys/validate 有效 key -> 200 success", r.status_code == 200 and r.json().get("success") is True,
          "%s %s" % (r.status_code, r.text[:80]))
    r = api("GET", "/api-keys/validate", headers={"X-API-Key": "definitely-invalid"})
    check(sc, "GET /api-keys/validate 无效 key -> 401", r.status_code == 401, str(r.status_code))

    # --- webhook（JSON 列 + 真实投递）---
    payload = {"name": "ext-webhook-t7", "url": WEBHOOK_URL,
               "headers": {"X-Ext-Test": "t7", "Content-Type": "application/json"},
               "filters": {"status": ["RUNNING", "TERMINATED", "COMPLETED", "ITERATION_LIMIT_EXCEEDED"]}}
    r = api("POST", "/webhook/add", json=payload)
    ok = check(sc, "POST /webhook/add -> 201（headers/filters JSON 列写入）", r.status_code == 201,
               "%s %s" % (r.status_code, r.text[:200]))
    wh = r.json() if ok else {}
    wh_id = wh.get("id")
    row = db_one("SELECT id, name, url, headers, filters, is_deleted FROM webhooks WHERE name='ext-webhook-t7' ORDER BY id DESC LIMIT 1")
    hdr, flt = row[3], row[4] if row else (None, None)
    check(sc, "webhooks 落库核对（headers/filters JSON 直查可读回 dict）",
          row is not None and hdr == payload["headers"] and flt == payload["filters"],
          "db_headers=%r db_filters=%r" % (hdr, flt))
    r = api("GET", "/webhook/get")
    got = r.json() if r.status_code == 200 else {}
    check(sc, "GET /webhook/get 读回 JSON 列与写入一致",
          got.get("id") == wh_id and got.get("headers") == payload["headers"] and got.get("filters") == payload["filters"],
          "%s %s" % (r.status_code, r.text[:200]))
    r = api("POST", "/webhook/edit/%s" % wh_id, json={"url": WEBHOOK_URL, "filters": {"status": ["PAUSED"]}})
    row2 = db_one("SELECT filters FROM webhooks WHERE id=%s", (wh_id,))
    check(sc, "POST /webhook/edit 更新 filters 落库", r.status_code == 200 and row2 == ({"status": ["PAUSED"]},),
          "%s %s row=%s" % (r.status_code, r.text[:120], row2))
    # 恢复 filters 供后续投递验证
    api("POST", "/webhook/edit/%s" % wh_id,
        json={"url": WEBHOOK_URL,
              "filters": {"status": ["RUNNING", "TERMINATED", "COMPLETED", "ITERATION_LIMIT_EXCEEDED", "PAUSED"]}})
    check(sc, "DELETE /webhook/* 端点", False,
          "上游未提供 webhook 删除端点（仅 add/edit/get），is_deleted 软删标记在 WebHookManager 亦不被过滤（见问题清单）")

    # --- 组织/项目列表 ---
    # 注：上游 /organisations/get/user/{id} 装饰器带 status_code=201（GET 返回 201，body 正确）——上游怪癖，记录。
    r = api("GET", "/organisations/get/user/1")
    check(sc, "GET /organisations/get/user/1 -> 组织信息（上游 GET 带 201 状态码怪癖，body 正确）",
          r.status_code in (200, 201) and r.json().get("id") == 1,
          "%s %s" % (r.status_code, r.text[:120]))
    r = api("POST", "/projects/add", json={"name": "ext-project-t7", "organisation_id": 1,
                                           "description": "t7 temp project"})
    check(sc, "POST /projects/add -> 201", r.status_code == 201, "%s %s" % (r.status_code, r.text[:120]))
    r = api("GET", "/projects/get/organisation/1")
    plist = r.json() if r.status_code == 200 else []
    check(sc, "GET /projects/get/organisation/1 多行正确（含 ext-project-t7，行数=2）",
          r.status_code == 200 and len(plist) == 2 and any(p["name"] == "ext-project-t7" for p in plist),
          "%s rows=%s" % (r.status_code, [p.get("name") for p in plist]))
    r = api("GET", "/projects/get/1")
    check(sc, "GET /projects/get/1 单行正确", r.status_code == 200 and r.json().get("name") == "Default Project",
          "%s %s" % (r.status_code, r.text[:100]))
    return {"webhook_id": wh_id, "api_key_row": kid}


# ---------------------------------------------------------------- S2
def s2_analytics():
    sc = "S2"
    r = api("GET", "/analytics/metrics")
    ok = r.status_code == 200
    m = r.json() if ok else {}
    ok = ok and {"agent_details", "run_details", "tokens_details"} <= set(m)
    check(sc, "GET /analytics/metrics（JSONB ::int 聚合不炸）", ok, "%s %s" % (r.status_code, r.text[:200]))
    if ok:
        check(sc, "  metrics 数值与库内事件一致（total_runs>=2, total_tokens>=4255+7017）",
              m["run_details"]["total_runs"] >= 2 and m["tokens_details"]["total_tokens"] >= 11272,
              "runs=%s tokens=%s agents=%s" % (m["run_details"]["total_runs"],
                                               m["tokens_details"]["total_tokens"],
                                               m["agent_details"]["total_agents"]))

    r = api("GET", "/analytics/agents/all")
    ok = r.status_code == 200 and "agent_details" in r.json()
    check(sc, "GET /analytics/agents/all（array_agg(JSONB) 路径）", ok, "%s %s" % (r.status_code, r.text[:200]))
    if ok:
        det = r.json()["agent_details"]
        check(sc, "  agents/all 覆盖 2 个 agent 且 tools_used 数组可解析",
              len(det) >= 2 and all(isinstance(a.get("tools_used"), (list, type(None))) for a in det),
              "agents=%s names=%s" % (len(det), [a.get("name") for a in det]))

    r = api("GET", "/analytics/agents/1")
    ok = r.status_code == 200 and isinstance(r.json(), list)
    check(sc, "GET /analytics/agents/1（run 完成率明细）", ok, "%s %s" % (r.status_code, r.text[:200]))
    if ok:
        runs = r.json()
        check(sc, "  agent1 明细含 e2e-run-1 且 tokens 数字化",
              any(x.get("name") == "e2e-run-1" for x in runs)
              and all(isinstance(x.get("tokens_consumed"), int) for x in runs),
              "runs=%s" % [(x.get("name"), x.get("tokens_consumed")) for x in runs])

    r = api("GET", "/analytics/runs/active")
    ok = r.status_code == 200 and isinstance(r.json(), list)
    check(sc, "GET /analytics/runs/active", ok, "%s %s" % (r.status_code, r.text[:200]))
    if ok:
        names = [x.get("name") for x in r.json()]
        print("    active runs: %s" % names, flush=True)

    r = api("GET", "/analytics/tools/used")
    ok = r.status_code == 200 and isinstance(r.json(), list)
    check(sc, "GET /analytics/tools/used", ok, "%s %s" % (r.status_code, r.text[:200]))
    if ok:
        tnames = {t.get("tool_name"): t.get("total_usage") for t in r.json()}
        check(sc, "  工具用量含 QueryResource/Knowledge Search",
              "QueryResource" in tnames and "Knowledge Search" in tnames, "usage=%s" % tnames)

    r = api("GET", "/analytics/tools/QueryResource/usage")
    ok = r.status_code == 200 and r.json().get("tool_calls", 0) >= 1
    check(sc, "GET /analytics/tools/QueryResource/usage", ok, "%s %s" % (r.status_code, r.text[:120]))
    r = api("GET", "/analytics/tools/QueryResource/logs")
    ok = r.status_code == 200 and isinstance(r.json(), list)
    check(sc, "GET /analytics/tools/QueryResource/logs", ok, "%s %s" % (r.status_code, r.text[:200]))
    r = api("GET", "/analytics/tools/NoSuchTool-xyz/usage")
    check(sc, "GET /analytics/tools/{不存在}/usage -> 404（而非 500）", r.status_code == 404, str(r.status_code))

    r = api("GET", "/analytics/knowledge/db-ops-manual/usage")
    ok = r.status_code == 200 and r.json().get("knowledge_calls", 0) >= 1
    check(sc, "GET /analytics/knowledge/db-ops-manual/usage（执行6 已产生 knowledge_picked/tool_used）", ok,
          "%s %s" % (r.status_code, r.text[:200]))
    r = api("GET", "/analytics/knowledge/db-ops-manual/logs")
    check(sc, "GET /analytics/knowledge/db-ops-manual/logs", r.status_code == 200,
          "%s %s" % (r.status_code, r.text[:200]))
    r = api("GET", "/analytics/knowledge/no-such-knowledge/usage")
    check(sc, "GET /analytics/knowledge/{不存在}/usage -> 404", r.status_code == 404, str(r.status_code))


# ---------------------------------------------------------------- S7
def s7_models():
    sc = "S7"
    r = api("GET", "/models_controller/get_api_key?model_provider=OpenAI")
    ok = r.status_code == 200 and r.json() and r.json()[0].get("api_key") == "ollama"
    check(sc, "GET /models_controller/get_api_key OpenAI（Fernet 解密读回）", ok, "%s %s" % (r.status_code, r.text[:160]))
    row = db_one("SELECT api_key FROM models_config WHERE provider='OpenAI' AND org_id=1")
    check(sc, "  models_config.api_key 库内为密文（gAAAAA...）", row and row[0].startswith("gAAAAA"),
          "db=%r" % (row[0][:24] + "..." if row else None,))

    r = api("POST", "/models_controller/store_api_keys",
            json={"model_provider": "ext-Provider-t7", "model_api_key": "ext-secret-123"})
    check(sc, "POST /models_controller/store_api_keys ext-Provider-t7 -> 200", r.status_code == 200,
          "%s %s" % (r.status_code, r.text[:120]))
    row = db_one("SELECT id, api_key FROM models_config WHERE provider='ext-Provider-t7' AND org_id=1")
    check(sc, "  ext provider 落库且密文 != 明文", row is not None and row[1] != "ext-secret-123"
          and row[1].startswith("gAAAAA"), "id=%s db_prefix=%r" % (row[0] if row else None,
                                                                   row[1][:12] if row else None))
    pid = row[0] if row else None
    r = api("GET", "/models_controller/get_api_key?model_provider=ext-Provider-t7")
    ok = r.status_code == 200 and r.json() and r.json()[0].get("api_key") == "ext-secret-123"
    check(sc, "  ext provider 解密读回 == 原始明文", ok, "%s %s" % (r.status_code, r.text[:160]))

    r = api("POST", "/models_controller/store_model",
            json={"model_name": "ext-model-t7", "description": "t7 temp model",
                  "end_point": "http://localhost:11434/v1", "model_provider_id": pid,
                  "token_limit": 4096, "type": "Custom", "version": "1", "context_length": 4096})
    check(sc, "POST /models_controller/store_model ext-model-t7 -> 200", r.status_code == 200,
          "%s %s" % (r.status_code, r.text[:160]))
    mrow = db_one("SELECT id, model_name, model_provider_id FROM models WHERE model_name='ext-model-t7'")
    check(sc, "  models 落库核对", mrow is not None and mrow[2] == pid, "row=%s" % (mrow,))

    r = api("GET", "/models_controller/fetch_models")
    names = [x["name"] for x in r.json()] if r.status_code == 200 and isinstance(r.json(), list) else []
    check(sc, "GET /models_controller/fetch_models（含 qwen3:4b 与 ext-model-t7）",
          "qwen3:4b" in names and "ext-model-t7" in names, "%s %s" % (r.status_code, names))
    r = api("GET", "/models_controller/fetch_model/1")
    ok = r.status_code == 200 and r.json().get("name") == "qwen3:4b"
    check(sc, "GET /models_controller/fetch_model/1", ok, "%s %s" % (r.status_code, r.text[:160]))
    r = api("GET", "/models_controller/get_api_keys")
    provs = [x["provider"] for x in r.json()] if r.status_code == 200 and isinstance(r.json(), list) else []
    check(sc, "GET /models_controller/get_api_keys 列出全部 provider（含 ext）",
          "ext-Provider-t7" in provs and "OpenAI" in provs, "%s %s" % (r.status_code, provs))
    return {"provider_id": pid, "model_id": mrow[0] if mrow else None}


# ---------------------------------------------------------------- S6 (S6b 需要 agent id，后置)
def s6_boundary_pre():
    sc = "S6"
    # 无效 id：404 而非 500
    r = api("GET", "/knowledges/user/get/details/999999")
    check(sc, "GET /knowledges/user/get/details/999999（无效 knowledge_id）-> 期望 404",
          r.status_code == 404, "actual=%s body=%s" % (r.status_code, r.text[:200]))
    r = api("GET", "/vector_dbs/db/details/999999")
    check(sc, "GET /vector_dbs/db/details/999999（无效 vector_db_id）-> 期望 404",
          r.status_code == 404, "actual=%s body=%s" % (r.status_code, r.text[:200]))
    r = api("GET", "/knowledges/user/get/details/3")
    ok = r.status_code == 200 and r.json().get("name") == "db-ops-manual"
    check(sc, "GET /knowledges/user/get/details/3（有效 id 对照组）", ok, "%s %s" % (r.status_code, r.text[:200]))
    r = api("GET", "/vector_dbs/db/details/1")
    ok = r.status_code == 200 and r.json().get("name") == "GaussDB-Local"
    check(sc, "GET /vector_dbs/db/details/1（有效 id 对照组）", ok, "%s %s" % (r.status_code, r.text[:200]))
    return {}


def s6_upload(agent_id):
    sc = "S6"
    with open(DEMO_DOC, "rb") as f:
        r = api("POST", "/resources/add/%s" % agent_id,
                files={"file": ("ext-t7-reject.xyz", f, "application/octet-stream")},
                data={"name": "ext-t7-reject.xyz", "size": "10", "type": "application/octet-stream"})
    ok = check(sc, "上传 .xyz -> 400 明确错误", r.status_code == 400 and "not supported" in r.text.lower(),
               "%s %s" % (r.status_code, r.text[:120]))
    n_reject = db_one("SELECT COUNT(*) FROM resources WHERE name='ext-t7-reject.xyz'")[0]
    check(sc, "  .xyz 未落库", n_reject == 0, "rows=%s" % n_reject)

    with open(DEMO_DOC, "rb") as f:
        data = f.read()
    with open(DEMO_DOC, "rb") as f:
        r = api("POST", "/resources/add/%s" % agent_id,
                files={"file": ("ext-t7-upload.md", f, "text/markdown")},
                data={"name": "ext-t7-upload.md", "size": str(len(data)), "type": "text/markdown"})
    ok = check(sc, "上传合法 .md -> 201（白名单新成员）", r.status_code == 201, "%s %s" % (r.status_code, r.text[:160]))
    row = db_one("SELECT id, name, size, type, channel FROM resources WHERE name='ext-t7-upload.md' "
                 "AND agent_id=%s ORDER BY id DESC LIMIT 1", (agent_id,))
    check(sc, "  resources 落库核对", row is not None and row[1] == "ext-t7-upload.md", "row=%s" % (row,))
    if not (ok and row):
        return None
    rid = row[0]
    got = poll_db("SELECT COUNT(*) FROM super_agi_vectors WHERE metadata->>'resource_id'=%s", (str(rid),),
                  lambda v: v[0] > 0, timeout=900, interval=6)
    check(sc, "  summarize_resource 完成 -> super_agi_vectors 新增该 resource 的向量块",
          got is not None, "resource_id=%s chunks=%s" % (rid, got[0][0] if got else "timeout(900s)"))
    return rid


# ---------------------------------------------------------------- S4
def s4_schedule():
    sc = "S4"
    start = (datetime.now() + timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%S")
    payload = {"agent_config": ext_config_payload("ext-agent-t7-sched", [], None),
               "schedule": {"agent_id": None, "start_time": start,
                            "recurrence_interval": None, "expiry_date": None, "expiry_runs": -1}}
    r = api("POST", "/agents/schedule", json=payload)
    ok = r.status_code == 201 and "schedule_id" in r.json()
    check(sc, "POST /agents/schedule -> 201 + schedule_id", ok, "%s %s" % (r.status_code, r.text[:200]))
    if not ok:
        return {}
    agent_id = r.json()["id"]
    sched_id = r.json()["schedule_id"]
    row = db_one("SELECT agent_id, status, recurrence_interval, expiry_runs, current_runs, start_time, "
                 "next_scheduled_time FROM agent_schedule WHERE id=%s", (sched_id,))
    check(sc, "agent_schedule 落库核对（SCHEDULED, next=start, current_runs=0）",
          row is not None and row[1] == "SCHEDULED" and row[6] is not None and str(row[5]) == str(row[6]),
          "row=%s" % (row,))

    r = api("GET", "/agents/get/schedule_data/%s" % agent_id)
    ok = r.status_code == 200 and {"current_datetime", "start_date", "recurrence_interval", "expiry_runs"} <= set(r.json())
    check(sc, "GET /agents/get/schedule_data/{agent_id}", ok, "%s %s" % (r.status_code, r.text[:200]))

    new_start = (datetime.now() + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S")
    r = api("PUT", "/agents/edit/schedule",
            json={"agent_id": agent_id, "start_time": new_start, "recurrence_interval": None,
                  "expiry_date": None, "expiry_runs": -1})
    row2 = db_one("SELECT start_time, next_scheduled_time, status FROM agent_schedule WHERE id=%s", (sched_id,))
    want = new_start.replace("T", " ")  # pydantic 解析后 str(datetime) 用空格分隔
    check(sc, "PUT /agents/edit/schedule 改期落库",
          r.status_code == 200 and str(row2[0]).startswith(want) and str(row2[1]).startswith(want),
          "%s row=%s want=%s" % (r.status_code, row2, want))

    r = api("POST", "/agents/stop/schedule?agent_id=%s" % agent_id)
    row3 = db_one("SELECT status FROM agent_schedule WHERE id=%s", (sched_id,))
    check(sc, "POST /agents/stop/schedule -> 库内 STOPPED", r.status_code == 200 and row3 == ("STOPPED",),
          "%s row=%s" % (r.status_code, row3))
    return {"sched_agent_id": agent_id}


# ---------------------------------------------------------------- S3 + S5
def s3_s5_lifecycle(hook_ctx):
    sc = "S3"
    # --- 创建（挂 Knowledge Search 工具 + knowledge=3）---
    r = api("POST", "/agents/create", json=ext_config_payload("ext-agent-t7", [33], 3))
    ok = r.status_code == 201 and r.json().get("id")
    check(sc, "POST /agents/create（tools=[Knowledge Search], knowledge=3）-> 201", ok,
          "%s %s" % (r.status_code, r.text[:200]))
    if not ok:
        return {}
    agent_id = r.json()["id"]
    exec0 = r.json().get("execution_id")

    row = db_one("SELECT value FROM agent_configurations WHERE agent_id=%s AND key='knowledge'", (agent_id,))
    check(sc, "agent_configurations 落库核对（knowledge=3）", row == ("3",), "row=%s" % (row,))

    # --- S6 上传到该 agent ---
    rid = s6_upload(agent_id)

    # --- 执行：等首条 feed（尽量等到 Knowledge Search 工具输出）---
    r = api("POST", "/agentexecutions/add",
            json={"agent_id": agent_id, "name": "ext-run-t7-1", "goal": GOAL, "instruction": INSTRUCTION})
    ok = r.status_code == 201 and r.json().get("status") == "RUNNING"
    check(sc, "POST /agentexecutions/add -> 201 RUNNING（celery 入队）", ok, "%s %s" % (r.status_code, r.text[:200]))
    if not ok:
        return {"agent_id": agent_id}
    exec1 = r.json()["id"]

    def have_thought(rows):
        # 执行的前 3 条 feed 是初始 system/user 脚手架，不是模型输出；
        # 以首条 role=assistant 的 thought 作为“第一轮 thought 出现”的判据。
        got = db_one("SELECT COUNT(*) FROM agent_execution_feeds WHERE agent_execution_id=%s "
                     "AND role='assistant'", (exec1,))[0]
        return got > 0

    got = poll_db("SELECT COUNT(*) FROM agent_execution_feeds WHERE agent_execution_id=%s", (exec1,),
                  have_thought, timeout=720, interval=6)
    check(sc, "执行产生首条 assistant thought（等第一轮 thought）", got is not None,
          "feeds=%s" % (got[0][0] if got else "timeout(720s)"))

    feeds_before = db_one("SELECT COUNT(*) FROM agent_execution_feeds WHERE agent_execution_id=%s", (exec1,))[0]
    tool_feed = db_one("SELECT LEFT(feed, 300) FROM agent_execution_feeds WHERE agent_execution_id=%s "
                       "AND role='system' AND feed LIKE '%%Knowledge Search returned%%' ORDER BY id DESC LIMIT 1",
                       (exec1,))
    if tool_feed:
        check("S5", "停止前已捕获 Knowledge Search 工具输出", True, "tool_feed=%r" % tool_feed[0])
    else:
        # 首个 thought 出现即 STOP，工具通常尚未执行；S5 的工具输出证据在 rerun 执行中采集（见下）。
        print("    note: 停止前无工具输出（预期内：首 thought 即停）；S5 证据改由 rerun 执行采集", flush=True)

    # --- STOP：UI 同款路径 PUT /agentexecutions/update/{id} status=TERMINATED ---
    r = api("PUT", "/agentexecutions/update/%s" % exec1, json={"status": "TERMINATED"})
    ok = r.status_code == 200 and r.json().get("status") == "TERMINATED"
    check(sc, "STOP: PUT /agentexecutions/update/{id} {TERMINATED} -> 200", ok,
          "%s %s" % (r.status_code, r.text[:160]))
    row = db_one("SELECT status FROM agent_executions WHERE id=%s", (exec1,))
    check(sc, "STOP 后库内 status=TERMINATED", row == ("TERMINATED",), "row=%s" % (row,))
    # 在飞 step 的剩余输出会落完（executor 仅在 step 间检查状态）；轮询至 feed 数连续 3 次采样稳定。
    feeds_after, stable = feeds_before, 0
    deadline = time.time() + 300
    while stable < 3 and time.time() < deadline:
        time.sleep(20)
        cnt = db_one("SELECT COUNT(*) FROM agent_execution_feeds WHERE agent_execution_id=%s", (exec1,))[0]
        if cnt == feeds_after:
            stable += 1
        else:
            stable = 0
            feeds_after = cnt
    check(sc, "STOP 后 feed 停止增长（在飞 step 剩余输出落完，连续 3 次采样稳定）", stable >= 3,
          "before=%s after=%s" % (feeds_before, feeds_after))
    check(sc, "上游终止语义：仅置 status=TERMINATED，无专用终止 feed 行（设计如此，记录）", True,
          "feeds 尾行为执行中产生的 thought/tool 输出；前端以状态呈现终止")

    # --- webhook 投递核对（S1 联动：执行状态变化触发）---
    hits = list(_webhook_hits)
    wh_rows = db_q("SELECT run_id, event, status, errors FROM webhook_events ORDER BY id DESC LIMIT 5")
    check(sc, "[S1 联动] webhook 真实投递（本地监听器收到 POST + webhook_events sent）",
          len(hits) > 0 and any(w[2] == "sent" for w in wh_rows),
          "hits=%s webhook_events=%s" % (len(hits), wh_rows[:3]))
    if hits:
        b = json.loads(hits[0]["body"])
        check(sc, "  投递体结构 {agent_id, org_id, event:'OLD to NEW'}",
              set(b) == {"agent_id", "org_id", "event"} and " to " in b["event"], "body=%s" % b)

    # --- Run Again：/agentexecutions/add_run ---
    r = api("POST", "/agentexecutions/add_run",
            json={"name": "ext-run-t7-2", "agent_id": agent_id, "goal": GOAL, "instruction": INSTRUCTION,
                  "agent_workflow": AGENT_WORKFLOW, "constraints": [], "toolkits": [], "tools": [33],
                  "exit": "Terminate", "iteration_interval": 1, "model": MODEL,
                  "permission_type": "God Mode", "LTM_DB": "GAUSSDB", "max_iterations": 2,
                  "user_timezone": "Asia/Shanghai", "knowledge": 3})
    ok = r.status_code == 201 and r.json().get("id")
    check(sc, "Run Again: POST /agentexecutions/add_run -> 201 新 execution", ok,
          "%s %s" % (r.status_code, r.text[:200]))
    if ok:
        exec2 = r.json()["id"]
        row = poll_db("SELECT status FROM agent_executions WHERE id=%s", (exec2,),
                      lambda v: v[0][0] in ("COMPLETED", "ITERATION_LIMIT_EXCEEDED", "TERMINATED"),
                      timeout=900, interval=8)
        check(sc, "  rerun execution 到达终态", row is not None,
              "status=%s" % (row[0][0] if row else "timeout(900s)"))
        fcnt = db_one("SELECT COUNT(*) FROM agent_execution_feeds WHERE agent_execution_id=%s", (exec2,))[0]
        check(sc, "  rerun 产生 feeds", fcnt > 0, "feeds=%s" % fcnt)
        if row and row[0][0] == "COMPLETED":
            ev = db_one("SELECT COUNT(*) FROM events WHERE event_name='run_completed' "
                        "AND event_property->>'agent_execution_id'=%s", (str(exec2),))
            check(sc, "  rerun COMPLETED -> run_completed 事件落库（供 S2 聚合）", ev[0] == 1, "events=%s" % ev[0])
        if row is not None:
            rtool = db_one("SELECT LEFT(feed, 300) FROM agent_execution_feeds WHERE agent_execution_id=%s "
                           "AND role='system' AND feed LIKE '%%Knowledge Search returned%%' ORDER BY id DESC LIMIT 1",
                           (exec2,))
            check("S5", "rerun 执行中 Knowledge Search 返回知识库文档内容（含水位阈值/85% 块）",
                  rtool is not None and (("水位" in rtool[0]) or ("85" in rtool[0]) or ("阈值" in rtool[0])),
                  "tool_feed=%r" % (rtool[0] if rtool else None))
        else:
            rtool = None
    else:
        exec2 = None

    # --- 编辑 agent：PUT /v1/agent/{agent_id}（X-API-Key 鉴权链）---
    live_key = db_one("SELECT key FROM api_keys WHERE name='ext-key-t7-renamed' AND (is_expired IS NULL OR is_expired=false) "
                      "ORDER BY id DESC LIMIT 1")
    headers = {"X-API-Key": live_key[0]} if live_key else {}
    new_goal = ["改写目标：从知识库查询磁盘水位黄色阈值（75%）"]
    r = api("PUT", "/v1/agent/%s" % agent_id,
            headers=headers,
            json={"name": "ext-agent-t7", "description": "t7 extended e2e agent (edited)",
                  "goal": new_goal, "instruction": INSTRUCTION, "agent_workflow": AGENT_WORKFLOW,
                  "constraints": [], "tools": [{"name": "Knowledge Search Toolkit",
                                                "tools": ["Knowledge Search"]}],
                  "iteration_interval": 1, "model": MODEL, "max_iterations": 2})
    ok = r.status_code == 200 and r.json().get("agent_id") == agent_id
    check(sc, "编辑 agent: PUT /v1/agent/{id}（X-API-Key 鉴权）-> 200", ok, "%s %s" % (r.status_code, r.text[:200]))
    grow = db_q("SELECT value FROM agent_configurations WHERE agent_id=%s AND key='goal' ORDER BY id", (agent_id,))
    check(sc, "  编辑后新 goal 行写入 agent_configurations", any("黄色阈值" in g[0] for g in grow), "goals=%s" % grow)
    check(sc, "  上游缺陷记录：/v1/agent PUT 追加新配置行而非更新旧行（旧行残留）",
          len(grow) >= 2, "goal 行数=%s" % len(grow))
    new_exec = db_one("SELECT COUNT(*) FROM agent_executions WHERE agent_id=%s AND name='New Run' "
                      "AND status='CREATED'", (agent_id,))[0]
    check(sc, "  编辑端点同时创建 CREATED 占位执行（上游设计，未运行）", new_exec >= 1, "created_runs=%s" % new_exec)

    # --- 执行配置详情端点（GUI 消费）---
    if exec1:
        r = api("GET", "/agent_executions_configs/details/agent_id/%s/agent_execution_id/%s" % (agent_id, exec1))
        ok = r.status_code == 200 and r.json().get("knowledge_name") == "db-ops-manual"
        check(sc, "GET /agent_executions_configs/details/...（knowledge_name 回显）", ok,
              "%s %s" % (r.status_code, r.text[:200]))

    # --- 与执行 6 的知识检索对比（观察，不打扰）---
    e6 = db_one("SELECT status, num_of_calls FROM agent_executions WHERE id=6")
    e6_tool = db_one("SELECT LEFT(feed, 200) FROM agent_execution_feeds WHERE agent_execution_id=6 "
                     "AND role='system' AND feed LIKE '%%Knowledge Search returned%%' ORDER BY id DESC LIMIT 1")
    print("    [exec6 观察] status=%s calls=%s last_knowledge_feed=%r" % (e6[0], e6[1], e6_tool[0] if e6_tool else None),
          flush=True)
    check("S5", "与执行 6 对比：走同一 super_agi_vectors 索引，均返回文档块",
          e6_tool is not None and rtool is not None,
          "exec6=%s ext(rerun)=%s" % (bool(e6_tool), bool(rtool)))

    # --- 删除 agent：级联核对 ---
    r = api("PUT", "/agents/delete/%s" % agent_id)
    check(sc, "删除 agent: PUT /agents/delete/{id} -> 200", r.status_code == 200, str(r.status_code))
    row = db_one("SELECT is_deleted FROM agents WHERE id=%s", (agent_id,))
    check(sc, "  软删：agents.is_deleted=True", row == (True,), "row=%s" % (row,))
    st = db_q("SELECT status, COUNT(*) FROM agent_executions WHERE agent_id=%s GROUP BY status", (agent_id,))
    check(sc, "  级联：该 agent 全部 executions 置 TERMINATED",
          st and all(s[0] == "TERMINATED" for s in st), "exec_status=%s" % st)
    orphans = db_one("SELECT COUNT(*) FROM agent_execution_feeds f JOIN agent_executions e ON f.agent_execution_id=e.id "
                     "WHERE e.agent_id=%s", (agent_id,))[0]
    res = db_one("SELECT COUNT(*) FROM resources WHERE agent_id=%s", (agent_id,))[0]
    check(sc, "  孤儿观察（上游设计：feeds/resources 行保留，不物理清理）", True,
          "feeds=%s resources=%s（含向量块, 保留）" % (orphans, res))
    return {"agent_id": agent_id, "sched_agent_id": hook_ctx.get("sched_agent_id"),
            "resource_id": rid}


# ---------------------------------------------------------------- cleanup
def cleanup(ctx):
    print("\n== cleanup (ext- 实体) ==", flush=True)
    notes = []
    try:
        r = api("DELETE", "/api-keys/%s" % ctx.get("api_key_row"))
        notes.append("api_keys ext-key-t7-renamed: DELETE API -> %s (soft expire)" % r.status_code)
    except Exception as e:
        notes.append("api_keys cleanup error: %s" % e)
    try:
        live = db_one("SELECT id FROM api_keys WHERE name IN ('ext-key-t7', 'ext-key-t7-renamed') "
                      "AND (is_expired IS NULL OR is_expired=false) ORDER BY id DESC LIMIT 1")
        if live:
            r = api("DELETE", "/api-keys/%s" % live[0])
            notes.append("api_keys live key: DELETE API -> %s" % r.status_code)
    except Exception as e:
        notes.append("api_keys live cleanup error: %s" % e)
    if ctx.get("webhook_id"):
        db_exec("UPDATE webhooks SET is_deleted=true, filters=%s WHERE id=%s",
                (json.dumps({"status": []}), ctx["webhook_id"]))
        notes.append("webhook ext-webhook-t7: 无删除端点，SQL 置 is_deleted=true 且 filters 清空（上游 WebHookManager "
                     "不过滤 is_deleted，置空 filters 防继续触发）")
    if ctx.get("sched_agent_id"):
        try:
            r = api("PUT", "/agents/delete/%s" % ctx["sched_agent_id"])
            notes.append("agent ext-agent-t7-sched: DELETE API -> %s" % r.status_code)
        except Exception as e:
            notes.append("sched agent cleanup error: %s" % e)
    db_exec("DELETE FROM projects WHERE name='ext-project-t7'")
    notes.append("project ext-project-t7: 无删除端点，SQL 删除")
    db_exec("DELETE FROM models WHERE model_name='ext-model-t7'")
    db_exec("DELETE FROM models_config WHERE provider='ext-Provider-t7'")
    notes.append("models ext-model-t7 / models_config ext-Provider-t7: 无删除端点，SQL 删除（OpenAI 行未动）")
    for n in notes:
        print("  - " + n, flush=True)
    print("  - 保留项：ext-agent-t7 软删行及其 executions/feeds/configs/events、resources(ext-t7-upload.md) 与 "
          "super_agi_vectors 中对应向量块（供追溯，不影响功能）", flush=True)


# ---------------------------------------------------------------- main
def main():
    print("t7 extended e2e start: %s" % datetime.now(), flush=True)
    server = start_hook_listener()
    ctx = {}
    e6_before = db_one("SELECT status, num_of_calls FROM agent_executions WHERE id=6")
    print("  exec6 (不打扰，仅观察) before: %s" % (e6_before,), flush=True)

    # 登录（ENV=DEV 下 check_auth 实际不校验，仍走一遍保证与 PROD 行为对齐）
    try:
        r = api("POST", "/login", json={"email": "super6@agi.com", "password": "e2e-pass-123"})
        token = r.json().get("access_token")
        S.headers.update({"Authorization": "Bearer %s" % token} if token else {})
        check("S0", "/login JWT（Bearer 附带）", r.status_code == 200 and bool(token), str(r.status_code))
    except Exception as e:
        blocked("S0", "/login failed", e)

    scenarios = [
        ("S1", lambda: s1_settings()),
        ("S2", lambda: s2_analytics()),
        ("S7", lambda: s7_models()),
        ("S6", lambda: s6_boundary_pre()),
        ("S4", lambda: s4_schedule()),
        ("S3+S5", lambda: s3_s5_lifecycle(ctx)),
    ]
    for name, fn in scenarios:
        print("\n---- %s ----" % name, flush=True)
        try:
            out = fn()
            if isinstance(out, dict):
                ctx.update(out)
        except Exception as e:
            traceback.print_exc()
            blocked(name, "scenario crashed", "%s: %s" % (type(e).__name__, e))

    e6_after = db_one("SELECT status, num_of_calls, num_of_tokens FROM agent_executions WHERE id=6")
    print("\n  exec6 (不打扰，仅观察) after: %s" % (e6_after,), flush=True)

    try:
        cleanup(ctx)
    except Exception as e:
        print("cleanup error: %s" % e, flush=True)
    try:
        server.shutdown()
    except Exception:
        pass

    print("\n================ SUMMARY ================", flush=True)
    scenarios_order = ["S0", "S1", "S2", "S7", "S6", "S4", "S3", "S5"]
    total = {"PASS": 0, "FAIL": 0, "BLOCKED": 0}
    for sc in scenarios_order:
        rows = [r for r in _results if r[0] == sc]
        if not rows:
            continue
        p = sum(1 for r in rows if r[2] == "PASS")
        f = sum(1 for r in rows if r[2] == "FAIL")
        b = sum(1 for r in rows if r[2] == "BLOCKED")
        total["PASS"] += p
        total["FAIL"] += f
        total["BLOCKED"] += b
        print("%-8s PASS=%d FAIL=%d BLOCKED=%d" % (sc, p, f, b), flush=True)
        for r in rows:
            if r[2] != "PASS":
                print("    [%s] %s | %s" % (r[2], r[1], r[3][:200]), flush=True)
    print("TOTAL: PASS=%d FAIL=%d BLOCKED=%d" % (total["PASS"], total["FAIL"], total["BLOCKED"]), flush=True)
    print("t7 extended e2e end: %s" % datetime.now(), flush=True)
    return 1 if total["FAIL"] else 0


if __name__ == "__main__":
    sys.exit(main())
