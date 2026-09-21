# -*- coding: utf-8 -*-
"""Task 9/12 t6: 端到端验收 —— SuperAGI 双进程（uvicorn + celery）在 GaussDB 上用 Ollama 本地模型跑通一次 Agent 执行。

支持集中式（super_agi_e2e@5432）与分布式（super_agi_dist@15400）两种形态，同一脚本同一标准。

流程：
  1. 启动 uvicorn（main:app，端口=argv[2]，默认 8001）
  2. 启动 celery worker（superagi.worker, --pool=solo）
  3. /users/add 注册（DEV 自动建 org+project）→ /login 拿 JWT
  4. 登记 OpenAI provider（api_key=ollama）+ models 行（qwen3:4b）
  5. /agents/create（Goal Based Workflow, LTM_DB=GAUSSDB）
  6. /resources/add/{agent_id} 上传 demo-docs 真实 Markdown（触发 summarize_resource → super_agi_vectors）
  7. /agentexecutions/add 触发执行（execute_agent.delay）
  8. 轮询 agent_execution_feeds / agent_executions.status
  9. psycopg2 直查目标库验证 feeds / events JSONB / 向量表 <+> 检索（含 05 文档语义命中）
 10. 非集中式目标时交叉检查集中式库（super_agi_e2e）无串写

参数化：argv[1]=DB URL（argv/环境变量 E2E_URL/内置默认三优先级）；
argv[2]=uvicorn 端口（E2E_PORT，默认 8001）；argv[3]=redis（纯数字=config.yaml 同实例的 db 号，
含 ":" 或 "."=独立实例 host:port，如 127.0.0.1:6380）。
DB_URL / REDIS_URL 通过环境变量注入 uvicorn/celery 子进程（superagi/config/config.py 的
os.environ 整体覆盖 config.yaml，worker.py 以 "redis://"+REDIS_URL+"/0" 拼 broker），不动 config.yaml。
"""
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from urllib.parse import unquote, urlparse

import psycopg2
import requests

ROOT = r"D:\workplace\code\SuperAGI\SuperAGI-0.0.14"
CENTRAL_URL = "opengauss+psycopg2://superagi_test:GaussTest2026@127.0.0.1:5432/super_agi_e2e"
# 目标参数化：argv[1..3] 优先，环境变量 E2E_URL / E2E_PORT / E2E_REDIS 兜底，
# 默认值保持集中式现状（super_agi_e2e @5432、uvicorn 8001、redis 127.0.0.1:6379 db0）。
E2E_URL = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("E2E_URL", CENTRAL_URL)
E2E_PORT = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("E2E_PORT", "8001")
E2E_REDIS = sys.argv[3] if len(sys.argv) > 3 else os.environ.get("E2E_REDIS", "0")
CONTROL_URL = os.environ.get("E2E_CONTROL_URL", CENTRAL_URL)
IS_ISOLATED_RUN = E2E_URL != CONTROL_URL  # 目标非集中式控制库时，启用串库交叉检查
AGENT_NAME = "e2e-dist-agent" if IS_ISOLATED_RUN else "e2e-gauss-agent"
RUN_NAME = "e2e-dist-run-1" if IS_ISOLATED_RUN else "e2e-run-1"
# Task 12: 资源场景换成真实文档（demo-docs 容量规划篇；产品只收 txt 等 5 类扩展名，
# 以 .txt 名义上传真实 Markdown 内容，走 FlatReader + token 分块链路）
DEMO_DOC = r"D:\workplace\doc\ObsidianNote\learning\rag\demo-docs\05-capacity-planning.md"
DEMO_DOC_QUERY = "磁盘水位红色阈值"  # 期待召回 3.1 三级水位阈值块（红 > 85%）
_p = urlparse(E2E_URL)
DB = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=unquote(_p.password), dbname=_p.path.lstrip("/"))
BASE = f"http://127.0.0.1:{E2E_PORT}"
LOGDIR = os.path.join(ROOT, "delivery", "tests", "_t6_logs")
OLLAMA = "http://localhost:11434/v1"
GOAL = ["At what disk usage level does the red alert threshold trigger in database capacity "
        "planning? Answer from your own knowledge in one short sentence."]
INSTRUCTION = ["Answer in one short sentence.", "No tools are available, just answer directly."]
AGENT_PAYLOAD = {
    "name": AGENT_NAME,
    "description": "E2E acceptance agent on GaussDB with local Ollama models",
    "goal": GOAL,
    "instruction": INSTRUCTION,
    "agent_workflow": "Goal Based Workflow",
    "constraints": [],
    "toolkits": [],
    "tools": [],
    "exit": "Terminate",
    "iteration_interval": 1,
    "model": "qwen3:4b",
    "permission_type": "God Mode",
    "LTM_DB": "GAUSSDB",
    "max_iterations": 2,
    "user_timezone": "Asia/Shanghai",
}

_results = []


def step_pass(msg):
    line = "[PASS] " + msg
    _results.append(line)
    print(line, flush=True)


def step_fail(msg):
    line = "[FAIL] " + msg
    print(line, flush=True)
    raise AssertionError(line)


def db_conn():
    return psycopg2.connect(**DB)


def db_one(sql, params=None):
    with db_conn() as c:
        with c.cursor() as cur:
            cur.execute(sql, params or {})
            return cur.fetchone()


def wait_api(timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(BASE + "/openapi.json", timeout=5)
            if r.status_code == 200:
                return True
        except requests.RequestException:
            pass
        time.sleep(2)
    return False


def poll_db(sql, params, expect_fn, timeout, what, interval=5):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = db_one(sql, params)
        except psycopg2.Error as e:
            print("  poll db error:", e, flush=True)
        if last is not None and expect_fn(last):
            return last
        time.sleep(interval)
    return None


def ollama_embed(text):
    r = requests.post(OLLAMA + "/embeddings",
                      json={"model": "qwen3-embedding:0.6b", "input": [text]}, timeout=120)
    r.raise_for_status()
    return r.json()["data"][0]["embedding"]


def gauss_search(table, text, top_k=3):
    q = ollama_embed(text)
    vec = "[" + ",".join(repr(float(v)) for v in q) + "]"
    sql = (f'SELECT id, text, metadata, 1 - (embedding <+> CAST(%(q)s AS floatvector)) AS score '
           f'FROM "{table}" ORDER BY embedding <+> CAST(%(q)s AS floatvector) LIMIT %(k)s')
    with db_conn() as c:
        with c.cursor() as cur:
            cur.execute(sql, {"q": vec, "k": top_k})
            return cur.fetchall()


def assert_vector_table_shape(table, expect_dim=1024):
    """分布式/集中式同标准：1024 维 floatvector + GsIVFFLAT 索引（_ivf 命名一致）。"""
    idx = db_one("SELECT indexdef FROM pg_indexes WHERE schemaname='public' AND tablename=%s", (table,))
    if not idx:
        step_fail(f"vector table {table} has no index")
    if "using gsivfflat" not in idx[0].lower():
        step_fail(f"vector table {table} index is not GsIVFFLAT: {idx[0]}")
    dim_row = db_one(f'SELECT vector_to_array(embedding) FROM "{table}" LIMIT 1')
    if not dim_row or dim_row[0] is None:
        step_fail(f"vector table {table}: cannot read embedding dims")
    if len(dim_row[0]) != expect_dim:
        step_fail(f"vector table {table} dim={len(dim_row[0])}, expected {expect_dim}")
    return idx[0], len(dim_row[0])


def tail_log(path, n=40):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return "".join(f.readlines()[-n:])
    except OSError as e:
        return f"(no log: {e})"


def central_snapshot():
    """集中式控制库（super_agi_e2e@5432）基线快照，用于串库交叉检查。"""
    _cp = urlparse(CONTROL_URL)
    cdb = dict(host=_cp.hostname, port=_cp.port or 5432, user=_cp.username,
               password=unquote(_cp.password), dbname=_cp.path.lstrip("/"))
    with psycopg2.connect(**cdb) as c:
        with c.cursor() as cur:
            cur.execute("SELECT count(*) FROM pg_tables WHERE schemaname='public'")
            tables = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM agents")
            agents = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM agent_executions")
            execs = cur.fetchone()[0]
            return {"tables": tables, "agents": agents, "execs": execs}


def central_cross_check(baseline, agent_name, run_name):
    """断言集中式库表数未变、且本次运行的 agent/execution 未串写到集中式库。"""
    _cp = urlparse(CONTROL_URL)
    cdb = dict(host=_cp.hostname, port=_cp.port or 5432, user=_cp.username,
               password=unquote(_cp.password), dbname=_cp.path.lstrip("/"))
    with psycopg2.connect(**cdb) as c:
        with c.cursor() as cur:
            cur.execute("SELECT count(*) FROM pg_tables WHERE schemaname='public'")
            tables_now = cur.fetchone()[0]
            if tables_now != baseline["tables"]:
                step_fail(f"cross-db check: central table count changed "
                          f"{baseline['tables']} -> {tables_now}")
            cur.execute("SELECT count(*) FROM agents WHERE name=%s", (agent_name,))
            if cur.fetchone()[0] > 0:
                step_fail(f"cross-db check: agent {agent_name!r} leaked into central db")
            cur.execute("SELECT count(*) FROM agent_executions WHERE name=%s", (run_name,))
            if cur.fetchone()[0] > 0:
                step_fail(f"cross-db check: execution {run_name!r} leaked into central db")
            cur.execute("SELECT count(*) FROM agents")
            agents_now = cur.fetchone()[0]
    step_pass(f"cross-db check: central db untouched (tables={tables_now} before={baseline['tables']}, "
              f"agents={baseline['agents']}->{agents_now}, no {agent_name!r}/{run_name!r} rows leaked)")


def main():
    os.makedirs(LOGDIR, exist_ok=True)
    env = os.environ.copy()
    env["OPENAI_API_BASE"] = "http://localhost:11434/v1"  # openai 库 import 期读该环境变量
    env["HF_ENDPOINT"] = "https://hf-mirror.com"  # py3.8 下 llama_index 用 transformers GPT2 分词器，走可达镜像
    env["TRANSFORMERS_OFFLINE"] = "1"  # GPT2 分词器已入本地缓存（~/.cache/huggingface），
    env["HF_HUB_OFFLINE"] = "1"        # 离线化以免疫 hf-mirror 偶发 SSL 抖动（HEAD 失败会拖垮 summarize）
    env["PYTHONUNBUFFERED"] = "1"
    # 目标库注入：config.py 的 os.environ 整体覆盖 yaml，DB_URL 恒注入保证
    # uvicorn/celery/向量建表与脚本连同一库（集中式下与 yaml 同值，等效无操作）。
    env["DB_URL"] = E2E_URL
    if E2E_REDIS and (":" in E2E_REDIS or "." in E2E_REDIS):
        # 独立 redis 实例（如分布式专用 127.0.0.1:6380）：worker.py 拼 "redis://"+REDIS_URL+"/0"
        env["REDIS_URL"] = E2E_REDIS
        print(f"  redis isolation: REDIS_URL={env['REDIS_URL']} (separate instance, db 0)", flush=True)
    elif E2E_REDIS != "0":
        # 老语义：config.yaml 同实例换 db 号。注意 superagi/worker.py 的
        # broker_url/result_backend 硬编码追加 "/0"，非 0 db 在产品代码放开前不会真正生效。
        import yaml
        with open(os.path.join(ROOT, "config.yaml"), "r", encoding="utf-8") as f:
            _redis_base = (yaml.safe_load(f) or {}).get("REDIS_URL", "127.0.0.1:6379")
        env["REDIS_URL"] = f"{_redis_base}/{E2E_REDIS}"
        print(f"  [warn] E2E_REDIS={E2E_REDIS}: injected REDIS_URL={env['REDIS_URL']} "
              f"(worker.py hardcodes broker '/0' — non-zero db may not take effect)", flush=True)

    baseline = central_snapshot() if IS_ISOLATED_RUN else None
    if baseline:
        print(f"  central db baseline (cross-check): {baseline}", flush=True)

    uvicorn_log = open(os.path.join(LOGDIR, f"uvicorn_{E2E_PORT}.log"), "w", encoding="utf-8")
    celery_log = open(os.path.join(LOGDIR, f"celery_{E2E_PORT}.log"), "w", encoding="utf-8")
    uvicorn_p = celery_p = None
    try:
        # ---- 1. uvicorn ----
        uvicorn_p = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", str(E2E_PORT)],
            cwd=ROOT, stdout=uvicorn_log, stderr=subprocess.STDOUT, env=env)
        if not wait_api():
            print(tail_log(os.path.join(LOGDIR, f"uvicorn_{E2E_PORT}.log")), flush=True)
            step_fail("uvicorn API did not become ready")
        step_pass(f"boot: uvicorn main:app ready at {BASE} (GaussDB e2e db)")

        # ---- 2. celery worker ----
        celery_p = subprocess.Popen(
            [sys.executable, "-m", "celery", "-A", "superagi.worker", "worker",
             "--loglevel=info", "--pool=solo"],
            cwd=ROOT, stdout=celery_log, stderr=subprocess.STDOUT, env=env)
        time.sleep(10)
        if celery_p.poll() is not None:
            print(tail_log(os.path.join(LOGDIR, f"celery_{E2E_PORT}.log")), flush=True)
            step_fail("celery worker exited immediately")
        step_pass("boot: celery worker (superagi.worker, --pool=solo) alive")

        # ---- 3. register + login ----
        r = requests.post(BASE + "/users/add",
                          json={"name": "E2E", "email": "super6@agi.com", "password": "e2e-pass-123"},
                          timeout=60)
        if r.status_code != 201:
            step_fail(f"/users/add -> {r.status_code} {r.text[:300]}")
        user = r.json()
        r = requests.post(BASE + "/login",
                          json={"email": "super6@agi.com", "password": "e2e-pass-123"}, timeout=60)
        if r.status_code != 200 or "access_token" not in r.json():
            step_fail(f"/login -> {r.status_code} {r.text[:300]}")
        token = r.json()["access_token"]
        r = requests.get(BASE + "/validate-access-token",
                         headers={"Authorization": f"Bearer {token}"}, timeout=60)
        if r.status_code != 200:
            step_fail(f"/validate-access-token -> {r.status_code} {r.text[:300]}")
        org_id = user["organisation_id"]
        step_pass(f"auth: user+org created (org_id={org_id}), /login JWT issued and validated")

        project = db_one("SELECT id FROM projects WHERE organisation_id=%s ORDER BY id LIMIT 1", (org_id,))
        if not project:
            # 首次 500（toolkit 加载失败）可能留下有 user/org 但无 project 的库；走 API 补建
            r = requests.post(BASE + "/projects/add",
                              json={"name": "Default Project", "organisation_id": org_id,
                                    "description": "New Default Project"}, timeout=60)
            if r.status_code != 201:
                step_fail(f"/projects/add -> {r.status_code} {r.text[:300]}")
            project = (r.json()["id"],)
        project_id = project[0]

        # ---- 4. model coordinates ----
        r = requests.post(BASE + "/models_controller/store_api_keys",
                          json={"model_provider": "OpenAI", "model_api_key": "ollama"}, timeout=120)
        if r.status_code != 200:
            step_fail(f"store_api_keys -> {r.status_code} {r.text[:300]}")
        provider = db_one("SELECT id FROM models_config WHERE provider='OpenAI' AND org_id=%s ORDER BY id LIMIT 1",
                          (org_id,))
        if not provider:
            step_fail("models_config row for OpenAI not found")
        model_provider_id = provider[0]
        r = requests.post(BASE + "/models_controller/store_model",
                          json={"model_name": "qwen3:4b", "description": "local ollama qwen3 4b",
                                "end_point": "", "model_provider_id": model_provider_id,
                                "token_limit": 8192, "type": "Custom", "version": "1",
                                "context_length": 8192}, timeout=120)
        if r.status_code != 200 or ("error" in r.json() and "already exists" not in r.json()["error"]):
            step_fail(f"store_model -> {r.status_code} {r.text[:300]}")
        step_pass(f"model source registered: provider OpenAI(api_key=ollama) + models row qwen3:4b "
                  f"(provider_id={model_provider_id})")

        # ---- 5. agent create ----
        payload = dict(AGENT_PAYLOAD)
        payload["project_id"] = project_id
        r = requests.post(BASE + "/agents/create", json=payload, timeout=120)
        if r.status_code != 201:
            step_fail(f"/agents/create -> {r.status_code} {r.text[:500]}")
        agent = r.json()
        agent_id, first_exec_id = agent["id"], agent["execution_id"]
        step_pass(f"agent created id={agent_id} (Goal Based Workflow, model qwen3:4b, LTM_DB=GAUSSDB), "
                  f"initial execution id={first_exec_id}")

        # ---- 6. resource upload (real demo-docs content -> summarize -> super_agi_vectors) ----
        # 产品限制：resources.py:60 accepted_file_types 硬编码 (.pdf/.docx/.pptx/.csv/.txt/.epub)，
        # 不收 .md —— 测试侧以 .txt 扩展名上传真实 Markdown 内容（解析/分块/向量链路不变）。
        with open(DEMO_DOC, "rb") as f:
            doc_bytes = f.read()
        doc_name = os.path.splitext(os.path.basename(DEMO_DOC))[0] + ".txt"
        with open(DEMO_DOC, "rb") as f:
            r = requests.post(f"{BASE}/resources/add/{agent_id}",
                              files={"file": (doc_name, f, "text/plain")},
                              data={"name": doc_name, "size": str(len(doc_bytes)),
                                    "type": "text/plain"}, timeout=120)
        if r.status_code not in (200, 201):
            step_fail(f"/resources/add -> {r.status_code} {r.text[:300]}")
        resource_id = db_one("SELECT id FROM resources WHERE agent_id=%s ORDER BY id DESC LIMIT 1",
                             (agent_id,))[0]
        step_pass(f"resource uploaded: {doc_name} ({len(doc_bytes)} bytes, real demo-docs markdown content "
                  f"as .txt, resource_id={resource_id}; summarize_resource queued -> GAUSSDB super_agi_vectors)")

        # ---- 7. trigger execution ----
        r = requests.post(BASE + "/agentexecutions/add",
                          json={"agent_id": agent_id, "name": RUN_NAME,
                                "goal": GOAL, "instruction": INSTRUCTION}, timeout=120)
        if r.status_code != 201:
            step_fail(f"/agentexecutions/add -> {r.status_code} {r.text[:500]}")
        run = r.json()
        exec_id = run["id"]
        if run["status"] != "RUNNING":
            step_fail(f"execution status is {run['status']}, expected RUNNING")
        step_pass(f"execution triggered: id={exec_id} status=RUNNING (execute_agent.delay via celery+redis)")

        # ---- 8. wait for feeds ----
        row = poll_db(
            "SELECT COUNT(*) FROM agent_execution_feeds WHERE agent_execution_id=%s", (exec_id,),
            lambda v: v[0] > 0, timeout=600, what="feeds")
        if not row:
            print(tail_log(os.path.join(LOGDIR, f"celery_{E2E_PORT}.log"), 60), flush=True)
            print(tail_log(os.path.join(LOGDIR, f"uvicorn_{E2E_PORT}.log"), 30), flush=True)
            step_fail("no agent_execution_feeds rows within 600s")
        feed_count = row[0]
        feed = db_one(
            "SELECT role, LEFT(feed, 120) FROM agent_execution_feeds "
            "WHERE agent_execution_id=%s ORDER BY id DESC LIMIT 1", (exec_id,))
        print(f"  last feed: role={feed[0]} content={feed[1]!r}", flush=True)
        step_pass(f"agent_execution_feeds populated: rows={feed_count} (celery -> AgentExecutor -> Ollama qwen3:4b)")

        # ---- 9. terminal status ----
        row = poll_db(
            "SELECT status FROM agent_executions WHERE id=%s", (exec_id,),
            lambda v: v[0] in ("COMPLETED", "ITERATION_LIMIT_EXCEEDED", "TERMINATED", "WAITING_FOR_PERMISSION"),
            timeout=600, what="terminal status")
        if not row:
            status_now = db_one("SELECT status, num_of_calls FROM agent_executions WHERE id=%s", (exec_id,))
            print("  current:", status_now, flush=True)
            print(tail_log(os.path.join(LOGDIR, f"celery_{E2E_PORT}.log"), 60), flush=True)
            step_fail("execution did not reach terminal status within 600s")
        step_pass(f"execution reached terminal status: {row[0]}")

        # ---- 10. events JSONB ----
        ev = db_one("SELECT event_name, event_property FROM events "
                    "WHERE event_property->>'agent_execution_id'=%s AND event_name='run_created' LIMIT 1",
                    (str(exec_id),))
        if not ev:
            step_fail("run_created event not found via JSONB query")
        step_pass(f"events JSONB queryable: event_name={ev[0]} event_property={json.dumps(ev[1])[:160]}")

        # ---- 11. LTM vector table (super-agent-index1) ----
        row = poll_db(
            "SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='super-agent-index1'",
            None, lambda v: v is not None, timeout=120, what="LTM table")
        if not row:
            print(tail_log(os.path.join(LOGDIR, f"celery_{E2E_PORT}.log"), 40), flush=True)
            step_fail("LTM table super-agent-index1 was not created")
        ltm_rows = poll_db(
            'SELECT COUNT(*) FROM "super-agent-index1"', None,
            lambda v: v[0] > 0, timeout=300, what="LTM rows")
        if not ltm_rows:
            step_fail("LTM table super-agent-index1 has no rows")
        hits = gauss_search("super-agent-index1", db_one(
            'SELECT text FROM "super-agent-index1" LIMIT 1')[0])
        ltm_idx, ltm_dim = assert_vector_table_shape("super-agent-index1")
        step_pass(f"LTM GaussDB vector table super-agent-index1: rows={ltm_rows[0]}, dim={ltm_dim}, "
                  f"index=GsIVFFLAT, <+> self-retrieval score={hits[0][3]:.4f}")

        # ---- 12. resource vector table (super_agi_vectors) + demo-docs semantic hit ----
        row = poll_db(
            "SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='super_agi_vectors'",
            None, lambda v: v is not None, timeout=180, what="resource vector table")
        if not row:
            print(tail_log(os.path.join(LOGDIR, f"celery_{E2E_PORT}.log"), 40), flush=True)
            step_fail("resource vector table super_agi_vectors was not created")
        rv_rows = poll_db(
            'SELECT COUNT(*) FROM super_agi_vectors', None,
            lambda v: v[0] > 0, timeout=300, what="resource vector rows")
        if not rv_rows:
            step_fail("super_agi_vectors has no rows (summarize_resource failed?)")
        rv_idx, rv_dim = assert_vector_table_shape("super_agi_vectors")
        # 语义命中：用 05 文档问题检索，期待召回 3.1 三级水位阈值块（红 > 85%）
        hits = gauss_search("super_agi_vectors", DEMO_DOC_QUERY, top_k=5)
        top = hits[0]
        hit_texts = [h[1] for h in hits]
        semantic_hit = any("85" in t for t in hit_texts)
        rid_ok = any((h[2] or {}).get("resource_id") == str(resource_id) for h in hits)
        if not semantic_hit:
            step_fail(f"demo-docs semantic miss: no '85%' chunk in top5 for query {DEMO_DOC_QUERY!r}")
        if not rid_ok:
            step_fail(f"resource ownership mismatch: no chunk with metadata.resource_id={resource_id}")
        step_pass(f"resource vector table super_agi_vectors: rows={rv_rows[0]}, dim={rv_dim}, "
                  f"index=GsIVFFLAT, semantic hit on demo-docs: query={DEMO_DOC_QUERY!r} "
                  f"top_score={top[3]:.4f}, top_text={top[1][:80]!r}")

        # ---- 13. cross-db isolation check ----
        if IS_ISOLATED_RUN:
            central_cross_check(baseline, AGENT_NAME, RUN_NAME)

        print(f"[PASS] E2E: boot + agent run + feeds/events + vector on GaussDB "
              f"(Ollama local models, target={DB['dbname']}@{DB['port']}, uvicorn:{E2E_PORT})",
              flush=True)
        return 0
    except AssertionError as e:
        print("E2E FAILED:", e, flush=True)
        return 1
    except Exception as e:
        import traceback
        traceback.print_exc()
        print("E2E FAILED (unexpected):", e, flush=True)
        return 1
    finally:
        for p in (uvicorn_p, celery_p):
            if p is not None and p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    p.kill()
        for f in (uvicorn_log, celery_log):
            f.close()


if __name__ == "__main__":
    print(f"t6 e2e boot start: {datetime.now()}", flush=True)
    code = main()
    print(f"t6 e2e boot end: {datetime.now()} exit={code}", flush=True)
    sys.exit(code)
