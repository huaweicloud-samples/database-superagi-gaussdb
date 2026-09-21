# -*- coding: utf-8 -*-
"""Task 9 t6: 端到端验收 —— SuperAGI 双进程（uvicorn + celery）在 GaussDB 上用 Ollama 本地模型跑通一次 Agent 执行。

流程：
  1. 启动 uvicorn（main:app, 默认 127.0.0.1:8001，可 E2E_PORT 覆盖）
  2. 启动 celery worker（superagi.worker, --pool=solo）
  3. /users/add 注册（DEV 自动建 org+project）→ /login 拿 JWT
  4. 登记 OpenAI provider（api_key=ollama）+ models 行（qwen3:4b）
  5. /agents/create（Goal Based Workflow, LTM_DB=GAUSSDB）
  6. /resources/add/{agent_id} 上传资源（触发 summarize_resource → super_agi_vectors）
  7. /agentexecutions/add 触发执行（execute_agent.delay）
  8. 轮询 agent_execution_feeds / agent_executions.status
  9. psycopg2 直查 super_agi_e2e 验证 feeds / events JSONB / 向量表 <+> 检索
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
# 目标参数化：argv[1..3] 优先，环境变量 E2E_URL / E2E_PORT / E2E_REDIS_DB 兜底，
# 默认值保持集中式现状（super_agi_e2e @5432、uvicorn 8001、redis db 0）。
E2E_URL = sys.argv[1] if len(sys.argv) > 1 else os.environ.get(
    "E2E_URL", "opengauss+psycopg2://superagi_test:GaussTest2026@127.0.0.1:5432/super_agi_e2e")
E2E_PORT = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("E2E_PORT", "8001")
E2E_REDIS_DB = sys.argv[3] if len(sys.argv) > 3 else os.environ.get("E2E_REDIS_DB", "0")
_p = urlparse(E2E_URL)
DB = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=unquote(_p.password), dbname=_p.path.lstrip("/"))
BASE = f"http://127.0.0.1:{E2E_PORT}"
LOGDIR = os.path.join(ROOT, "delivery", "tests", "_t6_logs")
OLLAMA = "http://localhost:11434/v1"
GOAL = ["Introduce yourself and the database you run on in one short sentence."]
INSTRUCTION = ["Answer in one short sentence.", "No tools are available, just answer directly."]
AGENT_PAYLOAD = {
    "name": "e2e-gauss-agent",
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
    sql = (f'SELECT id, text, 1 - (embedding <+> CAST(%(q)s AS floatvector)) AS score '
           f'FROM "{table}" ORDER BY embedding <+> CAST(%(q)s AS floatvector) LIMIT %(k)s')
    with db_conn() as c:
        with c.cursor() as cur:
            cur.execute(sql, {"q": vec, "k": top_k})
            return cur.fetchall()


def tail_log(path, n=40):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return "".join(f.readlines()[-n:])
    except OSError as e:
        return f"(no log: {e})"


def main():
    os.makedirs(LOGDIR, exist_ok=True)
    env = os.environ.copy()
    env["OPENAI_API_BASE"] = "http://localhost:11434/v1"  # openai 库 import 期读该环境变量
    env["HF_ENDPOINT"] = "https://hf-mirror.com"  # py3.8 下 llama_index 用 transformers GPT2 分词器，走可达镜像
    env["PYTHONUNBUFFERED"] = "1"
    if E2E_REDIS_DB != "0":
        # superagi config 环境变量优先于 config.yaml；注意 superagi/worker.py 的
        # broker_url/result_backend 硬编码追加 "/0"，非 0 db 在产品代码放开前不会真正生效。
        import yaml
        with open(os.path.join(ROOT, "config.yaml"), "r", encoding="utf-8") as f:
            _redis_base = (yaml.safe_load(f) or {}).get("REDIS_URL", "127.0.0.1:6379")
        env["REDIS_URL"] = f"{_redis_base}/{E2E_REDIS_DB}"
        print(f"  [warn] E2E_REDIS_DB={E2E_REDIS_DB}: injected REDIS_URL={env['REDIS_URL']} "
              f"(worker.py hardcodes broker '/0' — non-zero db may not take effect)", flush=True)

    uvicorn_log = open(os.path.join(LOGDIR, "uvicorn.log"), "w", encoding="utf-8")
    celery_log = open(os.path.join(LOGDIR, "celery.log"), "w", encoding="utf-8")
    uvicorn_p = celery_p = None
    try:
        # ---- 1. uvicorn ----
        uvicorn_p = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", str(E2E_PORT)],
            cwd=ROOT, stdout=uvicorn_log, stderr=subprocess.STDOUT, env=env)
        if not wait_api():
            print(tail_log(os.path.join(LOGDIR, "uvicorn.log")), flush=True)
            step_fail("uvicorn API did not become ready")
        step_pass(f"boot: uvicorn main:app ready at {BASE} (GaussDB e2e db)")

        # ---- 2. celery worker ----
        celery_p = subprocess.Popen(
            [sys.executable, "-m", "celery", "-A", "superagi.worker", "worker",
             "--loglevel=info", "--pool=solo"],
            cwd=ROOT, stdout=celery_log, stderr=subprocess.STDOUT, env=env)
        time.sleep(10)
        if celery_p.poll() is not None:
            print(tail_log(os.path.join(LOGDIR, "celery.log")), flush=True)
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

        # ---- 6. resource upload (summarize -> super_agi_vectors) ----
        note_content = ("GaussDB vector acceptance note: SuperAGI agent runs on GaussDB with "
                        "qwen3:4b local model and qwen3-embedding vectors stored in super_agi_vectors table.")
        note_path = os.path.join(LOGDIR, "e2e_note.txt")
        with open(note_path, "w", encoding="utf-8") as f:
            f.write(note_content)
        with open(note_path, "rb") as f:
            r = requests.post(f"{BASE}/resources/add/{agent_id}",
                              files={"file": ("e2e_note.txt", f, "text/plain")},
                              data={"name": "e2e_note.txt", "size": str(len(note_content.encode())),
                                    "type": "text/txt"}, timeout=120)
        if r.status_code not in (200, 201):
            step_fail(f"/resources/add -> {r.status_code} {r.text[:300]}")
        step_pass("resource uploaded (summarize_resource queued -> GAUSSDB super_agi_vectors)")

        # ---- 7. trigger execution ----
        r = requests.post(BASE + "/agentexecutions/add",
                          json={"agent_id": agent_id, "name": "e2e-run-1",
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
            print(tail_log(os.path.join(LOGDIR, "celery.log"), 60), flush=True)
            print(tail_log(os.path.join(LOGDIR, "uvicorn.log"), 30), flush=True)
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
            print(tail_log(os.path.join(LOGDIR, "celery.log"), 60), flush=True)
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
            print(tail_log(os.path.join(LOGDIR, "celery.log"), 40), flush=True)
            step_fail("LTM table super-agent-index1 was not created")
        ltm_rows = poll_db(
            'SELECT COUNT(*) FROM "super-agent-index1"', None,
            lambda v: v[0] > 0, timeout=300, what="LTM rows")
        if not ltm_rows:
            step_fail("LTM table super-agent-index1 has no rows")
        hits = gauss_search("super-agent-index1", db_one(
            'SELECT text FROM "super-agent-index1" LIMIT 1')[0])
        step_pass(f"LTM GaussDB vector table super-agent-index1: rows={ltm_rows[0]}, "
                  f"<+> self-retrieval score={hits[0][2]:.4f}")

        # ---- 12. resource vector table (super_agi_vectors) ----
        row = poll_db(
            "SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='super_agi_vectors'",
            None, lambda v: v is not None, timeout=180, what="resource vector table")
        if not row:
            print(tail_log(os.path.join(LOGDIR, "celery.log"), 40), flush=True)
            step_fail("resource vector table super_agi_vectors was not created")
        rv_rows = poll_db(
            'SELECT COUNT(*) FROM super_agi_vectors', None,
            lambda v: v[0] > 0, timeout=300, what="resource vector rows")
        if not rv_rows:
            step_fail("super_agi_vectors has no rows (summarize_resource failed?)")
        hits = gauss_search("super_agi_vectors", "GaussDB vector acceptance note SuperAGI agent")
        step_pass(f"resource vector table super_agi_vectors: rows={rv_rows[0]}, "
                  f"<+> retrieval top score={hits[0][2]:.4f}, text={hits[0][1][:60]!r}")

        print("[PASS] E2E: boot + agent run + feeds/events + vector on GaussDB (Ollama local models)",
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
