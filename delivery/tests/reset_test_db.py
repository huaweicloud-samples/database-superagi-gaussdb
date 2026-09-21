"""重置目标库：DROP 全部表后重跑 alembic upgrade head。
用法: venv-gauss python reset_test_db.py [TARGET_URL]
TARGET_URL 默认集中式 super_agi_test；ADMIN 连接从同一 URL 解析
（host/port/user/password/dbname），要求该账号有权 DROP 表。"""
import os
import subprocess
import sys
from urllib.parse import unquote, urlparse

import psycopg2

BASE_DIR = r"D:\workplace\code\SuperAGI\SuperAGI-0.0.14"
TARGET_URL = sys.argv[1] if len(sys.argv) > 1 else \
    "opengauss+psycopg2://superagi_test:GaussTest2026@127.0.0.1:5432/super_agi_test"

_p = urlparse(TARGET_URL)
ADMIN = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
             password=unquote(_p.password), dbname=_p.path.lstrip("/"),
             connect_timeout=10)

conn = psycopg2.connect(**ADMIN)
conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public'")
tables = [r[0] for r in cur.fetchall()]
for t in tables:
    cur.execute(f'DROP TABLE IF EXISTS "{t}" CASCADE')
conn.close()
print(f"dropped {len(tables)} tables")

env = os.environ.copy()
env["DB_URL"] = TARGET_URL
env["ENCRYPTION_KEY"] = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
subprocess.run([os.path.join(BASE_DIR, ".venv-gauss", "Scripts", "alembic.exe"), "upgrade", "head"],
               cwd=BASE_DIR, env=env, check=True)
print("migrations replayed to head")
