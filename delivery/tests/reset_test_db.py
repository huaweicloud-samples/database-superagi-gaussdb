"""重置 super_agi_test：DROP 全部表后重跑 alembic upgrade head。
用法: venv-gauss python reset_test_db.py"""
import os
import subprocess
import psycopg2

BASE_DIR = r"D:\workplace\code\SuperAGI\SuperAGI-0.0.14"
ADMIN = dict(host="127.0.0.1", port=5432, user="superagi_test",
             password="GaussTest2026", dbname="super_agi_test", connect_timeout=10)

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
env["DB_URL"] = "opengauss+psycopg2://superagi_test:GaussTest2026@127.0.0.1:5432/super_agi_test"
env["ENCRYPTION_KEY"] = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
subprocess.run([os.path.join(BASE_DIR, ".venv-gauss", "Scripts", "alembic.exe"), "upgrade", "head"],
               cwd=BASE_DIR, env=env, check=True)
print("migrations replayed to head")
