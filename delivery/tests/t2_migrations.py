"""T2: 迁移终态验证。用法: venv-gauss python t2_migrations.py [TARGET_URL]"""
import sys
from urllib.parse import unquote, urlparse

import psycopg2

TARGET_URL = sys.argv[1] if len(sys.argv) > 1 else \
    "opengauss+psycopg2://superagi_test:GaussTest2026@127.0.0.1:5432/super_agi_test"

_p = urlparse(TARGET_URL)
conn = psycopg2.connect(host=_p.hostname, port=_p.port or 5432, user=_p.username,
                        password=unquote(_p.password), dbname=_p.path.lstrip("/"),
                        connect_timeout=10)
cur = conn.cursor()
cur.execute("SELECT version_num FROM alembic_version")
head = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM pg_tables WHERE schemaname='public'")
count = cur.fetchone()[0]
cur.execute("SELECT data_type FROM information_schema.columns "
            "WHERE table_name='events' AND column_name='event_property'")
col = cur.fetchone()[0]
assert head == "9270eb5a8475", f"alembic head={head}"
assert count >= 38, f"table count={count}"
assert col == "jsonb", f"events.event_property={col}"
print(f"[PASS] head={head}, tables={count}, events.event_property={col}")
