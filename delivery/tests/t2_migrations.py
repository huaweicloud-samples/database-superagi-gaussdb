"""T2: 迁移终态验证。用法: venv-gauss python t2_migrations.py"""
import psycopg2

conn = psycopg2.connect(host="127.0.0.1", port=5432, user="superagi_test",
                        password="GaussTest2026", dbname="super_agi_test", connect_timeout=10)
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
