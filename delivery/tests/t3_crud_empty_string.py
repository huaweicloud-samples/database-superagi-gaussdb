"""T3: ORM CRUD + 空串拦截验证。用法: venv-gauss python t3_crud_empty_string.py"""
import os
import sys

# 必须在 import superagi 之前设置：config.py 在 import 时用 os.environ 快照
# 构造 _config_instance，之后设置环境变量不会生效；encyption_helper 在
# import 时也要求 ENCRYPTION_KEY（32 字符）。
os.environ["DB_URL"] = "opengauss+psycopg2://superagi_test:GaussTest2026@127.0.0.1:5432/super_agi_test"
os.environ["ENCRYPTION_KEY"] = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

sys.path.insert(0, r"D:\workplace\code\SuperAGI\SuperAGI-0.0.14")

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from superagi.helper.db_connection_helper import build_database_url, register_gaussdb_compat
from superagi.models.organisation import Organisation
from superagi.models.configuration import Configuration

engine = create_engine(build_database_url(), pool_pre_ping=True)
register_gaussdb_compat(engine)
Session = sessionmaker(bind=engine)
db = Session()

# 1) ORM CRUD
org = Organisation(name="gaussdb-test-org")
db.add(org)
db.commit()
assert org.id is not None, "organisation id not returned"
print(f"[PASS] ORM insert Organisation id={org.id}")

got = db.query(Organisation).filter(Organisation.id == org.id).first()
assert got.name == "gaussdb-test-org"
print("[PASS] ORM query roundtrip")

# 2) 空串写入（guard 应存为单个空格而非 NULL）
db.add(Configuration(key="gaussdb_test_empty", value=""))
db.commit()
row = db.query(Configuration).filter(Configuration.key == "gaussdb_test_empty").first()
assert row.value == " ", f"empty string not guarded, got {row.value!r}"
print("[PASS] empty-string guard: '' stored as ' '")

# 3) JSONB 写入（events 表）不受 guard 影响
from superagi.models.events import Event
ev = Event(event_name="t3", event_value=1, event_property={"k": "v", "empty": ""})
db.add(ev)
db.commit()
db.expire(ev)
got_ev = db.query(Event).filter(Event.id == ev.id).first()
assert got_ev.event_property["k"] == "v"
print("[PASS] JSONB event_property write/read")

# 清理
db.query(Configuration).filter(Configuration.key == "gaussdb_test_empty").delete()
db.query(Event).filter(Event.id == ev.id).delete()
db.query(Organisation).filter(Organisation.id == org.id).delete()
db.commit()
db.close()
print("T3 done")
