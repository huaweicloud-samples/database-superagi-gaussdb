"""T1: 连通与方言验证。用法: venv-gauss python t1_connect.py [TARGET_URL] [EXPECTED_COMPAT]

TARGET_URL 默认集中式 super_agi_test；EXPECTED_COMPAT 默认 'A'（集中式），
分布式库 datcompatibility='ORA' 时显式传第二参数。
"""
import sys
from urllib.parse import unquote, urlparse

from sqlalchemy import create_engine, text

sys.path.insert(0, r"D:\workplace\code\SuperAGI\SuperAGI-0.0.14")

TARGET_URL = sys.argv[1] if len(sys.argv) > 1 else \
    "opengauss+psycopg2://superagi_test:GaussTest2026@127.0.0.1:5432/super_agi_test"
EXPECTED_COMPAT = sys.argv[2] if len(sys.argv) > 2 else "A"


def main():
    eng = create_engine(TARGET_URL)
    with eng.connect() as c:
        ver = c.execute(text("SELECT version()")).scalar()
        compat = c.execute(text(
            "SELECT datcompatibility FROM pg_database WHERE datname = current_database()")).scalar()
        dialect = eng.dialect.name
    assert dialect == "opengauss", f"dialect={dialect}"
    assert compat == EXPECTED_COMPAT, f"datcompatibility={compat}, expected {EXPECTED_COMPAT}"
    print(f"[PASS] dialect={dialect}, datcompatibility={compat}")
    print(f"       server: {ver[:60]}")


if __name__ == "__main__":
    main()
