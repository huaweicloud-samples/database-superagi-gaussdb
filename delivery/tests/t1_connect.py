"""T1: 连通与方言验证。用法: venv-gauss python t1_connect.py"""
import sys
from sqlalchemy import create_engine, text

sys.path.insert(0, r"D:\workplace\code\SuperAGI\SuperAGI-0.0.14")

URL = "opengauss+psycopg2://superagi_test:GaussTest2026@127.0.0.1:5432/super_agi_test"


def main():
    eng = create_engine(URL)
    with eng.connect() as c:
        ver = c.execute(text("SELECT version()")).scalar()
        compat = c.execute(text(
            "SELECT datcompatibility FROM pg_database WHERE datname = current_database()")).scalar()
        dialect = eng.dialect.name
    assert dialect == "opengauss", f"dialect={dialect}"
    assert compat == "A", f"datcompatibility={compat}"
    print(f"[PASS] dialect={dialect}, datcompatibility={compat}")
    print(f"       server: {ver[:60]}")


if __name__ == "__main__":
    main()
