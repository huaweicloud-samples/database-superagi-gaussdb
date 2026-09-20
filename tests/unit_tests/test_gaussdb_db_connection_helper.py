from superagi.helper import db_connection_helper as h


def _patch_config(monkeypatch, **kwargs):
    """替换单测内 get_config，避免依赖真实 config.yaml。"""
    def fake_get_config(key, default=None):
        return kwargs.get(key, default)
    monkeypatch.setattr(h, "get_config", fake_get_config)


def test_build_url_without_db_url_uses_opengauss_scheme_and_quotes_password(monkeypatch):
    _patch_config(monkeypatch, DB_HOST="127.0.0.1", DB_PORT="5432", DB_USERNAME="appuser",
                  DB_PASSWORD="App@User456", DB_NAME="super_agi_test")
    url = h.build_database_url()
    assert url.startswith("opengauss+psycopg2://")
    assert url.endswith("/super_agi_test")
    assert "App%40User456" in url          # @ 必须 percent-encode，否则 make_url 按第一个 @ 截断
    assert "127.0.0.1:5432" in url


def test_build_url_without_username(monkeypatch):
    _patch_config(monkeypatch, DB_HOST="g", DB_USERNAME=None, DB_NAME="db")
    assert h.build_database_url() == "opengauss+psycopg2://g/db"


def test_build_url_keeps_query_params(monkeypatch):
    _patch_config(monkeypatch, DB_URL="opengauss+psycopg2://u:p%40ss@127.0.0.1:5432/db?sslmode=require")
    url = h.build_database_url()
    assert url.endswith("?sslmode=require")   # 修复原 urlparse 丢 query 的 bug


def test_build_url_plain_url_passthrough(monkeypatch):
    _patch_config(monkeypatch, DB_URL="postgresql://u:p@h:5432/db")
    assert h.build_database_url() == "postgresql://u:p@h:5432/db"


def test_escape_empty_strings_in_dict():
    out = h._escape_empty_strings({"a": "", "b": "x", "c": {"n": ""}, "d": None})
    assert out == {"a": " ", "b": "x", "c": {"n": ""}, "d": None}


def test_escape_empty_strings_in_batch():
    out = h._escape_empty_strings([{"a": ""}, {"b": ""}])
    assert out == [{"a": " "}, {"b": " "}]


def test_escape_for_alembic_option():
    assert h.escape_for_alembic_option("opengauss+psycopg2://u:p%40ss@h:5432/db") == "opengauss+psycopg2://u:p%%40ss@h:5432/db"


def test_escape_empty_strings_in_positional_params():
    out = h._escape_empty_strings(("", "x", None))
    assert out == (" ", "x", None)


def test_register_gaussdb_compat_replaces_empty_string_params():
    # 回归锁：监听器须按 retval 事件契约恒返回二元组，且位置参数
    # （qmark/format 方言在 cursor 层是 tuple）里的空串被替换。
    # 2.0.16 实证 dispatch 集合 __call__ 不返回结果，故走真实 engine
    # INSERT 端到端断言（若监听器返回 None，调用点解包会直接 TypeError）。
    from sqlalchemy import create_engine, text

    eng = create_engine("sqlite://")
    h.register_gaussdb_compat(eng)
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (v VARCHAR(10))"))
        conn.execute(text("INSERT INTO t (v) VALUES (:v)"), {"v": ""})
        val = conn.execute(text("SELECT v FROM t")).scalar()
    assert val == " "


def test_register_is_global_and_idempotent():
    # Engine 类级注册：注册后新建的引擎（不经任何显式注册，对齐
    # FastAPI DBSessionMiddleware 内部自建 engine 无法传参的场景）也必须
    # 拦截空串；重复调用幂等不炸（同 target 重复 listen 会累积注册）。
    from sqlalchemy import create_engine, text

    h.register_gaussdb_compat(create_engine("sqlite://"))
    h.register_gaussdb_compat(create_engine("sqlite://"))

    eng = create_engine("sqlite://")   # 注册后新建，显式注册零调用
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (v VARCHAR(10))"))
        conn.execute(text("INSERT INTO t (v) VALUES (:v)"), {"v": ""})
        val = conn.execute(text("SELECT v FROM t")).scalar()
    assert val == " "
