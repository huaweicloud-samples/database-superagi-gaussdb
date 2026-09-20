import pytest

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
    url = "opengauss+psycopg2://u:p%40ss@h:5432/db"
    assert h.escape_for_alembic_option(url) == url.replace("%", "%%")
