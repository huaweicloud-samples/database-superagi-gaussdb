"""GaussDB 507 兼容的数据库连接构建与运行时行为适配。

统一 db.py / main.py / migrations/env.py 三处连接构建：
- 默认方言 opengauss+psycopg2（GaussDB 的 version() 字符串无法被
  postgresql 方言解析，会抛 "Could not determine version from string"）。
- DB_URL 完整透传（含 query 参数，修复原 urlparse 重组丢参的问题）。
- 拼串分支对密码做 percent-encode（密码含 @ 时 make_url 会按第一个
  @ 截断，导致 host 解析错误）。
- A 兼容模式下空串写入会被静默转为 NULL（NOT NULL 列直接报错），
  register_gaussdb_compat 注册 before_cursor_execute 事件把空串参数
  统一替换为单个空格。
"""
from urllib.parse import quote, urlparse, urlunparse

from sqlalchemy import Engine, event

from superagi.config.config import get_config

_compat_registered = False


def build_database_url() -> str:
    """按配置构建 SQLAlchemy 连接 URL。"""
    db_url = get_config('DB_URL', None)
    if db_url:
        parsed = urlparse(db_url)
        if parsed.query:
            return urlunparse(parsed)
        return parsed.scheme + "://" + parsed.netloc + parsed.path

    db_host = get_config('DB_HOST', 'super__postgres')
    db_port = get_config('DB_PORT', '5432')
    db_username = get_config('DB_USERNAME')
    db_password = get_config('DB_PASSWORD')
    db_name = get_config('DB_NAME')
    if db_username is None:
        return f'opengauss+psycopg2://{db_host}/{db_name}'
    return (f'opengauss+psycopg2://{quote(str(db_username), safe="")}:'
            f'{quote(str(db_password or ""), safe="")}@{db_host}:{db_port}/{db_name}')


def escape_for_alembic_option(url: str) -> str:
    """alembic set_main_option 走 configparser 插值，% 必须双写。"""
    return url.replace("%", "%%")


def _escape_empty_strings(parameters):
    """把 SQL 参数里的空串替换为单个空格（递归处理批量参数）。

    dict 是命名参数：只替换顶层的 str 值，不递归进嵌套 dict
    （JSONB 列的 dict 参数是内容本身，其中的空串不能动）。
    list/tuple 是批量参数或位置参数（psycopg2 format/qmark 方言把
    位置参数交给 cursor 时是 tuple）：逐元素递归，裸空串也必须替换。
    """
    if isinstance(parameters, dict):
        return {k: (' ' if isinstance(v, str) and v == '' else v) for k, v in parameters.items()}
    if isinstance(parameters, (list, tuple)):
        return type(parameters)(_escape_empty_strings(p) for p in parameters)
    if isinstance(parameters, str) and parameters == '':
        return ' '
    return parameters


def register_gaussdb_compat(engine=None) -> None:
    """全局注册 GaussDB A 兼容模式所需的运行时行为修正（Engine 类级）。

    为什么类级：FastAPI 的 DBSessionMiddleware(db_url=...) 在内部自建 engine，
    外部拿不到引用、无法对其显式注册；Engine 类级监听对已存在和未来创建的
    所有 engine 实例统一生效。engine 参数仅为兼容旧调用点（main.py /
    db.py 显式传参），不参与注册。

    幂等：模块级 _compat_registered 防重复，重复调用直接 return
    （同一 target 重复 listen 会累积注册多个监听器，必须防）。

    retval=True 必需：缺省时监听器返回值会被 SQLAlchemy 丢弃（拦截变 no-op）；
    带 retval 后返回值被无条件解包，因此必须恒返回 (statement, parameters) 二元组。
    """
    global _compat_registered
    if _compat_registered:
        return
    _compat_registered = True

    @event.listens_for(Engine, "before_cursor_execute", retval=True)
    def _convert_empty_strings(conn, cursor, statement, parameters, context, executemany):
        return statement, _escape_empty_strings(parameters)
