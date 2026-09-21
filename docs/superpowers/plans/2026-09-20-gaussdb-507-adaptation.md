# SuperAGI × GaussDB 507 适配实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 SuperAGI 0.0.14 的元数据存储与向量存储完整运行在 GaussDB 507（A 兼容模式，集中式 + 分布式）上。

**Architecture:** 三处数据库连接构建收敛为共享 helper（默认 `opengauss+psycopg2` 方言 + 空串拦截事件）；新增 `GaussDB` 向量后端（floatvector 原生类型 + GsIVFFLAT/GsDiskANN 索引）挂入双工厂，与 llama_index 适配类共享同一张向量表；交付物沿用 dify/crewAI/n8n 同构的 delivery 模式。

**Tech Stack:** SQLAlchemy 2.0.16 + Alembic 1.11.1 + psycopg2 + opengauss-sqlalchemy 2.4.0；GaussDB 原生向量（非 pgvector，无 CREATE EXTENSION）。

---

## 背景速查（零上下文必读）

**环境凭据**（集中式容器 `gaussdb`，host 127.0.0.1:5432）：
- 测试库/用户：`superagi_test / GaussTest2026 @ super_agi_test`（独立库+独立用户，owner 关系已配好，public schema 已 GRANT CREATE）
- 应用用户：`appuser / App@User456`（密码含 `@`，是 URL 编码坑的来源，本计划用 `superagi_test` 做验证）
- 管理员路径：`docker exec gaussdb su - gausscore -c "export LD_LIBRARY_PATH=/opt/gaussdb/app/lib; /opt/gaussdb/app/bin/gsql -d <db> -c '<sql>'"`
- venv：`SuperAGI-0.0.14\.venv-gauss`（Python 3.8，已装 sqlalchemy 2.0.16/alembic 1.11.1/psycopg2-binary 2.9.9/opengauss-sqlalchemy 2.4.0/pydantic 1.10.8/fastapi 0.103.2/loguru/cryptography/pyyaml/requests/openai 0.28.1/tenacity/tiktoken）。**本项目目录不是 git 仓库，venv 目录名 `.venv-gauss`。**
- 通用命令前缀（cwd = `D:\workplace\code\SuperAGI\SuperAGI-0.0.14`）：
  - 测试：`& .\.venv-gauss\Scripts\python.exe -m pytest <path> -v`
  - 迁移：`$env:DB_URL='opengauss+psycopg2://superagi_test:GaussTest2026@127.0.0.1:5432/super_agi_test'; $env:ENCRYPTION_KEY='AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'; & .\.venv-gauss\Scripts\alembic.exe upgrade head`

**已验证结论（调研+验证阶段，报告在 `../survey/superagi-gaussdb-调研报告.md`）**：
1. `postgresql+psycopg2` 方言连 GaussDB 报 `Could not determine version from string`，必须用 `opengauss+psycopg2`（opengauss-sqlalchemy 2.4.0）
2. 34 个 Alembic 迁移在去除 `e39295ec089c` 的 JSONB btree 索引后全量重放通过（**该迁移文件已在验证阶段修改完成**，本计划不再改它）
3. A 模式：JSONB/数组/BOOLEAN/UUID/SERIAL 列 ✅；`->`/`->>`/`::int`/`array_agg(DISTINCT)`/`extract(epoch)`/ILIKE/RETURNING ✅；`ON CONFLICT` ❌（本项目运行时未用）；`gen_random_uuid()` ❌、`uuid()` ✅
4. 向量：`floatvector(n)` 建表必须定维（≤4096）；≤1024 用 `GSIVFFLAT(col cosine) WITH (IVF_NLIST=256)`；>1024 必须 `GSDISKANN(col cosine) WITH (pq_nseg=<整除维度>, pq_nclus=16, enable_pq=true, subgraph_count=1, enable_vector_copy=false)` + 建索引前 `SET maintenance_work_mem='512MB'`；检索 `ORDER BY col <+> :q LIMIT k`，score=1-distance；1536 维全链路已实测通过
5. 空串→NULL：NOT NULL 列写 `''` 硬失败、可空列静默转 NULL → 必须 `before_cursor_execute` 全局拦截（`''`→`' '`）
6. 非 sysadmin 禁止在 public schema 建函数 → 向量后端全部客户端计算，零 DB 函数依赖

**接口契约（已读源码确认）**：
- `superagi/vector_store/base.py`：5 个方法。实现返回形态跟随现有后端：`get_matching_text` 返回 `{"documents": [...]}`；`get_index_stats` 返回 `{"dimensions": int, "vector_count": int}`（pinecone.py:103-111）；`add_embeddings_to_vector_db` 兼容 `{"ids": [...], "vectors": [...], "payloads": [...]}`（qdrant 形态，knowledges.py:130-133 消费）
- `add_texts(texts, metadatas=None, embeddings=None, ids=None)`：ids 缺省时逐条 `str(uuid.uuid4())`（pinecone.py:67）
- 枚举 `superagi/types/vector_store_types.py:VectorStoreType`；工厂 `superagi/vector_store/vector_factory.py`（get_vector_storage:18-83 / build_vector_storage:86-110）与 `superagi/resource_manager/llama_vector_store_factory.py`（llama-index==0.6.35）
- `superagi/models/db.py:connect_db()`（Celery/后台用 engine，模块级单例）与 `main.py:64-89`（FastAPI 主进程 engine + DBSessionMiddleware）目前各自拼 `postgresql://` URL；`migrations/env.py:31-95` 同构逻辑。三者统一改为共享 helper。

**关键坑（实现时必须遵守）**：
- 密码含 `@`：URL 里必须 percent-encode（`quote(pwd, safe='')`）；alembic 的 `set_main_option` 会把 `%` 当插值语法，写入前必须 `url.replace('%', '%%')`
- `DB_URL` 存在时原代码 `urlparse` 重组会**丢弃 query 参数**（db.py:34-35/main.py:77-78/env.py:57-58 同 bug），本次修复保留 query
- 向量表名即 `index_name`，拼接 SQL 前必须用正则校验（`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`）
- 分布式形态 floatvector 建表硬限 1024 维（超限报 `dimensions for type vector cannot exceed 1024`），GaussDB 后端不做额外门禁（建表报错即暴露，注意事项文档中说明）

---

### Task 0: git 仓库与测试环境准备

**Files:**
- Create: `.gitignore`

- [x] **Step 1: git init 与基线提交**

```powershell
Set-Location "D:\workplace\code\SuperAGI\SuperAGI-0.0.14"
git init
git add -A
git commit -m "chore: SuperAGI 0.0.14 baseline (pre-GaussDB adaptation)"
```

- [x] **Step 2: 写 .gitignore（在基线提交之后追加，避免仓库里出现环境与本地配置）**

在项目根创建 `.gitignore`，内容：

```gitignore
.venv-gauss/
venv/
config.yaml
workspace/
gui/node_modules/
__pycache__/
*.pyc
.pytest_cache/
```

- [x] **Step 3: 安装 pytest 到 venv-gauss**

```powershell
uv pip install -p "D:\workplace\code\SuperAGI\SuperAGI-0.0.14\.venv-gauss\Scripts\python.exe" --index-url https://pypi.tuna.tsinghua.edu.cn/simple "pytest==7.3.2"
```

Expected: `+ pytest==7.3.2`（requirements.txt:99 本就钉 7.3.2）

- [x] **Step 4: 验证 pytest 可运行（跑一个空收集）**

```powershell
& .\.venv-gauss\Scripts\python.exe -m pytest tests/unit_tests --collect-only -q 2>&1 | Select-Object -First 5
```

Expected: 输出收集信息（原有单测可能因缺依赖报 collection error，属预期——只确认 pytest 本身工作，本项目新增测试放 `tests/unit_tests/test_gaussdb_*.py`，不受既有测试缺依赖影响；若整个目录 collection error 阻塞，后续步骤改用 `pytest tests/unit_tests/test_gaussdb_*.py` 指定文件运行）

- [x] **Step 5: Commit**

```powershell
git add .gitignore
git commit -m "chore: add gitignore for gaussdb adaptation"
```

---

### Task 1: 连接层共享 helper（URL 构建 + 空串拦截）

**Files:**
- Create: `superagi/helper/db_connection_helper.py`
- Test: `tests/unit_tests/test_gaussdb_db_connection_helper.py`

- [x] **Step 1: 写失败的单测**

创建 `tests/unit_tests/test_gaussdb_db_connection_helper.py`：

```python
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
```

- [x] **Step 2: 运行测试确认失败**

```powershell
& .\.venv-gauss\Scripts\python.exe -m pytest tests/unit_tests/test_gaussdb_db_connection_helper.py -v
```

Expected: FAIL，`ModuleNotFoundError: No module named 'superagi.helper.db_connection_helper'`

- [x] **Step 3: 实现 helper**

创建 `superagi/helper/db_connection_helper.py`：

```python
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

from sqlalchemy import event

from superagi.config.config import get_config


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
    return (f'opengauss+psycopg2://{quote(str(db_username))}:'
            f'{quote(str(db_password or ""), safe="")}@{db_host}:{db_port}/{db_name}')


def escape_for_alembic_option(url: str) -> str:
    """alembic set_main_option 走 configparser 插值，% 必须双写。"""
    return url.replace("%", "%%")


def _escape_empty_strings(parameters):
    """把 SQL 参数里的空串替换为单个空格（递归处理批量参数）。

    只处理 str 类型：JSONB 列的 dict 参数、None、数字等不受影响。
    """
    if isinstance(parameters, dict):
        return {k: (' ' if isinstance(v, str) and v == '' else v) for k, v in parameters.items()}
    if isinstance(parameters, (list, tuple)):
        return type(parameters)(_escape_empty_strings(p) for p in parameters)
    return parameters


def register_gaussdb_compat(engine) -> None:
    """在 engine 上注册 GaussDB A 兼容模式所需的运行时行为修正。"""

    @event.listens_for(engine, "before_cursor_execute")
    def _convert_empty_strings(conn, cursor, statement, parameters, context, executemany):
        new_params = _escape_empty_strings(parameters)
        if new_params is not parameters:
            return statement, new_params
```

- [x] **Step 4: 运行测试确认通过**

```powershell
& .\.venv-gauss\Scripts\python.exe -m pytest tests/unit_tests/test_gaussdb_db_connection_helper.py -v
```

Expected: 7 passed

- [x] **Step 5: Commit**

```powershell
git add superagi/helper/db_connection_helper.py tests/unit_tests/test_gaussdb_db_connection_helper.py
git commit -m "feat: shared GaussDB connection helper with url building and empty-string guard"
```

---

### Task 2: 三处连接构建与 Alembic env.py 切换

**Files:**
- Modify: `superagi/models/db.py:1-52`
- Modify: `main.py:61-89`
- Modify: `migrations/env.py:1-35,74-97`

- [x] **Step 1: 改造 db.py**

`superagi/models/db.py` 全文替换为：

```python
from sqlalchemy import create_engine
from superagi.config.config import get_config
from superagi.helper.db_connection_helper import build_database_url, register_gaussdb_compat
from superagi.lib.logger import logger

engine = None


def connect_db():
    """
    Connects to the database using SQLAlchemy (GaussDB 507 compatible).

    Returns:
        engine: The SQLAlchemy engine object representing the database connection.
    """

    global engine
    if engine is not None:
        return engine

    db_url = build_database_url()
    engine = create_engine(db_url,
                           pool_size=20,  # Maximum number of database connections in the pool
                           max_overflow=50,  # Maximum number of connections that can be created beyond the pool_size
                           pool_timeout=30,  # Timeout value in seconds for acquiring a connection from the pool
                           pool_recycle=1800,  # Recycle connections after this number of seconds (optional)
                           pool_pre_ping=False,  # Enable connection health checks (optional)
                           )
    register_gaussdb_compat(engine)

    # Test the connection
    try:
        connection = engine.connect()
        logger.info("Connected to the database! @ " + db_url)
        connection.close()
    except Exception as e:
        logger.error(f"Unable to connect to the database:{e}")
    return engine
```

说明：原 `get_config`/`urlparse` import 不再被使用，按 surgical 原则一并移除（本次改动使其成为孤儿）。

- [x] **Step 2: 改造 main.py 连接段**

`main.py:61-89` 段（从 `from urllib.parse import urlparse` 到 `app.add_middleware(DBSessionMiddleware, db_url=db_url)`）替换为：

```python
from superagi.helper.db_connection_helper import build_database_url, register_gaussdb_compat
app = FastAPI()

db_url = build_database_url()

engine = create_engine(db_url,
                       pool_size=20,  # Maximum number of database connections in the pool
                       max_overflow=50,  # Maximum number of connections that can be created beyond the pool_size
                       pool_timeout=30,  # Timeout value in seconds for acquiring a connection from the pool
                       pool_recycle=1800,  # Recycle connections after this number of seconds (optional)
                       pool_pre_ping=False,  # Enable connection health checks (optional)
                       )
register_gaussdb_compat(engine)

# app.add_middleware(DBSessionMiddleware, db_url=f'postgresql://{db_username}:{db_password}@localhost/{db_name}')
app.add_middleware(DBSessionMiddleware, db_url=db_url)
```

说明：原 `db_host/db_username/db_password/db_name/urlparse` 变量与 import 全部由 helper 接管；先确认 `main.py` 其他位置（如 `main.py:104-106` 附近）没有引用这些变量再删除——若有引用，保留对应变量行并改为 `db_url = build_database_url()` 之后不动其他。

- [x] **Step 3: 改造 migrations/env.py**

`migrations/env.py` 顶部 import 区（:21-24 保留，其余连接相关行）改为：

```python
from superagi.helper.db_connection_helper import build_database_url, escape_for_alembic_option
```

删除 `from urllib.parse import urlparse` 与 `db_host/db_username/db_password/db_name/database_url` 的五个 get_config 赋值行（:31-35 与 :82-86 两处）。

`run_migrations_offline` 内改为：

```python
def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    config.set_main_option("sqlalchemy.url", escape_for_alembic_option(build_database_url()))

    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()
```

`run_migrations_online` 内改为：

```python
def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    config.set_main_option('sqlalchemy.url', escape_for_alembic_option(build_database_url()))
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()
```

- [x] **Step 4: Alembic 全量回归（downgrade + upgrade）**

```powershell
Set-Location "D:\workplace\code\SuperAGI\SuperAGI-0.0.14"
$env:DB_URL = 'opengauss+psycopg2://superagi_test:GaussTest2026@127.0.0.1:5432/super_agi_test'
$env:ENCRYPTION_KEY = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
& .\.venv-gauss\Scripts\alembic.exe downgrade base
& .\.venv-gauss\Scripts\alembic.exe upgrade head
```

Expected: downgrade 逐迁移回退到 base 无错误（`e39295ec089c` 的 downgrade 已对称注释 JSONB 索引行）；upgrade 34 步到 head `9270eb5a8475`。
若 downgrade 在某步报错：记录报错迁移号，改用清库脚本重置（`& .\.venv-gauss\Scripts\python.exe delivery\tests\reset_test_db.py`，脚本在 Task 6 Step 2 创建；Task 6 未完成前可先用 `docker exec gaussdb su - gausscore -c "...gsql -d super_agi_test -c 'DROP SCHEMA public CASCADE; CREATE SCHEMA public;'"` 后重跑 upgrade head——注意 gausscore 会成为新 public owner，需再执行 `ALTER SCHEMA public OWNER TO superagi_test; GRANT CREATE, USAGE ON SCHEMA public TO superagi_test;`），并把 downgrade 失败点写进 delivery 注意事项文档。

- [x] **Step 5: Commit**

```powershell
git add superagi/models/db.py main.py migrations/env.py
git commit -m "feat: unify db connection building via gaussdb helper (opengauss dialect + empty-string guard)"
```

---

### Task 3: GaussDB 向量后端核心

**Files:**
- Create: `superagi/vector_store/gaussdb.py`
- Test: `tests/unit_tests/test_gaussdb_vector_store.py`

- [x] **Step 1: 写失败的单测（纯函数部分，不连库）**

创建 `tests/unit_tests/test_gaussdb_vector_store.py`：

```python
import pytest

from superagi.vector_store.gaussdb import GaussDB, calc_pq_nseg, _table_ddl, _index_ddl


class FakeEmbedding:
    def get_embedding(self, text):
        return [0.1, 0.2, 0.3]


def test_calc_pq_nseg_small_dims():
    assert calc_pq_nseg(384) == 384
    assert calc_pq_nseg(512) == 512


def test_calc_pq_nseg_mid_dims():
    assert calc_pq_nseg(1024) == 512


def test_calc_pq_nseg_large_dims():
    assert calc_pq_nseg(1536) == 96
    assert calc_pq_nseg(3072) == 96
    assert calc_pq_nseg(4096) == 128


def test_calc_pq_nseg_prime_fallback():
    assert calc_pq_nseg(1025) == 1025   # 质数回退：pq_nseg=dim 恒整除


def test_table_ddl_uses_floatvector_with_dim():
    ddl = _table_ddl("my_index")
    assert '"my_index"' in ddl
    assert "id VARCHAR(36) PRIMARY KEY" in ddl
    assert "embedding floatvector" in ddl and "NOT NULL" in ddl
    assert "metadata JSONB" in ddl


def test_index_ddl_ivfflat_under_1024():
    ddl = _index_ddl("my_index", 768)
    assert "USING GSIVFFLAT" in ddl
    assert "IVF_NLIST = 256" in ddl
    assert "cosine" in ddl


def test_index_ddl_diskann_above_1024():
    ddl = _index_ddl("my_index", 1536)
    assert "USING GSDISKANN" in ddl
    assert "pq_nseg=96" in ddl and "pq_nclus=16" in ddl
    assert "enable_pq=true" in ddl and "subgraph_count=1" in ddl
    assert "enable_vector_copy=false" in ddl


def test_table_name_validation():
    with pytest.raises(ValueError):
        GaussDB("bad name; DROP TABLE x", FakeEmbedding())
    with pytest.raises(ValueError):
        GaussDB("", FakeEmbedding())
```

- [x] **Step 2: 运行测试确认失败**

```powershell
& .\.venv-gauss\Scripts\python.exe -m pytest tests/unit_tests/test_gaussdb_vector_store.py -v
```

Expected: FAIL，`ModuleNotFoundError: No module named 'superagi.vector_store.gaussdb'`

- [x] **Step 3: 实现 gaussdb.py**

创建 `superagi/vector_store/gaussdb.py`：

```python
"""GaussDB 507 原生向量后端（floatvector 类型 + GsIVFFLAT/GsDiskANN 索引）。

维度策略（集中式）：
- dim <= 1024: GsIVFFLAT(embedding cosine) WITH (IVF_NLIST=256)
- dim > 1024:  GsDiskANN+PQ，pq_nseg 必须整除维度（calc_pq_nseg）
分布式形态 floatvector 建表硬限 1024 维，超限由数据库建表报错直接暴露。

向量表建在元数据库同实例（复用 build_database_url()），可用
GAUSSDB_VECTOR_DB_URL 配置覆盖为独立连接。检索统一使用余弦距离
<+>，score = 1 - distance。不依赖任何数据库端函数（非 sysadmin
禁止在 public schema 建函数），向量字面量由客户端构造。
"""
import json
import re
import uuid
from typing import Any, Iterable, List, Optional

from sqlalchemy import create_engine, text

from superagi.config.config import get_config
from superagi.helper.db_connection_helper import build_database_url
from superagi.lib.logger import logger
from superagi.vector_store.base import VectorStore
from superagi.vector_store.document import Document

_TABLE_NAME_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]{0,62}$')


def calc_pq_nseg(dim: int) -> int:
    """GsDiskANN+PQ 的 pq_nseg：必须整除维度。

    官方经验（gaussdb-doc 向量索引GsDiskANN）：<=512 取维度本身，
    <=1024 取维度一半，>1024 从 [96,128,192,256,384,512] 取首个整除值
    （1536->96, 4096->128），质数回退维度本身（恒整除）。
    """
    if dim <= 512:
        return dim
    if dim <= 1024:
        return dim // 2
    for candidate in (96, 128, 192, 256, 384, 512):
        if dim % candidate == 0:
            return candidate
    return dim


def _table_ddl(table_name: str) -> str:
    return (f'CREATE TABLE "{table_name}" ('
            f'id VARCHAR(36) PRIMARY KEY, '
            f'text TEXT NOT NULL, '
            f'metadata JSONB, '
            f'embedding floatvector({{dim}}) NOT NULL)')


def _index_ddl(table_name: str, dim: int) -> str:
    if dim <= 1024:
        return (f'CREATE INDEX "{table_name}_ivf" ON "{table_name}" '
                f'USING GSIVFFLAT(embedding cosine) WITH (IVF_NLIST = 256)')
    nseg = calc_pq_nseg(dim)
    return (f'CREATE INDEX "{table_name}_dsk" ON "{table_name}" '
            f'USING GSDISKANN(embedding cosine) WITH (pq_nseg={nseg}, pq_nclus=16, '
            f'enable_pq=true, subgraph_count=1, enable_vector_copy=false)')


def _vec_literal(values) -> str:
    """把浮点序列编码为 floatvector 文本字面量 '[x,y,...]'。"""
    return "[" + ",".join(repr(float(v)) for v in values) + "]"


class GaussDB(VectorStore):
    """基于 GaussDB 原生向量能力的向量存储（表名即 index_name）。"""

    def __init__(self, index_name: str, embedding_model: Any = None, db_url: Optional[str] = None):
        if not _TABLE_NAME_RE.match(index_name or ''):
            raise ValueError(f"Invalid index (table) name: {index_name!r}")
        self.index_name = index_name
        self.embedding_model = embedding_model
        url = db_url or get_config('GAUSSDB_VECTOR_DB_URL') or build_database_url()
        self.engine = create_engine(url, pool_size=5, pool_pre_ping=True)
        self._ensured_dim = None

    # ---- schema ----

    def _ensure_schema(self, dim: int) -> None:
        """惰性建表建索引；dim 由首批 embedding 决定，之后不再重复执行。"""
        if self._ensured_dim == dim:
            return
        with self.engine.begin() as conn:
            table_exists = conn.execute(text(
                "SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = :t"),
                {"t": self.index_name}).fetchone()
            if not table_exists:
                conn.execute(text(_table_ddl(self.index_name).format(dim=dim)))
                logger.info(f"GaussDB vector table {self.index_name} created (dim={dim})")
                conn.execute(text("SET maintenance_work_mem = '512MB'"))
                conn.execute(text(_index_ddl(self.index_name, dim)))
                logger.info(f"GaussDB vector index on {self.index_name} created (dim={dim})")
            else:
                index_exists = conn.execute(text(
                    "SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND tablename = :t"),
                    {"t": self.index_name}).fetchone()
                if not index_exists:
                    conn.execute(text("SET maintenance_work_mem = '512MB'"))
                    conn.execute(text(_index_ddl(self.index_name, dim)))
                    logger.info(f"GaussDB vector index on {self.index_name} created (dim={dim})")
        self._ensured_dim = dim

    # ---- VectorStore 接口 ----

    def add_texts(self, texts: Iterable[str],
                  metadatas: Optional[List[dict]] = None,
                  embeddings: Optional[List[List[float]]] = None,
                  ids: Optional[List[str]] = None,
                  **kwargs: Any) -> List[str]:
        texts = list(texts)
        ids = ids or [str(uuid.uuid4()) for _ in texts]
        metadatas = [metadatas[i] if metadatas and i < len(metadatas) else {} for i in range(len(texts))]
        if embeddings is None:
            embeddings = [self.embedding_model.get_embedding(t) for t in texts]
        self._ensure_schema(len(embeddings[0]))
        with self.engine.begin() as conn:
            for i in range(len(texts)):
                conn.execute(text(
                    f'INSERT INTO "{self.index_name}" (id, text, metadata, embedding) '
                    f'VALUES (:id, :t, CAST(:m AS JSONB), CAST(:e AS floatvector))'),
                    {"id": ids[i], "t": texts[i],
                     "m": json.dumps(metadatas[i]), "e": _vec_literal(embeddings[i])})
        return ids

    def query_by_embedding(self, embedding: List[float], top_k: int = 5,
                           metadata: Optional[dict] = None) -> list:
        """内部检索原语：返回 (id, text, metadata, score) 行列表。

        metadata 过滤走 JSONB ->> 比较。注意：过滤条件不在向量索引内
        生效（索引后过滤），命中数可能少于 top_k，见交付注意事项。
        """
        self._ensure_schema(len(embedding))
        params: dict = {"q": _vec_literal(embedding), "k": top_k}
        where = ""
        if metadata:
            clauses = []
            for idx, (k, v) in enumerate(metadata.items()):
                clauses.append(f"metadata->>:mk{idx} = :mv{idx}")
                params[f"mk{idx}"] = k
                params[f"mv{idx}"] = str(v)
            where = "WHERE " + " AND ".join(clauses)
        sql = (f'SELECT id, text, metadata, 1 - (embedding <+> CAST(:q AS floatvector)) AS score '
               f'FROM "{self.index_name}" {where} '
               f'ORDER BY embedding <+> CAST(:q AS floatvector) LIMIT :k')
        with self.engine.connect() as conn:
            return conn.execute(text(sql), params).fetchall()

    def get_matching_text(self, query: str, top_k: int = 5,
                          metadata: Optional[dict] = None, **kwargs: Any) -> dict:
        """返回 {"documents": [...]}（对齐现有后端返回形态）。"""
        embed_text = self.embedding_model.get_embedding(query)
        rows = self.query_by_embedding(embed_text, top_k, metadata)
        documents = [
            Document(text_content=r[1], metadata={**(r[2] or {}), "id": r[0], "score": r[3]})
            for r in rows
        ]
        return {"documents": documents}

    def get_index_stats(self) -> dict:
        with self.engine.connect() as conn:
            count = conn.execute(text(f'SELECT COUNT(*) FROM "{self.index_name}"')).scalar()
            dims = None
            if self._ensured_dim is not None:
                dims = self._ensured_dim
            else:
                row = conn.execute(text(
                    f'SELECT vector_to_array(embedding) FROM "{self.index_name}" LIMIT 1')).fetchone()
                if row and row[0] is not None:
                    dims = len(row[0])
        return {"dimensions": dims, "vector_count": int(count)}

    def add_embeddings_to_vector_db(self, embeddings: dict) -> None:
        """兼容两种入参形态：
        {"vectors": [(id, embedding, metadata), ...]}（pinecone 形态）
        {"ids": [...], "vectors": [...], "payloads": [...]}（qdrant 形态）
        """
        if "ids" in embeddings and "payloads" in embeddings:
            ids = embeddings["ids"]
            vectors = embeddings["vectors"]
            payloads = embeddings["payloads"]
        else:
            ids = [v[0] for v in embeddings["vectors"]]
            vectors = [v[1] for v in embeddings["vectors"]]
            payloads = [v[2] for v in embeddings["vectors"]]
        self._ensure_schema(len(vectors[0]))
        with self.engine.begin() as conn:
            for tid, emb, meta in zip(ids, vectors, payloads):
                conn.execute(text(
                    f'INSERT INTO "{self.index_name}" (id, text, metadata, embedding) '
                    f'VALUES (:id, :t, CAST(:m AS JSONB), CAST(:e AS floatvector)) '
                    f'ON DUPLICATE KEY UPDATE text = :t, metadata = CAST(:m AS JSONB), '
                    f'embedding = CAST(:e AS floatvector)'),
                    {"id": str(tid), "t": (meta or {}).get("text", ""),
                     "m": json.dumps(meta or {}), "e": _vec_literal(emb)})

    def delete_embeddings_from_vector_db(self, ids: List[str]) -> None:
        with self.engine.begin() as conn:
            conn.execute(text(f'DELETE FROM "{self.index_name}" WHERE id = ANY(:ids)'), {"ids": list(ids)})
```

说明：
- `ON DUPLICATE KEY UPDATE` 是 A 模式下的 upsert 写法（`ON CONFLICT` 不支持，见背景速查第 3 条）
- metadata 里存 text（对齐 pinecone.py:73 `metadata[self.text_field] = text`），upsert 时 text 取 `meta.get("text", "")`

- [x] **Step 4: 运行测试确认通过**

```powershell
& .\.venv-gauss\Scripts\python.exe -m pytest tests/unit_tests/test_gaussdb_vector_store.py -v
```

Expected: 8 passed

- [x] **Step 5: Commit**

```powershell
git add superagi/vector_store/gaussdb.py tests/unit_tests/test_gaussdb_vector_store.py
git commit -m "feat: GaussDB native vector store backend (floatvector + GsIVFFLAT/GsDiskANN)"
```

---

### Task 4: 枚举注册与双工厂挂载（含 llama_index 适配）

**Files:**
- Modify: `superagi/types/vector_store_types.py:10`
- Modify: `superagi/vector_store/vector_factory.py:76-83,86-110`
- Create: `superagi/vector_store/llama_gaussdb.py`
- Modify: `superagi/resource_manager/llama_vector_store_factory.py:51-59`

- [x] **Step 1: 枚举加 GAUSSDB**

`superagi/types/vector_store_types.py:10` 的 `LANCEDB = 'LanceDB'` 行后加：

```python
    GAUSSDB = 'gaussdb'
```

- [x] **Step 2: vector_factory 挂载**

`superagi/vector_store/vector_factory.py` 顶部 import 区加：

```python
from superagi.vector_store.gaussdb import GaussDB
```

`get_vector_storage` 的 `if vector_store == VectorStoreType.REDIS:` 分支（:77-81）之前加：

```python
        if vector_store == VectorStoreType.GAUSSDB:
            return GaussDB(index_name, embedding_model)
```

`build_vector_storage` 的 `if vector_store == VectorStoreType.WEAVIATE:` 分支（:105-110）之前加：

```python
        if vector_store == VectorStoreType.GAUSSDB:
            return GaussDB(index_name, embedding_model, db_url=creds.get("url"))
```

说明：`get_vector_storage` 不在工厂里探维度建表（Redis 分支需要 `create_index()`，GaussDB 后端是惰性 ensure，首次写入时才建）。

- [x] **Step 3: llama_index 0.6.35 适配类**

先确认 llama_index 0.6.35 实际 API（venv-gauss 未装 llama_index，用全局 python 检查其已装版本，若未装则跳过本步检查、代码按 0.6.35 文档签名写并在 Task 6 t5 脚本中验证）：

```powershell
python -c "import llama_index, inspect; from llama_index.vector_stores.types import VectorStore; print(llama_index.__version__); print([m for m in dir(VectorStore) if not m.startswith('_')])"
```

创建 `superagi/vector_store/llama_gaussdb.py`：

```python
"""llama_index 0.6.x VectorStore 协议的 GaussDB 适配（Resource Manager 用）。

与 VectorFactory 的 GaussDB 后端共享同一张表（index_name 即表名）：
Resource Manager 经 llama 侧写入、query_resource 经主后端检索，数据互通。
"""
from typing import Any, List, Optional

from superagi.vector_store.gaussdb import GaussDB


class LlamaGaussDBVectorStore:
    """实现 llama_index 0.6.x VectorStore 协议（stores_text）。"""

    stores_text: bool = True

    def __init__(self, index_name: str, db_url: Optional[str] = None):
        self._store = GaussDB(index_name, embedding_model=None, db_url=db_url)

    @property
    def client(self) -> Any:
        return self._store.engine

    def add(self, embedding_results) -> None:
        """embedding_results: llama_index NodeEmbedding 列表（TypedDict：
        {id, embedding, node, extra_info}），node 取 get_content()。"""
        texts, metas, embs, ids = [], [], [], []
        for r in embedding_results:
            node = r["node"]
            texts.append(node.get_content())
            metas.append(dict(node.metadata or {}))
            embs.append(r["embedding"])
            ids.append(r["id"])
        self._store.add_texts(texts, metadatas=metas, embeddings=embs, ids=ids)

    def delete(self, ref_doc_id: str, **delete_kwargs) -> None:
        self._store.delete_embeddings_from_vector_db([ref_doc_id])

    def query(self, query, **kwargs):
        """query: llama_index VectorStoreQuery（query_embedding/similarity_top_k）。

        返回 VectorStoreQueryResult(nodes, similarities, ids)。
        """
        from llama_index.vector_stores.types import VectorStoreQueryResult
        from llama_index.schema import TextNode

        rows = self._store.query_by_embedding(query.query_embedding,
                                              query.similarity_top_k or 5)
        nodes = [TextNode(id_=r[0], text=r[1], metadata=r[2] or {}) for r in rows]
        return VectorStoreQueryResult(
            nodes=nodes,
            similarities=[r[3] for r in rows],
            ids=[r[0] for r in rows],
        )
```

- [x] **Step 4: llama_vector_store_factory 挂载**

`superagi/resource_manager/llama_vector_store_factory.py` 的 `if self.vector_store_name == VectorStoreType.QDRANT:` 分支之前加：

```python
        if self.vector_store_name == VectorStoreType.GAUSSDB:
            from superagi.vector_store.llama_gaussdb import LlamaGaussDBVectorStore
            return LlamaGaussDBVectorStore(self.index_name)
```

- [x] **Step 5: 语法与 import 校验（不连库）**

```powershell
& .\.venv-gauss\Scripts\python.exe -c "from superagi.types.vector_store_types import VectorStoreType; print(VectorStoreType.GAUSSDB); from superagi.vector_store.llama_gaussdb import LlamaGaussDBVectorStore; print('llama adapter import ok')"
```

Expected: `VectorStoreType.GAUSSDB` 与 `llama adapter import ok`（llama_gaussdb 的 llama_index import 在方法内部惰性执行，构造实例不触发）

- [x] **Step 6: Commit**

```powershell
git add superagi/types/vector_store_types.py superagi/vector_store/vector_factory.py superagi/vector_store/llama_gaussdb.py superagi/resource_manager/llama_vector_store_factory.py
git commit -m "feat: register GAUSSDB vector store type in both factories with llama_index adapter"
```

---

### Task 5: 依赖与配置模板

**Files:**
- Modify: `requirements.txt:92`（psycopg2==2.9.6 行之后）
- Modify: `config_template.yaml:22-29,104-112`
- Create: `.env.gaussdb.example`

- [x] **Step 1: requirements.txt 加方言包**

在 `requirements.txt` 的 `psycopg2==2.9.6`（:92）行后插入一行：

```
opengauss-sqlalchemy==2.4.0
```

- [x] **Step 2: config_template.yaml 更新**

`DATABASE INFO` 段（:22-29）替换为：

```yaml
#DATABASE INFO (GaussDB 507 compatible)
# 方式一：分项配置（方言默认 opengauss+psycopg2，密码无需手动编码）
DB_NAME: super_agi_main
DB_HOST: gaussdb-host
DB_PORT: 5432
DB_USERNAME: superagi
DB_PASSWORD: password
# 方式二：完整 URL（scheme 需为 opengauss+psycopg2；密码含特殊字符时需 percent-encode）
#DB_URL: opengauss+psycopg2://superagi:password@gaussdb-host:5432/super_agi_main
REDIS_URL: "super__redis:6379"
```

`## RESOURCE_VECTOR_STORE` 注释段（:104-109）替换为：

```yaml
## To config a vector store for resources manager uncomment config below
## RESOURCE_VECTOR_STORE can be GAUSSDB, REDIS, PINECONE, CHROMA, QDRANT
#RESOURCE_VECTOR_STORE: GAUSSDB
#RESOURCE_VECTOR_STORE_INDEX_NAME: super_agi_vectors

## To use GaussDB native vector store (floatvector + GsIVFFLAT/GsDiskANN).
## 默认与元数据同库；如需独立连接，取消注释并填写完整 URL
#GAUSSDB_VECTOR_DB_URL: opengauss+psycopg2://superagi:password@gaussdb-host:5432/super_agi_vectors
```

- [x] **Step 3: 创建 .env.gaussdb.example**

```bash
# GaussDB 507 deployment example (copy to config.yaml or export as env vars)
# 元数据存储：GaussDB A 兼容模式库（CREATE DATABASE super_agi_main DBCOMPATIBILITY='A' ENCODING='UTF8';）
DB_HOST=127.0.0.1
DB_PORT=5432
DB_USERNAME=superagi
DB_PASSWORD=yourpassword
DB_NAME=super_agi_main

# 或完整 URL 方式（密码含 @ : / 等特殊字符时需 percent-encode）
#DB_URL=opengauss+psycopg2://superagi:yourpassword@127.0.0.1:5432/super_agi_main

# 向量存储：GaussDB 原生向量（与元数据同库，无需额外配置）
RESOURCE_VECTOR_STORE=GAUSSDB
RESOURCE_VECTOR_STORE_INDEX_NAME=super_agi_vectors
LTM_DB=GAUSSDB

# Redis（Celery broker/结果后端/任务队列，仍需 Redis；不再需要 redis-stack 的 RediSearch）
REDIS_URL=127.0.0.1:6379

# 部署前置（管理员在目标库执行）：
#   GRANT CREATE, USAGE ON SCHEMA public TO <user>;
# 注意：非 sysadmin 不能在 public schema 建函数（本适配零 DB 函数依赖，无需处理）
```

- [x] **Step 4: Commit**

```powershell
git add requirements.txt config_template.yaml .env.gaussdb.example
git commit -m "chore: gaussdb config templates and opengauss-sqlalchemy dependency"
```

---

### Task 6: 真库分层测试（delivery/tests）

**Files:**
- Create: `delivery/tests/t1_connect.py`
- Create: `delivery/tests/t2_migrations.py`
- Create: `delivery/tests/t3_crud_empty_string.py`
- Create: `delivery/tests/t4_vector.py`
- Create: `delivery/tests/reset_test_db.py`

- [x] **Step 1: t1 连通测试**

创建 `delivery/tests/t1_connect.py`：

```python
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
```

- [x] **Step 2: reset 工具 + t2 迁移状态测试**

创建 `delivery/tests/reset_test_db.py`（管理员清理后重放迁移的辅助脚本，仅测试库使用）：

```python
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
```

创建 `delivery/tests/t2_migrations.py`：

```python
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
```

- [x] **Step 3: t3 CRUD + 空串拦截测试**

创建 `delivery/tests/t3_crud_empty_string.py`：

```python
"""T3: ORM CRUD + 空串拦截验证。用法: venv-gauss python t3_crud_empty_string.py"""
import sys

sys.path.insert(0, r"D:\workplace\code\SuperAGI\SuperAGI-0.0.14")

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from superagi.helper.db_connection_helper import build_database_url, register_gaussdb_compat
from superagi.models.organisation import Organisation
from superagi.models.configuration import Configuration

import os
os.environ["DB_URL"] = "opengauss+psycopg2://superagi_test:GaussTest2026@127.0.0.1:5432/super_agi_test"

engine = create_engine(build_database_url(), pool_pre_ping=True)
register_gaussdb_compat(engine)   # 空串拦截挂载点
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

# 2) 空串写入（拦截后应存为单个空格而非 NULL）
db.add(Configuration(key="gaussdb_test_empty", value=""))
db.commit()
row = db.query(Configuration).filter(Configuration.key == "gaussdb_test_empty").first()
assert row.value == " ", f"empty string not guarded, got {row.value!r}"
print("[PASS] empty-string guard: '' stored as ' '")

# 3) JSONB 写入（events 表）不受拦截影响
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
```

说明：`Configuration.value` 列若为 nullable String，空串拦截后写入 `' '`；若该列 NOT NULL，拦截前写入会直接违反约束——两种情况本测试都验证拦截生效。若 `Configuration` 模型字段名与实际不符（执行时以 `superagi/models/configuration.py` 为准），同构调整。

- [x] **Step 4: t4 向量后端测试（mock embedding，无需 API key）**

创建 `delivery/tests/t4_vector.py`：

```python
"""T4: GaussDB 向量后端全链路（mock 1536 维 embedding，覆盖 GsDiskANN 路线）。
用法: venv-gauss python t4_vector.py"""
import os
import random
import sys

sys.path.insert(0, r"D:\workplace\code\SuperAGI\SuperAGI-0.0.14")

from superagi.vector_store.gaussdb import GaussDB


class MockEmbedding1536:
    """固定种子的 1536 维伪 embedding，替代 OpenAI API。"""
    def get_embedding(self, text):
        rng = random.Random(text)
        return [rng.uniform(-1, 1) for _ in range(1536)]


TABLE = "t4_gaussdb_vectors"

store = GaussDB(TABLE, MockEmbedding1536(),
                db_url="opengauss+psycopg2://superagi_test:GaussTest2026@127.0.0.1:5432/super_agi_test")

ids = store.add_texts(["alpha doc", "beta doc", "gamma doc"],
                      metadatas=[{"agent_id": 1}, {"agent_id": 1}, {"agent_id": 2}])
assert len(ids) == 3 and all(len(i) == 36 for i in ids)
print(f"[PASS] add_texts -> 3 ids (table={TABLE}, dim=1536, GsDiskANN+PQ)")

stats = store.get_index_stats()
assert stats["dimensions"] == 1536 and stats["vector_count"] == 3, stats
print(f"[PASS] get_index_stats -> {stats}")

res = store.get_matching_text("alpha doc", top_k=2)
docs = res["documents"]
assert len(docs) >= 1 and docs[0].metadata["score"] > 0.99, docs[0].metadata
print(f"[PASS] get_matching_text self-retrieval score={docs[0].metadata['score']:.4f}")

res_f = store.get_matching_text("alpha doc", top_k=3, metadata={"agent_id": 1})
assert all(d.metadata.get("agent_id") == 1 for d in res_f["documents"])
print(f"[PASS] metadata filtered search -> {len(res_f['documents'])} docs (post-filter caveat)")

store.delete_embeddings_from_vector_db([ids[0]])
stats2 = store.get_index_stats()
assert stats2["vector_count"] == 2, stats2
print(f"[PASS] delete_embeddings -> count={stats2['vector_count']}")

# 清理
with store.engine.begin() as conn:
    conn.execute(__import__("sqlalchemy").text(f'DROP TABLE "{TABLE}"'))
print("T4 done")
```

- [x] **Step 5: 依次运行 t1-t4**

```powershell
Set-Location "D:\workplace\code\SuperAGI\SuperAGI-0.0.14\delivery\tests"
$py = "D:\workplace\code\SuperAGI\SuperAGI-0.0.14\.venv-gauss\Scripts\python.exe"
& $py t1_connect.py
& $py t2_migrations.py
& $py t3_crud_empty_string.py
& $py t4_vector.py
```

Expected: 每个脚本全部 `[PASS]` 行且正常退出。t3 若因模型字段名不符报错，读对应模型文件修正测试（不得为过测试改模型）。

- [x] **Step 6: Commit**

```powershell
Set-Location "D:\workplace\code\SuperAGI\SuperAGI-0.0.14"
git add delivery/tests/
git commit -m "test: delivery tier tests (connect/migrations/crud+empty-string/vector)"
```

---

### Task 7: 交付文档

**Files:**
- Create: `delivery/实施部署交付文档.md`
- Create: `delivery/测试指南.md`
- Create: `delivery/注意事项.md`

- [x] **Step 1: 实施部署交付文档**

创建 `delivery/实施部署交付文档.md`，章节结构（内容要点如下，正文按要点展开写全）：

```markdown
# SuperAGI 0.0.14 × GaussDB 507 实施部署交付文档

## 1. 交付物清单
- 适配代码（git 提交列表：连接 helper/三处连接改造/GaussDB 向量后端/双工厂/配置模板）
- delivery/tests/（t1-t4 分层测试 + reset_test_db）
- .env.gaussdb.example / config_template.yaml

## 2. GaussDB 环境前置条件
- 内核 507.0.0+，enable_vectordb=on（SHOW enable_vectordb 核对；off 时需 gs_guc set + 重启）
- 建库：CREATE DATABASE super_agi_main DBCOMPATIBILITY='A' ENCODING='UTF8';
- 建用户：CREATE USER superagi WITH PASSWORD '...'; ALTER DATABASE super_agi_main OWNER TO superagi;
- 权限：GRANT CREATE, USAGE ON SCHEMA public TO superagi;（库 owner 也需要，GaussDB 507 收紧 public schema）
- 认证：开源 psycopg2 直连（md5 认证场景验证通过；sha256-only 实例需 password_encryption_type=1 + gs_hba md5，参考 dify 适配经验）

## 3. SuperAGI 部署步骤
- config.yaml（按 .env.gaussdb.example；DB_URL scheme 必须 opengauss+psycopg2）
- alembic upgrade head（启动脚本 entrypoint.sh 已内置）
- 验证：alembic current == 9270eb5a8475；39 张表

## 4. 向量存储说明
- RESOURCE_VECTOR_STORE=GAUSSDB / LTM_DB=GAUSSDB；向量表建在元数据库同实例
- 维度策略：<=1024 GsIVFFLAT；>1024（如 ada-002 1536）GsDiskANN+PQ 自动选择
- 分布式形态：floatvector 建表硬限 1024 维，>1024 维 embedding 模型不可用（建表报错即暴露）

## 5. 回滚
- git revert 到 baseline 提交；config.yaml 换回 postgresql:// URL 即回到原 PG 形态
```

- [x] **Step 2: 测试指南**

创建 `delivery/测试指南.md`：

```markdown
# 测试指南

## 分层测试（delivery/tests，依次执行）
1. t1_connect.py — 连通/方言/兼容模式
2. t2_migrations.py — 迁移终态（head=9270eb5a8475，>=38 表，events JSONB）
3. t3_crud_empty_string.py — ORM CRUD + 空串拦截 + JSONB 读写
4. t4_vector.py — 向量后端全链路（1536 维 GsDiskANN+PQ，mock embedding）
执行方式见 delivery/tests 各文件头注释（venv-gauss python 逐个运行）。

## 全量迁移回归
reset_test_db.py 重置测试库并重放 34 个迁移（downgrade base + upgrade head 亦可）。

## 单元测试
.venv-gauss\Scripts\python.exe -m pytest tests/unit_tests/test_gaussdb_*.py -v

## 端到端（可选，需全量依赖 + Redis + OpenAI key）
- docker-compose 起后端，创建 Organisation/Project/Agent，运行一次 Agent 执行
- APM events 落库核对（analytics 页面）

## 结果回填
| 项 | 结果 | 日期 | 执行人 |
|---|---|---|---|
| t1-t4 | | | |
| 迁移回归 | | | |
| 单元测试 | | | |
| 端到端 | | | |
```

- [x] **Step 3: 注意事项**

创建 `delivery/注意事项.md`：

```markdown
# 注意事项（GaussDB 507 适配）

## 密码与连接
- DB_URL 密码含 @ : / ? # 等字符必须 percent-encode（@ -> %40）；分项配置（DB_HOST/DB_USERNAME/DB_PASSWORD）无需编码，推荐使用
- alembic 命令需环境变量 DB_URL + ENCRYPTION_KEY（32 字符）

## A 兼容模式语义差异
- 空串 '' 写入一律转 NULL（适配已用 before_cursor_execute 拦截为空格）：应用层判空请用 IS NULL 或 strip 后比较
- VARCHAR(n) 按字节计数：UTF-8 中文每字 3 字节，自建表/长中文列建议 TEXT
- DATE 映射 TIMESTAMP(0)（mapping_date_to_datea=on）；本适配全用 Python 侧 utcnow 默认，无感

## 向量能力
- 维度上限：集中式 4096（>1024 走 GsDiskANN+PQ）；分布式建表硬限 1024
- metadata 过滤为索引后过滤，带过滤检索命中数可能少于 top_k（GaussDB 向量索引特性）
- GsDiskANN 索引约为原数据 10-50 倍磁盘；数据量变化超 20% 建议重建索引
- 余弦距离下零向量产生 NaN，避免写入全零 embedding
- 非 sysadmin 不能在 public schema 建函数；本适配零 DB 函数依赖，自建扩展时注意

## 分布式形态差异（相对集中式）
- datcompatibility='ORA'（集中式为 'A'），pgxc_node 非空
- 主键即分布键策略、无外键约束；批量写建议 MERGE INTO 单语句（executemany 乱序风险）
- 本适配的 34 个迁移在分布式需按验证阶段结果复核（当前仅集中式全量回归）

## 已知不适用
- ON CONFLICT 语法不可用（代码未使用，MERGE INTO/ON DUPLICATE KEY UPDATE 为替代）
- gen_random_uuid()/uuid-ossp/pgcrypto 不可用；DB 端 uuid() 可用（本适配用应用层 uuid4）
```

- [x] **Step 4: Commit**

```powershell
git add delivery/
git commit -m "docs: delivery documents (deployment/testing/caveats) for gaussdb adaptation"
```

---

### Task 8: 最终回归与收尾

- [x] **Step 1: 全量回归**

```powershell
Set-Location "D:\workplace\code\SuperAGI\SuperAGI-0.0.14"
& .\.venv-gauss\Scripts\python.exe -m pytest tests/unit_tests/test_gaussdb_db_connection_helper.py tests/unit_tests/test_gaussdb_vector_store.py -v
& .\.venv-gauss\Scripts\python.exe delivery\tests\reset_test_db.py
Set-Location delivery\tests
& "D:\workplace\code\SuperAGI\SuperAGI-0.0.14\.venv-gauss\Scripts\python.exe" t1_connect.py
& "D:\workplace\code\SuperAGI\SuperAGI-0.0.14\.venv-gauss\Scripts\python.exe" t2_migrations.py
& "D:\workplace\code\SuperAGI\SuperAGI-0.0.14\.venv-gauss\Scripts\python.exe" t3_crud_empty_string.py
& "D:\workplace\code\SuperAGI\SuperAGI-0.0.14\.venv-gauss\Scripts\python.exe" t4_vector.py
```

Expected: 单测 15 passed；reset 后迁移回放成功；t1-t4 全 [PASS]

- [x] **Step 2: llama 适配冒烟（可选，若 venv 有 llama_index）**

```powershell
python -c "import llama_index; print(llama_index.__version__)"
```

若全局 python 有 llama_index 0.6.35：写 `delivery/tests/t5_llama_smoke.py`（LlamaGaussDBVectorStore add 2 条 mock node + query 返回 VectorStoreQueryResult）并运行；无则跳过并在测试指南结果表标记"未执行（缺 llama_index 环境）"。

- [x] **Step 3: 更新 tasks/todo.md 与调研报告验证记录，git 收尾**

```powershell
Set-Location "D:\workplace\code\SuperAGI\SuperAGI-0.0.14"
git add -A
git commit -m "chore: final regression results and doc updates"
git log --oneline
```

### Task 9: 端到端验收（Ollama 本地模型 + 实例启动）

**Files:**
- Create: `delivery/tests/t6_e2e_boot.py`
- Modify: `superagi/vector_store/embedding/openai.py`（仅当 embedding 模型名硬编码时，加配置覆盖）
- Create: `delivery/e2e-config/README.md`（config.yaml 模板与启动说明）

- [x] **Step 1: 现场探测**

```powershell
ollama list
curl.exe -s http://localhost:11434/v1/models
docker ps --format "{{.Names}}" | Select-String redis
```

确认 Ollama 在跑且有 `qwen3:4b` / `qwen3-embedding:0.6b`（1024 维）；Redis 无则起：`docker run -d --name superagi-e2e-redis -p 6379:6379 redis:7`。

- [x] **Step 2: 建端到端独立库 super_agi_e2e（不影响 super_agi_test）**

```powershell
python -c "import psycopg2; c=psycopg2.connect(host='127.0.0.1',port=5432,user='appuser',password='App@User456',dbname='postgres'); c.autocommit=True; cur=c.cursor(); cur.execute(\"SELECT 1 FROM pg_database WHERE datname='super_agi_e2e'\"); cur.fetchone() or cur.execute(\"CREATE DATABASE super_agi_e2e DBCOMPATIBILITY='A' ENCODING='UTF8'\"); cur.execute('ALTER DATABASE super_agi_e2e OWNER TO superagi_test'); print('db ready')"; docker exec gaussdb su - gausscore -c "export LD_LIBRARY_PATH=/opt/gaussdb/app/lib; /opt/gaussdb/app/bin/gsql -d super_agi_e2e -c 'GRANT CREATE, USAGE ON SCHEMA public TO superagi_test;'"
```

Expected: `db ready` + `GRANT`。注意 e2e 库复用 `superagi_test` 用户（密码无特殊字符）。

- [x] **Step 3: 全量依赖 venv（.venv-e2e，Python 3.8）**

```powershell
py -3.8 -m venv "D:\workplace\code\SuperAGI\SuperAGI-0.0.14\.venv-e2e"
uv pip install -p "D:\workplace\code\SuperAGI\SuperAGI-0.0.14\.venv-e2e\Scripts\python.exe" --index-url https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt
```

若整体解析失败（requirements 内版本互斥/Python 3.8 无 wheel），改为剔除编译巨坑后的清单逐批安装：先装核心（requirements 里除 `llama_cpp_python`、`langchain` 生态外的全部），再装 `llama-index==0.6.35` 及其依赖，最后 langchain 生态；`llama_cpp_python==0.2.7`（C++ 编译，local-llm 可选功能，GAUSSDB 端到端不需要）**跳过**；逐包记录失败项，凡 import 链不需要的可跳过（启动 uvicorn 后按 ImportError 补装）。这是本任务最大风险点，预算单独一轮迭代。

- [x] **Step 4: config.yaml（e2e）与 embedding 模型名核对**

读 `superagi/vector_store/embedding/openai.py`：若模型名硬编码 `text-embedding-ada-002`，加配置覆盖 `get_config("OPENAI_EMBEDDING_MODEL", "text-embedding-ada-002")`（一行改动 + 单测可选）；`OpenAiEmbedding` 的 api_base 确认走 `OPENAI_API_BASE` 配置（config_template.yaml:12 已有此项）。

创建项目根 `config.yaml`：

```yaml
DB_URL: opengauss+psycopg2://superagi_test:GaussTest2026@127.0.0.1:5432/super_agi_e2e
OPENAI_API_KEY: ollama
OPENAI_API_BASE: http://localhost:11434/v1
MODEL_NAME: qwen3:4b
RESOURCES_SUMMARY_MODEL_NAME: qwen3:4b
OPENAI_EMBEDDING_MODEL: qwen3-embedding:0.6b
RESOURCE_VECTOR_STORE: GAUSSDB
RESOURCE_VECTOR_STORE_INDEX_NAME: super_agi_vectors
LTM_DB: GAUSSDB
REDIS_URL: 127.0.0.1:6379
ENV: 'DEV'
ENCRYPTION_KEY: abcdefghijklmnopqrstuvwxyz123456
STORAGE_TYPE: "FILE"
TOOLS_DIR: "superagi/tools"
RESOURCES_INPUT_ROOT_DIR: workspace/input/{agent_id}
RESOURCES_OUTPUT_ROOT_DIR: workspace/output/{agent_id}/{agent_execution_id}
MAX_MODEL_TOKEN_LIMIT: 8192
```

在测试库跑一次迁移：`$env:DB_URL='opengauss+psycopg2://superagi_test:GaussTest2026@127.0.0.1:5432/super_agi_e2e'; $env:ENCRYPTION_KEY='AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'; & .\.venv-gauss\Scripts\alembic.exe upgrade head`（head=9270eb5a8475）。

- [x] **Step 5: 启动实例（t6_e2e_boot.py 前半）**

创建 `delivery/tests/t6_e2e_boot.py`：

```python
"""T6: 实例启动 + API 端到端（Ollama）。
用法: .venv-e2e python t6_e2e_boot.py  （前提: config.yaml、Redis、Ollama、迁移就绪）"""
import json
import subprocess
import sys
import time

import requests

BASE = "http://127.0.0.1:8000"
ROOT = r"D:\workplace\code\SuperAGI\SuperAGI-0.0.14"


def wait_api(timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{BASE}/", timeout=3)
            print("api up, status", r.status_code)
            return True
        except Exception:
            time.sleep(2)
    return False


def main():
    proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"],
                            cwd=ROOT)
    try:
        assert wait_api(), "backend did not come up"
        print("[PASS] SuperAGI backend started on :8000 (GaussDB metadata + GAUSSDB vector store)")
        # API 探活细节（登录/组织/agent 创建/执行）在 Step 6 按实际 auth 流程补全
    finally:
        proc.terminate()


if __name__ == "__main__":
    main()
```

运行：`& .\.venv-e2e\Scripts\python.exe delivery\tests\t6_e2e_boot.py`。启动失败按 traceback 逐个补装缺失包/修配置，直到 `[PASS]`。

- [x] **Step 6: API 端到端流程（agent 创建 + 执行 + 落库验证）**

读 `superagi/controllers/` 下 auth 与 agent 执行相关路由，按实际 API 形状在 `t6_e2e_boot.py` 的 `main()` 中追加完整流程（执行者现场对齐路由签名）：
1. 登录/注册拿 JWT（`ENV=DEV` 下的 auth API，读 `superagi/controllers/auth.py`）
2. 创建 organisation/project（若注册流程自动创建则直接读回）
3. 创建 agent：model=qwen3:4b、goals=["用一句话介绍你自己"]、agent_type/prompt 模板按 API 必填项
4. 触发执行（execute API）→ 轮询 agent_executions.status 到 RUNNING/FINISHED 或取到 agent_execution_feeds 内容
5. 落库验证（psycopg2 直查 super_agi_e2e）：agent_execution_feeds 有行、events 表 JSONB event_property 可查、向量表 `super_agi_vectors`（GsIVFFLAT，1024 维）有行且 `<+>` 可检索
6. 输出端到端验收报告行

全部通过后：`[PASS] E2E: boot + agent run + feeds/events + vector search on GaussDB (Ollama local models)`。

- [x] **Step 7: delivery/e2e-config/README.md 记录启动方法与 config 模板（含踩坑修正）**

- [x] **Step 8: Commit**

```powershell
git add delivery/ superagi/vector_store/embedding/openai.py config_template.yaml
git commit -m "test: e2e acceptance with ollama local models on gaussdb"
```

- [x] **Step 9: 分层测试 t1-t4 复跑确认无回归，更新 tasks/todo.md 验收记录**

---

## Self-Review 记录

- **Spec 覆盖**：调研报告 §4.1（方言/连接 3 处、迁移修正、空串防御、GUC 核对）→ Task 1/2/5；§4.2（向量后端 5 方法、索引策略、score 公式、元数据入库）→ Task 3/4；§4.3（交付四件套）→ Task 6/7；分布式约束（维度门禁说明）→ Task 7 注意事项。迁移文件 e39295ec089c 已在验证阶段改毕，计划中不重复改但回归覆盖（Task 2 Step 4）。
- **Placeholder 扫描**：Task 7 文档类步骤给出完整章节与内容要点（文档正文以要点展开属文档任务常规粒度）；Task 8 Step 2 为条件性步骤（环境探测决定），已写明两个分支的完整动作。其余全部步骤含完整代码/命令/预期输出。
- **类型一致性**：`GaussDB(index_name, embedding_model, db_url)` 签名在 Task 3 定义、Task 4/6 调用一致；`calc_pq_nseg/_table_ddl/_index_ddl/_vec_literal/query_by_embedding` 内部引用一致；`build_database_url/register_gaussdb_compat/escape_for_alembic_option` 在 Task 1 定义、Task 2/3/6 调用一致；返回形态 `{"documents": [...]}` 与 `{"dimensions", "vector_count"}` 对齐 pinecone.py 既有契约。
