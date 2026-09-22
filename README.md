# database-superagi-gaussdb

[![Status](https://img.shields.io/badge/Status-Incubating-blue)]()
[![Huawei Cloud](https://img.shields.io/badge/Huawei%20Cloud-Samples-red)]()
[![Scenario](https://img.shields.io/badge/Scenario-application%20intelligence-success)](https://3ms.huawei.com/docs/docinfo/1300901382590492672?bookstackId=866760814559879168&gid=3591759&l=zh-cn&documentkind=&attachmentIdx=5)

基于 **SuperAGI 0.0.14** 的 **华为云 GaussDB** 适配示例：将 SuperAGI 的元数据存储与向量存储完整迁移至 GaussDB 507（A/Oracle 兼容模式），支持**集中式**与**分布式**两种部署形态，并使用 GaussDB 内核原生向量能力（`floatvector` + `GsIVFFLAT` / `GsDiskANN`）承载 Agent 的长期记忆与资源检索——无需任何扩展插件。

## Overview

SuperAGI 上游使用 PostgreSQL 作为元数据库，向量存储依赖外置服务（Redis Stack / Pinecone / Qdrant 等）。本仓库完成的适配：

| 模块 | 适配内容 |
|---|---|
| **连接层** | 三处连接构建统一为共享 helper，方言 `opengauss+psycopg2`；A 兼容模式空串→NULL 的全局守卫（Engine 级 `before_cursor_execute`） |
| **元数据存储** | 34 个 Alembic 迁移全量兼容（双向 downgrade 回归验证）；JSONB/JSON 列与全部运行时查询形态实测通过 |
| **向量存储** | 新增 GaussDB 原生向量后端（`floatvector` + GsIVFFLAT/GsDiskANN 自动选型）挂入双工厂；llama_index 0.6.35 适配；向量表建在元数据库同实例，摆脱 Redis Stack 依赖 |
| **本地模型兼容** | Ollama 等 OpenAI 兼容端点的模型名注入 llama_index 白名单；embedding 模型可配置 |
| **前端** | Node 24 兼容修复（echarts SSR、assetPrefix）、production 构建支持、过期凭证自动清理 |

## Getting Started

### 1. 前置条件

- GaussDB 507.0.0+（集中式或分布式），`SHOW enable_vectordb;` 为 `on`
- Python 3.8 ~ 3.11、Redis（Celery 队列必需）、Node.js（前端）
- Ollama 或任一 OpenAI 兼容模型服务（可选，用于本地模型端到端）

GaussDB 侧初始化（管理员执行）：

```sql
CREATE DATABASE super_agi_main DBCOMPATIBILITY = 'A' ENCODING = 'UTF8';
CREATE USER superagi WITH PASSWORD '<your-password>';
ALTER DATABASE super_agi_main OWNER TO superagi;
GRANT CREATE, USAGE ON SCHEMA public TO superagi;   -- GaussDB 507 收紧了 public schema，库 owner 也需要
```

### 2. 配置

复制 `.env.gaussdb.example` 为 `config.yaml` 并填写连接信息。推荐分项配置（密码无需 URL 编码）：

```yaml
DB_HOST: <gaussdb-host>
DB_PORT: 5432
DB_USERNAME: superagi
DB_PASSWORD: <your-password>
DB_NAME: super_agi_main
RESOURCE_VECTOR_STORE: GAUSSDB
LTM_DB: GAUSSDB
REDIS_URL: 127.0.0.1:6379
```

> 方言为 `opengauss+psycopg2`（GaussDB 的 version 字符串无法被 PostgreSQL 方言解析）。若使用完整 `DB_URL`，密码中的特殊字符需 percent-encode。

### 3. 部署

```bash
pip install -r requirements.txt        # 含 opengauss-sqlalchemy==2.4.0
alembic upgrade head                   # 34 个迁移，终态 39 张表
uvicorn main:app --host 0.0.0.0 --port 8001
celery -A superagi.worker worker --pool=solo --beat   # Windows 用 --pool=solo
cd gui && npm install && npm run build && npm start   # 前端 http://localhost:3000
```

### 4. 验证

`delivery/tests/` 提供分层验证脚本（连接 → 迁移终态 → CRUD+空串守卫 → 向量全链路）与扩展扫描（t7），执行方式见 [delivery/测试指南.md](delivery/测试指南.md)。

## Test Results

| 轮次 | 覆盖 | 结果 |
|---|---|---|
| 主链路端到端（Ollama 本地模型） | 实例启动 → Agent 创建执行 → feeds/events 落库 → 向量检索 | 13/13 通过 |
| 集中式 1024 维（GsIVFFLAT） | 全流程复跑 | 14/14 通过 |
| 分布式（`super_agi_dist`，ORA 模式） | 34 迁移全量重放 + 分层测试 + 端到端 | 14/14 通过 |
| 扩展扫描（Settings/Analytics/生命周期/调度/边界） | 7 类用户路径 | 76 通过，5 个上游产品缺陷登记 |
| 向量维度梯度 | 1024 / 1536 / 4096 维（GsDiskANN+PQ） | 全部实测通过 |

## Known Limitations

- **分布式形态向量维度硬限 1024**：超过 1024 维的 embedding 模型（如 ada-002 的 1536 维）建表即报错，请选用 ≤1024 维模型
- **Knowledge 为 Marketplace 知识包载体**：自建知识库无文档导入 UI（上游设计），文档入库走 Agent 的 Resources 上传
- `metadata` 过滤为索引后过滤，带过滤检索的命中数可能少于 `top_k`（GaussDB 向量索引特性）
- 更多运维注意点与上游既有缺陷登记见 [delivery/注意事项.md](delivery/注意事项.md)

## Repository Structure

```
├── superagi/                     # 后端（vector_store/gaussdb.py 为 GaussDB 向量后端）
│   └── helper/db_connection_helper.py   # 连接构建 + A 模式空串守卫
├── migrations/                   # 34 个 Alembic 迁移（GaussDB 兼容）
├── gui/                          # Next.js 前端
├── delivery/                     # 交付物：实施部署文档 / 测试指南 / 注意事项 / 分层测试脚本
│   └── e2e-config/               # Ollama 端到端启动配置与踩坑记录
├── config_template.yaml          # GaussDB 形态配置模板
└── .env.gaussdb.example          # 环境变量示例
```

## Contributing

Please use pull requests and follow the repository review rules.

## License

This project is licensed under the MIT-0 license.

## Maintainers

CODEOWNERS: @plafaithing

## Feedback
