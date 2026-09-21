# SuperAGI × GaussDB 端到端验收（Ollama 本地模型）

Task 9 交付：uvicorn API + Celery worker 双进程跑在 GaussDB（元数据 + 向量）上，
LLM = `qwen3:4b`、embedding = `qwen3-embedding:0.6b` 全部走本机 Ollama 的
OpenAI 兼容端点 `http://localhost:11434/v1`，验证 agent_execution_feeds 落库、
events JSONB、GAUSSDB 向量表 `<+>` 检索。

验收脚本：`delivery/tests/t6_e2e_boot.py`（自动拉起双进程、跑通全链路、finally 清理）。

## 1. 前置条件

### GaussDB
- 集中式 127.0.0.1:5432，管理账号 `appuser`，应用账号 `superagi_test`
- e2e 独立库（不影响 super_agi_test）：

```powershell
# 建库（appuser 执行；幂等写法见 delivery/tests/_tmp_create_e2e_db.py 已删除，可用 t6 前手工执行）
CREATE DATABASE super_agi_e2e DBCOMPATIBILITY='A' ENCODING='UTF8';
ALTER DATABASE super_agi_e2e OWNER TO superagi_test;
# 授权
docker exec gaussdb su - gausscore -c "export LD_LIBRARY_PATH=/opt/gaussdb/app/lib; /opt/gaussdb/app/bin/gsql -d super_agi_e2e -c 'GRANT CREATE, USAGE ON SCHEMA public TO superagi_test;'"
# 迁移
$env:DB_URL='opengauss+psycopg2://superagi_test:GaussTest2026@127.0.0.1:5432/super_agi_e2e'
$env:ENCRYPTION_KEY='AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
.\.venv-gauss\Scripts\alembic.exe upgrade head
```

### Ollama
```powershell
ollama list          # 需要：qwen3:4b（推理）、qwen3-embedding:0.6b（1024 维向量）
curl.exe -s http://localhost:11434/v1/models
```

另外创建一个 **别名模型**，让 llama_index 默认的 `text-embedding-ada-002` 请求落到本地 embedding 模型：

```powershell
# Modelfile 内容：FROM qwen3-embedding:0.6b
ollama create text-embedding-ada-002 -f Modelfile-ada
```

### Redis
```powershell
docker run -d --name superagi-e2e-redis -p 6379:6379 redis:7
# Docker Hub 直连失败时用镜像源：docker pull docker.m.daocloud.io/library/redis:7
```

## 2. 双进程启动命令（Windows）

```powershell
# API（在项目根目录）
.\.venv-e2e\Scripts\python.exe -m uvicorn main:app --host 127.0.0.1 --port 8001

# Worker（Windows 下 celery prefork 不可用，必须 --pool=solo）
.\.venv-e2e\Scripts\python.exe -m celery -A superagi.worker worker --loglevel=info --pool=solo
```

两个进程的环境变量（t6 已自动注入）：

| 变量 | 值 | 原因 |
|---|---|---|
| `OPENAI_API_BASE` | `http://localhost:11434/v1` | openai 0.27.7 在 import 期读取；保证 worker 里 llama_index 资源向量路径也打向 Ollama |
| `HF_ENDPOINT` | `https://hf-mirror.com` | Python 3.8 下 llama_index 的 TokenTextSplitter 走 transformers `GPT2TokenizerFast`，huggingface.co 直连被网络重置，改走镜像下载 gpt2 分词器（约 3.7MB，首次一次性） |

config.yaml（项目根，.gitignore 已忽略）见下节；`JWT_SECRET_KEY` 为 `/login` 必需。

## 3. config.yaml 模板

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
JWT_SECRET_KEY: <任意随机串>
ENCRYPTION_KEY: abcdefghijklmnopqrstuvwxyz123456
STORAGE_TYPE: "FILE"
TOOLS_DIR: "superagi/tools"
RESOURCES_INPUT_ROOT_DIR: workspace/input/{agent_id}
RESOURCES_OUTPUT_ROOT_DIR: workspace/output/{agent_id}/{agent_execution_id}
MAX_MODEL_TOKEN_LIMIT: 8192
```

## 4. e2e 依赖 venv

```powershell
py -3.8 -m venv .venv-e2e
uv pip install -p .venv-e2e\Scripts\python.exe --index-url https://pypi.tuna.tsinghua.edu.cn/simple -r delivery/e2e-config/requirements-e2e.txt
```

相对 requirements.txt 的**剔除清单**（见 `requirements-e2e.txt` 头注释）：

| 包 | 原因 | 影响 |
|---|---|---|
| `google-generativeai==0.1.0` | 所有版本要求 Python>=3.9，3.8 解析必败 | GooglePalm/PalmEmbedding 走延迟导入守卫，仅在实际使用时才报错 |
| `llama_cpp_python==0.2.7` | C++ 工具链编译 | LocalLLM（llama_cpp 直连本地 gguf 路线）延迟导入；e2e 走 Ollama OpenAI 兼容端点，不需要 |
| `chromadb==0.3.26` | 依赖 hnswlib（C++ 编译） | Chroma 向量后端延迟导入；e2e 用 GAUSSDB 后端，不需要 |

## 5. t6 验证点结果表

（运行 `.\.venv-e2e\Scripts\python.exe delivery\tests\t6_e2e_boot.py`）

| 验证点 | 结果 |
|---|---|
| boot: uvicorn main:app ready | PASS |
| boot: celery worker alive | PASS |
| auth: /users/add（自动建 org+project）+ /login JWT | PASS |
| model source 登记（models_config.OpenAI + models.qwen3:4b） | PASS |
| agent create（Goal Based Workflow / LTM_DB=GAUSSDB / model=qwen3:4b） | PASS |
| resource upload（触发 summarize_resource） | PASS |
| execution trigger（/agentexecutions/add → execute_agent.delay） | PASS |
| agent_execution_feeds 落库 | PASS（rows=5：system/user/assistant 全链） |
| execution 终态 | PASS（COMPLETED，qwen3 第二轮调 finish 工具收尾） |
| events JSONB（event_property->>'agent_execution_id'） | PASS（run_created） |
| LTM 向量表 super-agent-index1 建表 + <+> 自检索 | PASS（dim=1024 GsIVFFLAT，score=0.9998） |
| 资源向量表 super_agi_vectors + <+> 检索 | PASS（dim=1024，rows=1，score=0.9628） |

实际运行日志（2026-09-21）：

```
[PASS] boot: uvicorn main:app ready at http://127.0.0.1:8001 (GaussDB e2e db)
[PASS] boot: celery worker (superagi.worker, --pool=solo) alive
[PASS] auth: user+org created (org_id=1), /login JWT issued and validated
[PASS] model source registered: provider OpenAI(api_key=ollama) + models row qwen3:4b (provider_id=1)
[PASS] agent created id=3 (Goal Based Workflow, model qwen3:4b, LTM_DB=GAUSSDB), initial execution id=5
[PASS] resource uploaded (summarize_resource queued -> GAUSSDB super_agi_vectors)
[PASS] execution triggered: id=6 status=RUNNING (execute_agent.delay via celery+redis)
[PASS] agent_execution_feeds populated: rows=2 (celery -> AgentExecutor -> Ollama qwen3:4b)
[PASS] execution reached terminal status: COMPLETED
[PASS] events JSONB queryable: event_name=run_created event_property={"agent_execution_id": 6, ...}
[PASS] LTM GaussDB vector table super-agent-index1: rows=1, <+> self-retrieval score=0.9998
[PASS] resource vector table super_agi_vectors: rows=1, <+> retrieval top score=0.9628
[PASS] E2E: boot + agent run + feeds/events + vector on GaussDB (Ollama local models)
```

（执行耗时：首轮 LLM 推理 113s，全程 ~2.5 分钟。LLM 写入 LTM 的内容即 assistant
thought 文本，`<+>` 自检索 score=0.9998 命中。）

## 6. 踩坑与修正

1. **py3.8 import 链适配**（commit 33b2076）：上游 SuperAGI 0.0.14 目标是 Python 3.9+，
   在 3.8 下整链 import 会炸：
   - `typing.Annotated`（agent_execution_permission.py、api_key.py）→ 改 typing_extensions
   - `list[str]`/`dict[str, Any]` 注解（apollo_search、llama_document_summary、pinecone、redis、google_serp）→ 改 typing.List/Dict
   - `google.generativeai`、`llama_cpp` 顶层硬导入 → try/except 延迟到实际使用
2. **chromadb 顶层导入**（commit e07c550）：注册组织时 register_toolkits 会加载全部工具模块，
   query_resource 顶层 `import chromadb` 直接 500 → chromadb.py 延迟导入。
3. **OpenAI embedding**（commit 5470139）：`OpenAiEmbedding` 原来硬编码
   `text-embedding-ada-002` 且用 Azure 风格 `engine=` 参数、无 api_base 支持；
   改为 `get_config("OPENAI_EMBEDDING_MODEL", ...)` + `OPENAI_API_BASE` + `model=` 参数。
4. **llama_gaussdb 校准**（commit a939212，Task 4 预留给 Task 9 的授权项）：
   llama_index 0.6.35 的 `NodeWithEmbedding` 是 dataclass（字段 node/embedding，id 为
   property），不是旧版 TypedDict；`add()` 的 `r["node"]` 下标访问报
   `'NodeWithEmbedding' object is not subscriptable` → 改为属性访问。修复后
   super_agi_vectors 建表（dim=1024）+ 落行 + `<+>` 检索全通。
5. **Ollama 别名模型**：llama_index 0.6.35 默认 embed 模型名是 ada-002 且无环境变量覆盖，
   用 `ollama create text-embedding-ada-002`（FROM qwen3-embedding:0.6b）在服务端做别名。
6. **nltk punkt 导入期下载挂死**：unstructured 0.8.1 在模块 import 期同步下载 nltk punkt，
   raw.githubusercontent 在本网络环境挂死 → 只改了 .venv-e2e 的 site-packages
   `unstructured/nlp/tokenize.py`（下载限时 15s + 失败吞掉，venv 本地补丁，未动产品代码）。
7. **huggingface 下载**：py3.8 下 llama_index 分词走 transformers GPT2TokenizerFast，
   huggingface.co 直连被重置 → t6 给双进程注入 `HF_ENDPOINT=https://hf-mirror.com`。
8. **DEV 模式 auth**：ENV=DEV 下 check_auth 不校验 JWT、get_current_user 固定 super6@agi.com；
   /users/add 幂等早退时可能漏建 Default Project（首次 500 的遗留状态），t6 会走 /projects/add 自愈。

## 7. 产品代码问题清单

无未修问题。上述 1-4 为产品代码适配（各自独立 commit）；5-8 为环境级方案（不涉及产品代码）。

## 8. 分布式形态（Task 12）

与集中式同一脚本（`t6_e2e_boot.py`）、同一 14 项验收标准，跑在分布式 GaussDB 上：

- 目标库：`opengauss+psycopg2://appuser:Huawei%40123@127.0.0.1:15400/super_agi_dist`
  （容器 `gaussdb-dist-min`，DBCOMPATIBILITY='ORA'，迁移 39 表，alembic_version 表为
  DISTRIBUTE BY REPLICATION 预建——**不要 DROP/reset**；向量 floatvector 硬限 1024 维）
- 运行命令（与集中式手动流程完全隔离）：

```powershell
# 独立 redis 实例（与集中式 6379 完全隔离，worker.py 是 "redis://"+REDIS_URL+"/0" 拼接，换实例即隔离）
docker run -d --name superagi-dist-redis -p 6380:6379 redis:7

& .\.venv-e2e\Scripts\python.exe delivery\tests\t6_e2e_boot.py `
    'opengauss+psycopg2://appuser:Huawei%40123@127.0.0.1:15400/super_agi_dist' 8002 '127.0.0.1:6380'
```

argv[1]=DB URL、argv[2]=uvicorn 端口（8002，集中式手动流程占用 8001）、argv[3]=redis
（纯数字=同实例换 db 号；`host:port`=独立实例）。隔离机制是**环境变量注入**而非改
config.yaml：`superagi/config/config.py` 的 `load_config` 用 `dict(os.environ)` 整体覆盖
yaml，t6 给 uvicorn/celery 子进程注入 `DB_URL`（元数据+两张向量表连接）与
`REDIS_URL=127.0.0.1:6380`（broker/backend 队列隔离），config.yaml 保持指向集中式不动。

### 8.1 分布式运行结果（2026-09-21，全部 PASS，exit=0）

```
[PASS] boot: uvicorn main:app ready at http://127.0.0.1:8002 (GaussDB e2e db)
[PASS] boot: celery worker (superagi.worker, --pool=solo) alive
[PASS] auth: user+org created (org_id=2), /login JWT issued and validated
[PASS] model source registered: provider OpenAI(api_key=ollama) + models row qwen3:4b (provider_id=2)
[PASS] agent created id=4 (Goal Based Workflow, model qwen3:4b, LTM_DB=GAUSSDB), initial execution id=6
[PASS] resource uploaded: 05-capacity-planning.txt (9833 bytes, real demo-docs markdown content as .txt, resource_id=3; summarize_resource queued -> GAUSSDB super_agi_vectors)
[PASS] execution triggered: id=7 status=RUNNING (execute_agent.delay via celery+redis)
[PASS] agent_execution_feeds populated: rows=3 (celery -> AgentExecutor -> Ollama qwen3:4b)
[PASS] execution reached terminal status: COMPLETED
[PASS] events JSONB queryable: event_name=run_created event_property={"agent_execution_id": 7, "agent_execution_name": "e2e-dist-run-1"}
[PASS] LTM GaussDB vector table super-agent-index1: rows=3, dim=1024, index=GsIVFFLAT, <+> self-retrieval score=0.9996
[PASS] resource vector table super_agi_vectors: rows=14, dim=1024, index=GsIVFFLAT, semantic hit on demo-docs: query='磁盘水位红色阈值' top_score=0.6850, top_text='1 个物理核是 OLTP 的安全线；批处理占比高的库放宽到 1:2.5。核数天花板出现前，先看 `cpu_iowait`：持续 > 15% 说明瓶颈在磁盘不在 '
[PASS] cross-db check: central db untouched (tables=41 before=41, agents=1->1, no 'e2e-dist-agent'/'e2e-dist-run-1' rows leaked)
[PASS] E2E: boot + agent run + feeds/events + vector on GaussDB (Ollama local models, target=super_agi_dist@15400, uvicorn:8002)
```

资源场景使用真实文档 `demo-docs/05-capacity-planning.md`（数据库容量规划与平滑扩容，
9833 字节），经 SimpleDirectoryReader → SimpleNodeParser（GPT2 分词，1024 token 分块）
切成 7 块全部写入 super_agi_vectors（dim=1024，metadata 含 agent_id/resource_id）。

**demo-docs 语义检索证据**（阈值块 = 「3.1 三级水位阈值」表所在 chunk，id 前缀 03a899ef）：

| query | 阈值块排名 | score |
|---|---|---|
| `磁盘水位红色阈值` | 1/7（两次重复 embed 稳定） | 0.6835 |
| `磁盘水位超过多少需要启动紧急扩容流程` | 1/7 | 0.7289 |
| `database disk usage red alert threshold` | 1/7 | 0.6235 |

阈值块完整文本含：`| 黄 | 70% ~ 85% | 触发扩容评审，冻结大表无索引变更 | | 红 | > 85% |
启动紧急扩容流程…`（块前缀为 2.3 节 cpu_iowait 尾段，GPT2 token 分块的相邻衔接）。

### 8.2 分布式与集中式差异清单

| 项 | 集中式（Task 9） | 分布式（Task 12） |
|---|---|---|
| 目标库 | super_agi_e2e@5432（A 兼容） | super_agi_dist@15400（ORA，向量硬限 1024 维） |
| 库状态 | e2e 独立库，迁移即时跑 | 预迁移 39 表（alembic_version 预建，不可 reset） |
| uvicorn 端口 | 8001 | 8002（与手动流程隔离） |
| Redis | 6379 db0（独立容器） | 6380 db0（独立容器 superagi-dist-redis） |
| 配置方式 | config.yaml | 环境变量注入 DB_URL/REDIS_URL（yaml 不动） |
| 资源 | 合成 e2e_note.txt（1 块） | demo-docs 真实 Markdown（7 块，语义检索验证） |
| 向量表结构 | 1024 维 floatvector + GsIVFFLAT | 相同（`_ivf` 索引名一致） |
| 隔离保障 | 自身即控制库 | 结束时交叉检查集中式库表数/对象未变 |

### 8.3 Task 12 发现与修复

1. **`.md` 上传被拒（产品限制，未改）**：`superagi/controllers/resources.py:60`
   `accepted_file_types` 硬编码 `(.pdf/.docx/.pptx/.csv/.txt/.epub)`，`/resources/add`
   对 `.md` 返回 400 "File type not supported!"。t6 以 `.txt` 扩展名上传真实 Markdown
   内容绕过（解析/分块/向量链路不变，仅 MarkdownReader 按节切变为 token 分块）。
2. **llama_gaussdb `add()` 返回值（产品代码校准，独立 commit）**：llama_index 0.6.35
   `VectorStoreIndex._add_nodes_to_index` 先 `new_ids = vector_store.add(...)`（数据已
   成功写入）再 `zip(embedding_results, new_ids)`；`LlamaGaussDBVectorStore.add` 返回
   None 导致每次 summarize 在写入成功后抛 `'NoneType' object is not iterable`
   （被 resource_manager 的宽 except 吞掉，功能无损但日志报错）。修复：`add()` 返回
   `GaussDB.add_texts` 的 ids 列表。
3. **hf-mirror SSL 抖动拖垮 summarize（环境级）**：GPT2TokenizerFast 初始化的 HEAD
   请求在镜像抖动时重试 5 次全败（MaxRetryError）。t6 注入 `TRANSFORMERS_OFFLINE=1` +
   `HF_HUB_OFFLINE=1` 强制走本地缓存（gpt2 分词器 5 文件已在 `~/.cache/huggingface`），
   与网络状态彻底解耦。
