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

_TABLE_NAME_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_-]{0,62}$')


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
            raise ValueError(f"Invalid GaussDB vector table (index) name: {index_name!r}")
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
        if not texts:
            return []
        ids = ids or [str(uuid.uuid4()) for _ in texts]
        metadatas = [metadatas[i] if metadatas and i < len(metadatas) else {} for i in range(len(texts))]
        if embeddings is None:
            embeddings = [self.embedding_model.get_embedding(t) for t in texts]
        if len(ids) != len(texts) or (embeddings is not None and len(embeddings) != len(texts)):
            raise ValueError("Number of ids/embeddings must match number of texts.")
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
        if not embedding:
            return []
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
        vectors = embeddings.get("vectors") or []
        if not vectors:
            return
        if "ids" in embeddings and "payloads" in embeddings:
            ids = embeddings["ids"]
            vectors = embeddings["vectors"]
            payloads = embeddings["payloads"]
        else:
            ids = [v[0] for v in vectors]
            payloads = [v[2] for v in vectors]
            vectors = [v[1] for v in vectors]
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
