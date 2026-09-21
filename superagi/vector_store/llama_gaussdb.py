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

    def add(self, embedding_results) -> List[str]:
        """embedding_results: llama_index NodeWithEmbedding 列表（dataclass：
        字段 node/embedding，id 为 property（node.node_id）），node 取 get_content()。
        返回 ids：llama_index 0.6.35 的 _add_nodes_to_index 会 zip(embedding_results,
        new_ids)，add() 返回 None 会在写入成功后抛 'NoneType' object is not iterable。
        """
        texts, metas, embs, ids = [], [], [], []
        for r in embedding_results:
            node = r.node
            texts.append(node.get_content())
            metas.append(dict(node.metadata or {}))
            embs.append(r.embedding)
            ids.append(r.id)
        return self._store.add_texts(texts, metadatas=metas, embeddings=embs, ids=ids)

    def delete(self, ref_doc_id: str, **delete_kwargs) -> None:
        self._store.delete_embeddings_from_vector_db([ref_doc_id])

    def query(self, query, **kwargs):
        """query: llama_index VectorStoreQuery（query_embedding/similarity_top_k）。

        filters（ExactMatchFilter 列表，.key/.value）转 metadata JSONB
        精确匹配（如 agent_id/resource_id），防止跨 agent 召回。
        返回 VectorStoreQueryResult(nodes, similarities, ids)。
        """
        from llama_index.vector_stores.types import VectorStoreQueryResult
        from llama_index.schema import TextNode

        metadata = None
        if getattr(query, "filters", None):
            metadata = {f.key: f.value for f in query.filters.filters}
        rows = self._store.query_by_embedding(query.query_embedding,
                                              query.similarity_top_k or 5,
                                              metadata=metadata)
        nodes = [TextNode(id_=r[0], text=r[1], metadata=r[2] or {}) for r in rows]
        return VectorStoreQueryResult(
            nodes=nodes,
            similarities=[r[3] for r in rows],
            ids=[r[0] for r in rows],
        )
