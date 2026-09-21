"""T4: GaussDB 向量后端全链路（mock 维度 → dim<=1024 走 GsIVFFLAT，>1024 走 GsDiskANN+PQ）。
含 Task 3 质量审查的三项真库验证闭环：
  V1 JSONB dict 化（psycopg2 typecaster）
  V2 upsert 路径（ON DUPLICATE KEY UPDATE）
  V3 科学计数法向量字面量
用法: venv-gauss python t4_vector.py [TARGET_URL] [DIM]
  DIM 默认 1536（集中式 GsDiskANN+PQ 路线）；分布式 floatvector 硬限 1024 维，
  传 1024 时走 GsIVFFLAT 路线。"""
import os
import random
import sys

sys.path.insert(0, r"D:\workplace\code\SuperAGI\SuperAGI-0.0.14")

from sqlalchemy import text

from superagi.vector_store.gaussdb import GaussDB


TARGET_URL = sys.argv[1] if len(sys.argv) > 1 else \
    "opengauss+psycopg2://superagi_test:GaussTest2026@127.0.0.1:5432/super_agi_test"
DIM = int(sys.argv[2]) if len(sys.argv) > 2 else 1536
INDEX_KIND = "GsIVFFLAT" if DIM <= 1024 else "GsDiskANN+PQ"


class MockEmbedding:
    def __init__(self, dim):
        self.dim = dim

    def get_embedding(self, text):
        rng = random.Random(text)
        return [rng.uniform(-1, 1) for _ in range(self.dim)]


TABLE = "t4_gaussdb_vectors"

store = GaussDB(TABLE, MockEmbedding(DIM), db_url=TARGET_URL)

# 先清残留
with store.engine.begin() as conn:
    conn.execute(text(f'DROP TABLE IF EXISTS "{TABLE}"'))

ids = store.add_texts(["alpha doc", "beta doc", "gamma doc"],
                      metadatas=[{"agent_id": 1}, {"agent_id": 1}, {"agent_id": 2}])
assert len(ids) == 3 and all(len(i) == 36 for i in ids)
print(f"[PASS] add_texts -> 3 ids (dim={DIM}, {INDEX_KIND} lazy schema)")

stats = store.get_index_stats()
assert stats["dimensions"] == DIM and stats["vector_count"] == 3, stats
print(f"[PASS] get_index_stats -> {stats}")

# V1: JSONB dict 化（psycopg2 自动 typecaster）
rows = store.query_by_embedding(store.embedding_model.get_embedding("alpha doc"), top_k=3)
assert len(rows) >= 1
assert isinstance(rows[0][2], dict), f"metadata col is {type(rows[0][2])}, not dict — V1 FAIL"
print("[PASS] V1: JSONB metadata column deserialized to dict by psycopg2")

# V2: upsert 路径（同 id 二次写入，ON DUPLICATE KEY UPDATE）
store.add_embeddings_to_vector_db({"vectors": [(ids[0],
    store.embedding_model.get_embedding("alpha doc UPDATED"),
    {"agent_id": 1, "text": "alpha doc UPDATED"})]})
rows2 = store.query_by_embedding(store.embedding_model.get_embedding("alpha doc UPDATED"), top_k=1)
assert rows2[0][0] == ids[0] and rows2[0][1] == "alpha doc UPDATED", rows2[0][:2]
assert store.get_index_stats()["vector_count"] == 3, "upsert should not add a row"
print("[PASS] V2: upsert via ON DUPLICATE KEY UPDATE (update-in-place, no dup row)")

# V3: 科学计数法字面量（极小值向量）
tiny_ids = store.add_texts(["tiny doc"], embeddings=[[1e-320] * DIM])
assert len(tiny_ids) == 1
print("[PASS] V3: scientific-notation vector literal accepted")
store.delete_embeddings_from_vector_db(tiny_ids)

res = store.get_matching_text("alpha doc UPDATED", top_k=2)
docs = res["documents"]
assert len(docs) >= 1 and docs[0].metadata["score"] > 0.99, docs[0].metadata
print(f"[PASS] get_matching_text self-retrieval score={docs[0].metadata['score']:.4f}")

res_f = store.get_matching_text("alpha doc", top_k=3, metadata={"agent_id": 1})
assert all(d.metadata.get("agent_id") == 1 for d in res_f["documents"])
print(f"[PASS] metadata filtered search -> {len(res_f['documents'])} docs")

store.delete_embeddings_from_vector_db([ids[0]])
assert store.get_index_stats()["vector_count"] == 2
print("[PASS] delete_embeddings -> count=2")

# 清理
with store.engine.begin() as conn:
    conn.execute(text(f'DROP TABLE "{TABLE}"'))
print("T4 done")
