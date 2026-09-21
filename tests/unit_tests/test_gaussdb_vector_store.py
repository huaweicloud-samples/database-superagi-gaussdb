import pytest

from superagi.vector_store.gaussdb import GaussDB, calc_pq_nseg, _table_ddl, _index_ddl, _vec_literal


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


def test_hyphenated_index_name_allowed():
    # 默认 index 名（super-agent-index1 等）含连字符，双引号包裹的标识符合法
    GaussDB("super-agent-index1", FakeEmbedding(), db_url="sqlite://")


def test_vec_literal_special_floats():
    assert _vec_literal([1.0, 0.5]) == "[1.0,0.5]"
    assert _vec_literal([1e-320]).startswith("[")   # 极小值走科学计数法，格式合法即可


def test_add_texts_empty_list_is_noop():
    store = GaussDB("noop_test_table", FakeEmbedding(),
                    db_url="sqlite://")   # 不连真库；空列表短路在 ensure 之前返回
    assert store.add_texts([]) == []
    assert store.add_embeddings_to_vector_db({"vectors": []}) is None
