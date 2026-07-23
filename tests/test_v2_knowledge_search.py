"""@brief Knowledge 混合检索 SQL 组装回归 / Knowledge hybrid-search SQL assembly regressions."""

from backend.infrastructure.knowledge_search import _DENSE_SQL, _LEXICAL_SQL


def test_hybrid_search_sql_preserves_postgres_json_path_literals() -> None:
    """@brief Python 模板不得消费 PostgreSQL JSON path / Python formatting must preserve PostgreSQL JSON paths."""

    lexical = _LEXICAL_SQL.format(filters="")
    dense = _DENSE_SQL.format(filters="")

    assert "#>> '{metadata,path}'" in lexical
    assert "#>> '{metadata,path}'" in dense
