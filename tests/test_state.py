import pytest

from text2sql.state import MemoryStateStore, SQLiteStateStore


def _exercise(store):
    assert store.get("skills", "x") is None
    store.put("skills", "alpha", {"value": 1})
    store.put("skills", "alpine", [1, 2])
    assert store.get("skills", "alpha") == {"value": 1}
    assert list(store.list("skills", "alp")) == ["alpha", "alpine"]
    store.put("skills", "alpha", {"value": 2})
    assert store.get("skills", "alpha") == {"value": 2}
    store.delete("skills", "alpha")
    assert store.get("skills", "alpha", "missing") == "missing"


def test_memory_state_contract():
    _exercise(MemoryStateStore())


def test_sqlite_state_round_trip_and_reopen(tmp_path):
    path = tmp_path / "state.db"
    _exercise(SQLiteStateStore(path))
    first = SQLiteStateStore(path)
    first.put("prompt", "addendum", "Use fiscal years")
    second = SQLiteStateStore(path)
    assert second.get("prompt", "addendum") == "Use fiscal years"


def test_state_rejects_non_json(tmp_path):
    store = SQLiteStateStore(tmp_path / "state.db")
    with pytest.raises(TypeError):
        store.put("x", "bad", object())


def test_sqlite_state_rejects_symlink(tmp_path):
    target = tmp_path / "target.db"
    target.touch()
    link = tmp_path / "state.db"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlinks unavailable")
    with pytest.raises(ValueError, match="symlink"):
        SQLiteStateStore(link)
