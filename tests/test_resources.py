from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from verifiers_ucloud import _resources


@pytest.fixture(autouse=True)
def clear_cache():
    _resources.ensure_file_descriptor_capacity.cache_clear()
    yield
    _resources.ensure_file_descriptor_capacity.cache_clear()


def fake_resource(monkeypatch, soft, hard):
    fake = SimpleNamespace(
        RLIMIT_NOFILE=7,
        RLIM_INFINITY=-1,
        getrlimit=Mock(return_value=(soft, hard)),
        setrlimit=Mock(),
    )
    monkeypatch.setattr(_resources, "resource", fake)
    return fake


@pytest.mark.parametrize("hard", [65536, -1])
def test_raise_soft_limit_once_without_changing_hard_limit(monkeypatch, hard):
    fake = fake_resource(monkeypatch, 1024, hard)
    _resources.ensure_file_descriptor_capacity()
    _resources.ensure_file_descriptor_capacity()
    fake.setrlimit.assert_called_once_with(7, (8192, hard))
    fake.getrlimit.assert_called_once()


@pytest.mark.parametrize("soft", [8192, 65536, -1])
def test_never_lower_existing_limit(monkeypatch, soft):
    fake = fake_resource(monkeypatch, soft, -1)
    _resources.ensure_file_descriptor_capacity()
    fake.setrlimit.assert_not_called()


def test_restricted_hard_limit_warns_once_and_keeps_small_runs_available(
    monkeypatch, caplog
):
    fake = fake_resource(monkeypatch, 1024, 4096)
    _resources.ensure_file_descriptor_capacity()
    _resources.ensure_file_descriptor_capacity()
    fake.setrlimit.assert_called_once_with(7, (4096, 4096))
    assert len(caplog.records) == 1
    assert "hard limit" in caplog.text


def test_failed_raise_is_actionable_and_does_not_break_small_runs(monkeypatch, caplog):
    fake = fake_resource(monkeypatch, 1024, 65536)
    fake.setrlimit.side_effect = OSError("not allowed")
    _resources.ensure_file_descriptor_capacity()
    assert "ulimit -n 8192" in caplog.text


def test_non_unix_platform_does_not_require_resource(monkeypatch):
    monkeypatch.setattr(_resources, "resource", None)
    _resources.ensure_file_descriptor_capacity()
