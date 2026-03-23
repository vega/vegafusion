from __future__ import annotations

import pytest

import vegafusion as vf
import vegafusion._vegafusion as _core


def test_runtime_exposes_url_policy_properties() -> None:
    rt = vf.VegaFusionRuntime(
        memory_limit=1,
        worker_threads=1,
        base_url="https://example.com/data/",
        allowed_base_urls=["https://example.com/data/"],
    )

    assert rt.base_url == "https://example.com/data/"
    assert rt.allowed_base_urls == ["https://example.com/data/"]


def test_runtime_passes_url_policy_to_embedded_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    class FakeRuntime:
        def clear_cache(self) -> None:
            return None

    class FakePyVegaFusionRuntime:
        @staticmethod
        def new_embedded(
            cache_capacity: int,
            memory_limit: int,
            worker_threads: int,
            base_url: str | bool | None = None,
            allowed_base_urls: list[str] | None = None,
        ) -> FakeRuntime:
            calls.append(
                {
                    "cache_capacity": cache_capacity,
                    "memory_limit": memory_limit,
                    "worker_threads": worker_threads,
                    "base_url": base_url,
                    "allowed_base_urls": allowed_base_urls,
                }
            )
            return FakeRuntime()

    monkeypatch.setattr(_core, "PyVegaFusionRuntime", FakePyVegaFusionRuntime)

    rt = vf.VegaFusionRuntime(
        cache_capacity=8,
        memory_limit=256,
        worker_threads=2,
        base_url=False,
        allowed_base_urls=["file:///tmp/allowed/"],
    )

    _ = rt.runtime

    assert calls == [
        {
            "cache_capacity": 8,
            "memory_limit": 256,
            "worker_threads": 2,
            "base_url": False,
            "allowed_base_urls": ["file:///tmp/allowed/"],
        }
    ]


def test_grpc_connect_rejects_local_url_policy() -> None:
    rt = vf.VegaFusionRuntime(base_url=False)

    with pytest.raises(ValueError, match="base_url or allowed_base_urls"):
        rt.grpc_connect("http://127.0.0.1:50051")

    rt = vf.VegaFusionRuntime(allowed_base_urls=[])

    with pytest.raises(ValueError, match="base_url or allowed_base_urls"):
        rt.grpc_connect("http://127.0.0.1:50051")


def test_url_policy_setters_reject_changes_while_using_grpc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class FakeRuntime:
        def clear_cache(self) -> None:
            calls.append("clear_cache")

    class FakePyVegaFusionRuntime:
        @staticmethod
        def new_grpc(url: str) -> FakeRuntime:
            calls.append(url)
            return FakeRuntime()

    monkeypatch.setattr(_core, "PyVegaFusionRuntime", FakePyVegaFusionRuntime)

    rt = vf.VegaFusionRuntime()
    rt.grpc_connect("http://127.0.0.1:50051")

    with pytest.raises(ValueError, match="vegafusion-server"):
        rt.base_url = False

    with pytest.raises(ValueError, match="vegafusion-server"):
        rt.allowed_base_urls = []

    assert calls == ["http://127.0.0.1:50051"]
