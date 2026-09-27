"""Smoke test: the package layout is importable."""

import importlib

import pytest


@pytest.mark.parametrize("package", ["grpcq", "bench", "services"])
def test_package_is_importable(package: str) -> None:
    module = importlib.import_module(package)

    assert module.__name__ == package
