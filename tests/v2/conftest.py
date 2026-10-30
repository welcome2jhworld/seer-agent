import pytest

from v2_harness import Harness


@pytest.fixture
def harness(tmp_path):
    def make(worker, **settings):
        return Harness(tmp_path / "v2.sqlite", worker, **settings)
    return make
