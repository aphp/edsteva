import pytest

from edsteva import improve_performances


@pytest.mark.first
def test_perf():
    improve_performances()
