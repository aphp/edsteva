import pandas as pd
import pytest
from databricks import koalas as ks

from edsteva.utils import framework


@pytest.fixture()
def example_objects():
    return dict(
        pandas=[
            pd.DataFrame({"col": [1, 2, 3]}),
            pd.Series([4, 5, 6]),
        ],
        koalas=[
            ks.DataFrame({"val": [7, 8, 9]}),
            ks.Series([10, 11, 12]),
        ],
    )


def test_identify_pandas(example_objects):
    for obj in example_objects["pandas"]:
        assert framework.is_pandas(obj) is True
        assert framework.is_koalas(obj) is False
        assert framework.get_framework(obj) is pd


def test_identify_koalas(example_objects):
    for obj in example_objects["koalas"]:
        assert framework.is_pandas(obj) is False
        assert framework.is_koalas(obj) is True
        assert framework.get_framework(obj) is ks


def test_framework_pandas(example_objects):
    for obj in example_objects["pandas"]:
        converted = framework.pandas(obj)
        assert converted is obj

    for obj in example_objects["koalas"]:
        converted = framework.pandas(obj)
        assert framework.is_pandas(converted) is True


def test_framework_koalas(example_objects):
    for obj in example_objects["pandas"]:
        converted = framework.koalas(obj)
        assert framework.is_koalas(converted) is True

    for obj in example_objects["koalas"]:
        converted = framework.koalas(obj)
        assert converted is obj


def test_unconvertible_objects():
    objects = [1, "coucou", {"a": [1, 2]}, [1, 2, 3], 2.5, ks, pd]
    for obj in objects:
        with pytest.raises(ValueError):
            framework.pandas(obj)

    for obj in objects:
        with pytest.raises(ValueError):
            framework.koalas(obj)
