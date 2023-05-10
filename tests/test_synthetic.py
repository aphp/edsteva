import pandas as pd
import pytest
from databricks import koalas as ks

from edsteva import improve_performances
from edsteva.io import SyntheticData
from edsteva.probes import VisitProbe
from edsteva.utils.checks import MissingColumnError, MissingTableError, check_tables

pytestmark = pytest.mark.filterwarnings("ignore")


improve_performances()
data_missing = SyntheticData(mean_visit=100, seed=41, mode="step").generate()


def test_bad_params():
    with pytest.raises(Exception):
        SyntheticData(mean_visit=100, seed=41, mode="fail").generate()
    with pytest.raises(Exception):
        SyntheticData(mean_visit=100, seed=41, module="fail").generate()


def test_convert():
    data_koalas = SyntheticData(
        mean_visit=100, seed=41, mode="step", module="koalas"
    ).generate()
    for table in data_koalas.available_tables:
        assert isinstance(getattr(data_koalas, table), type(ks.DataFrame()))
    data_koalas.convert_to_koalas()
    data_koalas.reset_to_pandas()
    for table in data_koalas.available_tables:
        assert isinstance(getattr(data_koalas, table), type(pd.DataFrame()))

    data_pandas = SyntheticData(
        mean_visit=100, seed=41, mode="step", module="pandas"
    ).generate()
    for table in data_pandas.available_tables:
        isinstance(getattr(data_pandas, table), type(pd.DataFrame()))
    data_pandas.reset_to_pandas()
    data_pandas.convert_to_koalas()
    for table in data_pandas.available_tables:
        assert isinstance(getattr(data_pandas, table), type(ks.DataFrame()))


def test_missing_checks():
    with pytest.raises(TypeError):
        data_fake = [1, 2, 3]
        visit = VisitProbe()
        visit.compute(
            data=data_fake,
        )
    with pytest.raises(MissingColumnError):
        data_missing.visit_occurrence = data_missing.visit_occurrence.drop(
            columns="visit_occurrence_id"
        )
        visit = VisitProbe()
        visit.compute(
            data=data_missing,
        )
    with pytest.raises(AttributeError):
        check_tables(data=45, required_tables=["fail"])
    with pytest.raises(MissingTableError):
        data_missing.delete_table("unknown_table")  # Test typo
        data_missing.delete_table("fact_relationship")
        visit = VisitProbe()
        visit.compute(
            data=data_missing,
        )
