import os
from datetime import datetime

import pytest

from edsteva import improve_performances
from edsteva.io import SyntheticData
from edsteva.probes import NoteProbe, VisitProbe

pytestmark = pytest.mark.filterwarnings("ignore")

improve_performances()
data_step = SyntheticData(seed=41, mode="step").generate()
data_rect = SyntheticData(seed=41, mode="rect").generate()

params = [
    dict(
        only_impute_per_care_site=True,
        impute_missing_dates=True,
        care_site_levels="UF",
        care_site_short_names=None,
        care_site_ids=None,
        note_types={"ALL": ".*"},
        stay_types=None,
        start_date=None,
        end_date=None,
        test_save=False,
        module="koalas",
    ),
    dict(
        only_impute_per_care_site=False,
        impute_missing_dates=True,
        care_site_levels="Pole",
        care_site_ids="1",
        care_site_short_names="Hôpital-1",
        note_types="CRH",
        stay_types="hospitalisés",
        start_date="2010-01-03",
        end_date=None,
        test_save=False,
        module="pandas",
    ),
    dict(
        only_impute_per_care_site=False,
        impute_missing_dates=False,
        care_site_levels=["Hospital", "UF", "Pole"],
        care_site_ids=["1", "2"],
        care_site_short_names=["Hôpital-1", "Hôpital-2"],
        stay_types={"ALL": ".*", "HC": "hospitalisés", "Urg": "urgences"},
        note_types={"ALL": ".*", "CRH": "CRH", "Urg": "urg"},
        start_date=datetime(2010, 5, 10),
        end_date=datetime(2020, 1, 1),
        test_save=True,
        module="pandas",
    ),
]


@pytest.mark.parametrize("data", [data_step, data_rect])
@pytest.mark.parametrize("params", params)
def test_compute_visit_probe(data, params):
    if params["module"] == "koalas":
        data.convert_to_koalas()
    elif params["module"] == "pandas":
        data.reset_to_pandas()
    visit = VisitProbe()
    visit.compute(
        data=data,
        only_impute_per_care_site=params["only_impute_per_care_site"],
        impute_missing_dates=params["impute_missing_dates"],
        start_date=params["start_date"],
        end_date=params["end_date"],
        stay_types=params["stay_types"],
        care_site_ids=params["care_site_ids"],
        care_site_short_names=params["care_site_short_names"],
    )
    if params["test_save"]:
        visit.save(
            path="test.pickle",
        )
        assert os.path.isfile("test.pickle")

        visit = VisitProbe()
        visit.load("test.pickle")
        predictor = visit.predictor.copy()
        visit.filter_care_site(care_site_ids="1")
        assert visit.predictor.care_site_id.str.startswith("1").all()
        visit.filter_care_site(
            care_site_ids=["1", "2"], care_site_short_names="Hôpital-2"
        )
        visit.reset_predictor()
        assert predictor.equals(visit.predictor)
        visit.delete()
        assert not os.path.isfile("test.pickle")


@pytest.mark.parametrize("data", [data_step, data_rect])
@pytest.mark.parametrize("params", params)
def test_compute_note_probe(data, params):
    if params["module"] == "koalas":
        data.convert_to_koalas()
    elif params["module"] == "pandas":
        data.reset_to_pandas()
    note = NoteProbe()
    note.compute(
        data=data,
        only_impute_per_care_site=params["only_impute_per_care_site"],
        impute_missing_dates=params["impute_missing_dates"],
        start_date=params["start_date"],
        end_date=params["end_date"],
        stay_types=params["stay_types"],
        note_types=params["note_types"],
        care_site_ids=params["care_site_ids"],
        care_site_short_names=params["care_site_short_names"],
    )
