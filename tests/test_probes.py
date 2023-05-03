import os
from datetime import datetime

import pytest

from edsteva import CACHE_DIR, improve_performances
from edsteva.io import SyntheticData
from edsteva.probes import BiologyProbe, ConditionProbe, NoteProbe, VisitProbe
from edsteva.utils.checks import MissingColumnError, MissingTableError

pytestmark = pytest.mark.filterwarnings("ignore")


improve_performances()
data_step = SyntheticData(seed=41, mode="step").generate()
data_rect = SyntheticData(seed=41, mode="rect").generate()
data_missing = SyntheticData(seed=41, mode="step").generate()


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
    with pytest.raises(MissingTableError):
        data_missing.delete_table("unknown_table")  # Test typo
        data_missing.delete_table("fact_relationship")
        visit = VisitProbe()
        visit.compute(
            data=data_missing,
        )


params = [
    dict(
        visit_predictor="per_visit_default",
        note_predictor="per_visit_default",
        condition_predictor="per_visit_default",
        biology_predictor="per_visit_default",
        only_impute_per_care_site=True,
        impute_missing_dates=True,
        care_site_levels=["UF", "UC", "UH"],
        care_site_short_names=None,
        care_site_ids=None,
        care_site_specialties=["REA ADULTE", "PSYCHIATRIE"],
        specialties_sets=None,
        stay_durations=[1],
        note_types={"ALL": ".*"},
        stay_types=None,
        diag_types=None,
        condition_types={"ALL": ".*"},
        source_systems=["ORBIS"],
        concepts_sets={
            "entity 1": "A0",
            "entity 2": "A1",
            "entity 3": "A2",
            "entity 4": "A3",
            "entity 5": "A4",
        },
        start_date=None,
        end_date=None,
        test_save=False,
        module="koalas",
    ),
    dict(
        visit_predictor="per_visit_default",
        note_predictor="per_note_default",
        condition_predictor="per_condition_default",
        biology_predictor="per_measurement_default",
        only_impute_per_care_site=False,
        impute_missing_dates=True,
        care_site_levels="Pole",
        care_site_ids="1",
        care_site_short_names="Hôpital-1",
        care_site_specialties="PSYCHIATRIE",
        specialties_sets={"All": ".*"},
        stay_durations=None,
        note_types="CRH",
        stay_types="hospitalisés",
        diag_types="DP",
        condition_types="C",
        source_systems=["ORBIS"],
        concepts_sets={"All": ".*"},
        start_date="2010-01-03",
        end_date=None,
        test_save=False,
        module="pandas",
    ),
    dict(
        visit_predictor="per_visit_default",
        note_predictor="per_visit_default",
        condition_predictor="per_visit_default",
        biology_predictor="per_visit_default",
        only_impute_per_care_site=False,
        impute_missing_dates=False,
        care_site_levels=["Hospital", "UF", "Pole"],
        care_site_ids=["1", "2"],
        care_site_short_names=["Hôpital-1", "Hôpital-2"],
        care_site_specialties=None,
        specialties_sets=None,
        stay_durations=None,
        stay_types={"ALL": ".*", "HC": "hospitalisés", "Urg": "urgences"},
        note_types={"ALL": ".*", "CRH": "CRH", "Urg": "urg"},
        diag_types={"ALL": ".*", "DP/DR": "DP|DR"},
        condition_types={"ALL": ".*", "Cancer": "C"},
        source_systems=["ORBIS"],
        concepts_sets={"entity 1": "A0"},
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
    visit = VisitProbe(completeness_predictor=params["visit_predictor"])
    visit.compute(
        data=data,
        only_impute_per_care_site=params["only_impute_per_care_site"],
        impute_missing_dates=params["impute_missing_dates"],
        start_date=params["start_date"],
        end_date=params["end_date"],
        stay_types=params["stay_types"],
        care_site_ids=params["care_site_ids"],
        care_site_short_names=params["care_site_short_names"],
        care_site_specialties=params["care_site_specialties"],
        specialties_sets=params["specialties_sets"],
        stay_durations=params["stay_durations"],
    )

    if params["test_save"]:
        # Test Cache saving
        visit.save()
        assert os.path.isfile(CACHE_DIR / "edsteva" / "probes" / "visitprobe.pickle")
        visit = VisitProbe()
        visit.load()
        visit.delete()
        assert not os.path.isfile(
            CACHE_DIR / "edsteva" / "probes" / "visitprobe.pickle"
        )

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
    note = NoteProbe(completeness_predictor=params["note_predictor"])
    note.compute(
        data=data,
        only_impute_per_care_site=params["only_impute_per_care_site"],
        impute_missing_dates=params["impute_missing_dates"],
        start_date=params["start_date"],
        end_date=params["end_date"],
        stay_types=params["stay_types"],
        care_site_ids=params["care_site_ids"],
        care_site_short_names=params["care_site_short_names"],
        care_site_specialties=params["care_site_specialties"],
        specialties_sets=params["specialties_sets"],
        stay_durations=params["stay_durations"],
        note_types=params["note_types"],
    )


@pytest.mark.parametrize("data", [data_step, data_rect])
@pytest.mark.parametrize("params", params)
def test_compute_condition_probe(data, params):
    if params["module"] == "koalas":
        data.convert_to_koalas()
    elif params["module"] == "pandas":
        data.reset_to_pandas()
    condition = ConditionProbe(completeness_predictor=params["condition_predictor"])
    condition.compute(
        data=data,
        only_impute_per_care_site=params["only_impute_per_care_site"],
        impute_missing_dates=params["impute_missing_dates"],
        start_date=params["start_date"],
        end_date=params["end_date"],
        stay_types=params["stay_types"],
        care_site_ids=params["care_site_ids"],
        care_site_short_names=params["care_site_short_names"],
        care_site_specialties=params["care_site_specialties"],
        specialties_sets=params["specialties_sets"],
        stay_durations=params["stay_durations"],
        diag_types=params["diag_types"],
        condition_types=params["condition_types"],
        source_systems=params["source_systems"],
    )


@pytest.mark.parametrize("data", [data_step, data_rect])
@pytest.mark.parametrize("params", params)
def test_compute_biology_probe(data, params):
    if params["module"] == "koalas":
        data.convert_to_koalas()
    elif params["module"] == "pandas":
        data.reset_to_pandas()
    biology = BiologyProbe(completeness_predictor=params["biology_predictor"])
    biology.compute(
        data=data,
        only_impute_per_care_site=params["only_impute_per_care_site"],
        impute_missing_dates=params["impute_missing_dates"],
        start_date=params["start_date"],
        end_date=params["end_date"],
        stay_types=params["stay_types"],
        care_site_ids=params["care_site_ids"],
        care_site_short_names=params["care_site_short_names"],
        care_site_specialties=params["care_site_specialties"],
        specialties_sets=params["specialties_sets"],
        stay_durations=params["stay_durations"],
        concepts_sets=params["concepts_sets"],
    )
