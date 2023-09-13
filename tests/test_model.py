from pathlib import Path

import pandas as pd
import pytest

from edsteva import CACHE_DIR, improve_performances
from edsteva.io import SyntheticData
from edsteva.models.rectangle_function import RectangleFunction
from edsteva.models.step_function import StepFunction
from edsteva.probes import BiologyProbe, NoteProbe, VisitProbe
from edsteva.utils.loss_functions import l1_loss

pytestmark = pytest.mark.filterwarnings("ignore")

improve_performances()
data_step = SyntheticData(seed=41, mode="step").generate()
data_rect = SyntheticData(seed=41, mode="rect").generate()


def test_base_model():
    data = data_step
    visit = VisitProbe()
    visit.compute(
        data=data,
        start_date=data.t_min,
        end_date=data.t_max,
        stay_types={"ALL": ".*", "HC": "hospitalisés", "Urg": "urgences"},
        care_site_ids=["1", "2"],
        care_site_short_names=["Hôpital-1", "Hôpital-2"],
    )
    visit_model = StepFunction(algo="quantile")
    with pytest.raises(Exception):
        visit_model.is_computed_estimates()
    with pytest.raises(Exception):
        visit_model.estimates = "fail"
        visit_model.is_computed_estimates()
    with pytest.raises(TypeError):
        visit_model.fit(pd.DataFrame({"test": [1, 2]}))

    visit_model.fit(probe=visit)
    with pytest.raises(Exception):
        visit_model.estimates = visit_model.estimates.iloc[0:0]
        visit_model.is_computed_estimates()

    visit_model.reset_estimates()
    # Test Cache saving
    visit_model.save()
    assert Path.is_file(CACHE_DIR / "edsteva" / "models" / "stepfunction.pickle")
    visit_model = StepFunction()
    visit_model.load()
    visit_model.delete()
    assert not Path.is_file(CACHE_DIR / "edsteva" / "models" / "stepfunction.pickle")

    # Test target saving
    visit_model.save(
        name="Test",
    )
    assert Path.is_file(CACHE_DIR / "edsteva" / "models" / "test.pickle")
    visit_model.delete()
    assert not Path.is_file(CACHE_DIR / "edsteva" / "models" / "test.pickle")
    visit_model.save(
        path="test.pickle",
    )
    assert Path.is_file(Path("test.pickle"))

    visit_model = StepFunction()
    visit_model.load("test.pickle")
    visit_model.delete()
    assert not Path.is_file(Path("test.pickle"))


def test_step_function_visit_occurence():
    data = data_step
    visit = VisitProbe()
    visit.compute(
        data=data,
        start_date=data.t_min,
        end_date=data.t_max,
        stay_types={"ALL": ".*", "HC": "hospitalisés", "Urg": "urgences"},
        care_site_ids=["1", "2"],
        care_site_short_names=["Hôpital-1", "Hôpital-2"],
    )

    visit_model = StepFunction(algo="quantile")
    visit_model.fit(
        probe=visit,
        metric_functions=["error", "error_after_t0"],
        start_date=data.t_min,
        end_date=data.t_max,
    )
    visit_model = StepFunction(algo="loss_minimization")
    visit_model.fit(probe=visit, loss_function=l1_loss)
    visit_model.fit(
        probe=visit,
        start_date=data.t_min,
        end_date=data.t_max,
        metric_functions="error_after_t0",
    )

    simulation = data.visit_occurrence[
        ["care_site_id", "t_0_min", "t_0_max"]
    ].drop_duplicates()
    model = visit_model.estimates[["care_site_id", "t_0"]].drop_duplicates()
    prediction = simulation.merge(model, on="care_site_id")
    prediction["t_0_min"] = pd.to_datetime(
        prediction["t_0_min"], unit="s"
    ) - pd.DateOffset(months=2)
    prediction["t_0_max"] = pd.to_datetime(
        prediction["t_0_max"], unit="s"
    ) + pd.DateOffset(months=2)
    true_t0_min = prediction["t_0_min"] <= prediction["t_0"]
    true_t0_max = prediction["t_0_max"] >= prediction["t_0"]
    assert (true_t0_min & true_t0_max).all()


def test_rect_function_visit_occurence():
    data = data_rect
    visit = VisitProbe()
    visit.compute(
        data=data,
        start_date=data.t_min,
        end_date=data.t_max,
        stay_types={"ALL": ".*", "HC": "hospitalisés", "Urg": "urgences"},
        care_site_ids=["1", "2"],
        care_site_short_names=["Hôpital-1", "Hôpital-2"],
    )

    visit_model = RectangleFunction()
    visit_model._default_metrics = []
    visit_model.fit(
        probe=visit,
        start_date=data.t_min,
        end_date=data.t_max,
    )
    t_cols = ["t_0_min", "t_0_max", "t_1_min", "t_1_max"]
    simulation = data.visit_occurrence[["care_site_id", *t_cols]].drop_duplicates()
    model = visit_model.estimates[["care_site_id", "t_0", "t_1"]].drop_duplicates()
    prediction = simulation.merge(model, on="care_site_id")
    prediction["t_0_min"] = pd.to_datetime(
        prediction["t_0_min"], unit="s"
    ) - pd.DateOffset(months=2)
    prediction["t_0_max"] = pd.to_datetime(
        prediction["t_0_max"], unit="s"
    ) + pd.DateOffset(months=2)
    prediction["t_1_min"] = pd.to_datetime(
        prediction["t_1_min"], unit="s"
    ) - pd.DateOffset(months=2)
    prediction["t_1_max"] = pd.to_datetime(
        prediction["t_1_max"], unit="s"
    ) + pd.DateOffset(months=2)
    true_t0_min = prediction["t_0_min"] <= prediction["t_0"]
    true_t0_max = prediction["t_0_max"] >= prediction["t_0"]
    true_t1_min = prediction["t_1_min"] <= prediction["t_1"]
    true_t1_max = prediction["t_1_max"] >= prediction["t_1"]
    assert (true_t0_min & true_t0_max & true_t1_min & true_t1_max).all()


def test_step_function_note():
    data = data_step
    note = NoteProbe()
    note.compute(
        data=data,
        start_date=data.t_min,
        end_date=data.t_max,
        stay_types={"ALL": ".*", "HC": "hospitalisés", "Urg": "urgences"},
        care_site_ids=["1", "2"],
        care_site_short_names=["Hôpital-1", "Hôpital-2"],
    )

    note_model = StepFunction()
    note_model.fit(
        probe=note,
        start_date=data.t_min,
        end_date=data.t_max,
    )

    simulation = data.note[
        ["care_site_id", "t_0", "note_class_source_value"]
    ].drop_duplicates()

    simulation = simulation.rename(columns={"note_class_source_value": "note_type"})
    simulation["t_0_min"] = pd.to_datetime(simulation["t_0"], unit="s") - pd.DateOffset(
        months=2
    )
    simulation["t_0_max"] = pd.to_datetime(simulation["t_0"], unit="s") + pd.DateOffset(
        months=2
    )
    model = note_model.estimates[["care_site_id", "t_0", "note_type"]].drop_duplicates()
    prediction = model.merge(
        simulation.drop(columns="t_0"), on=["care_site_id", "note_type"]
    )
    assert (
        (prediction["t_0"] <= prediction["t_0_max"])
        & (prediction["t_0_min"] <= prediction["t_0"])
    ).all()


def test_step_function_biology():
    biology = BiologyProbe(completeness_predictor="per_visit_default")
    biology.compute(
        data=data_step,
        start_date=data_step.t_min,
        end_date=data_step.t_max,
        stay_types={"ALL": ".*", "HC": "hospitalisés", "Urg": "urgences"},
        care_site_ids=["1", "2"],
        care_site_short_names=["Hôpital-1", "Hôpital-2"],
        concepts_sets=None,
        concept_codes=True,
        length_of_stays=None,
    )

    biology_model = StepFunction()
    biology_model.fit(
        probe=biology,
        start_date=data_step.t_min,
        end_date=data_step.t_max,
    )

    simulation = data_step.measurement.merge(
        data_step.visit_occurrence, on="visit_occurrence_id"
    )
    simulation["ANABIO_concept_code"] = (
        simulation["measurement_source_concept_id"]
        .str.extract(r"\b[A-Z]\d{4}\b")
        .str[0]
    )

    simulation = simulation.groupby(
        ["ANABIO_concept_code", "care_site_id"], as_index=False
    )[["t_0"]].min()
    simulation.t_0 = pd.to_datetime(simulation.t_0, unit="s")

    biology_model = biology_model.estimates.merge(
        simulation,
        on=["ANABIO_concept_code", "care_site_id"],
        suffixes=("_model", "_simulation"),
    )

    assert (
        (
            biology_model.t_0_model
            <= biology_model.t_0_simulation + pd.DateOffset(months=2)
        )
        & (
            biology_model.t_0_model
            > biology_model.t_0_simulation - pd.DateOffset(months=2)
        )
    ).all()
