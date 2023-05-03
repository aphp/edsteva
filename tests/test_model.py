import os

import pandas as pd
import pytest

from edsteva import CACHE_DIR
from edsteva.io import SyntheticData
from edsteva.models.rectangle_function import RectangleFunction
from edsteva.models.step_function import StepFunction
from edsteva.probes import NoteProbe, VisitProbe
from edsteva.utils.loss_functions import l1_loss
from edsteva.viz.dashboards import normalized_probe_dashboard, probe_dashboard
from edsteva.viz.plots import (
    estimates_densities_plot,
    normalized_probe_plot,
    probe_plot,
)

pytestmark = pytest.mark.filterwarnings("ignore")

step_data = SyntheticData(seed=41, mode="step").generate()
rect_data = SyntheticData(seed=41, mode="rect").generate()


def test_step_function_visit_occurence():
    data = step_data
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
    )

    # Test Cache saving
    visit_model.save()
    assert os.path.isfile(CACHE_DIR / "edsteva" / "models" / "stepfunction.pickle")
    visit_model = StepFunction()
    visit_model.load()
    visit_model.delete()
    assert not os.path.isfile(CACHE_DIR / "edsteva" / "models" / "stepfunction.pickle")

    # Test target saving
    visit_model.save(
        path="test.pickle",
    )
    assert os.path.isfile("test.pickle")

    visit_model = StepFunction()
    visit_model.load("test.pickle")
    visit_model.delete()
    assert not os.path.isfile("test.pickle")

    simulation = data.visit_occurrence[
        ["care_site_id", "t_0_min", "t_0_max"]
    ].drop_duplicates()
    model = visit_model.estimates[["care_site_id", "t_0"]].drop_duplicates()
    prediction = simulation.merge(model, on="care_site_id")
    prediction["t_0_min"] = prediction["t_0_min"].astype(
        "datetime64[s]"
    ) - pd.DateOffset(months=2)
    prediction["t_0_max"] = prediction["t_0_max"].astype(
        "datetime64[s]"
    ) + pd.DateOffset(months=2)
    true_t0_min = prediction["t_0_min"] <= prediction["t_0"]
    true_t0_max = prediction["t_0_max"] >= prediction["t_0"]
    assert (true_t0_min & true_t0_max).all()


def test_rect_function_visit_occurence():
    data = rect_data
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
    visit_model.fit(
        probe=visit,
        start_date=data.t_min,
        end_date=data.t_max,
    )
    visit_model.save(
        path="test.pickle",
    )
    assert os.path.isfile("test.pickle")

    visit_model = RectangleFunction()
    visit_model.load("test.pickle")
    visit_model.delete()
    assert not os.path.isfile("test.pickle")

    t_cols = ["t_0_min", "t_0_max", "t_1_min", "t_1_max"]
    simulation = data.visit_occurrence[["care_site_id", *t_cols]].drop_duplicates()
    model = visit_model.estimates[["care_site_id", "t_0", "t_1"]].drop_duplicates()
    prediction = simulation.merge(model, on="care_site_id")
    prediction["t_0_min"] = prediction["t_0_min"].astype(
        "datetime64[s]"
    ) - pd.DateOffset(months=2)
    prediction["t_0_max"] = prediction["t_0_max"].astype(
        "datetime64[s]"
    ) + pd.DateOffset(months=2)
    prediction["t_1_min"] = prediction["t_1_min"].astype(
        "datetime64[s]"
    ) - pd.DateOffset(months=2)
    prediction["t_1_max"] = prediction["t_1_max"].astype(
        "datetime64[s]"
    ) + pd.DateOffset(months=2)
    true_t0_min = prediction["t_0_min"] <= prediction["t_0"]
    true_t0_max = prediction["t_0_max"] >= prediction["t_0"]
    true_t1_min = prediction["t_1_min"] <= prediction["t_1"]
    true_t1_max = prediction["t_1_max"] >= prediction["t_1"]
    assert (true_t0_min & true_t0_max & true_t1_min & true_t1_max).all()


def test_step_function_note():
    data = step_data
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
    simulation["t_0_min"] = simulation["t_0"].astype("datetime64[s]") - pd.DateOffset(
        months=2
    )
    simulation["t_0_max"] = simulation["t_0"].astype("datetime64[s]") + pd.DateOffset(
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


@pytest.mark.parametrize(
    "data,Model",
    [
        (step_data, StepFunction),
        (rect_data, RectangleFunction),
    ],
)
def test_viz_visit(data, Model):
    visit = VisitProbe()
    visit.compute(
        data=data,
        start_date=data.t_min,
        end_date=data.t_max,
        stay_types={"ALL": ".*", "HC": "hospitalisés", "Urg": "urgences"},
        care_site_ids=["1", "2"],
        care_site_short_names=["Hôpital-1", "Hôpital-2"],
    )

    visit_model = Model()
    visit_model.fit(
        probe=visit,
        start_date=data.t_min,
        end_date=data.t_max,
    )

    probe_plot(
        probe=visit,
        care_site_level="Hospital",
        start_date=data.t_min,
        end_date=data.t_max,
    )

    probe_plot(
        probe=visit,
        fitted_model=visit_model,
        care_site_level="Hospital",
        start_date=data.t_min,
        end_date=data.t_max,
    )

    normalized_probe_plot(
        probe=visit,
        fitted_model=visit_model,
        care_site_level="Hospital",
        start_date=data.t_min,
        end_date=data.t_max,
        stay_type="ALL",
        care_site_id="1",
        care_site_short_name="Hôpital-1",
        t_min=-24,
        t_max=24,
    )

    estimates_densities_plot(
        probe=visit,
        fitted_model=visit_model,
    )

    probe_dashboard(
        probe=visit,
        fitted_model=visit_model,
        care_site_level="Hospital",
    )

    probe_dashboard(
        probe=visit,
        care_site_level="Hospital",
    )

    normalized_probe_dashboard(
        probe=visit,
        fitted_model=visit_model,
        care_site_level="Hospital",
    )
