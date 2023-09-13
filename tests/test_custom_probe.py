import pytest

from edsteva import improve_performances
from edsteva.io import SyntheticData
from edsteva.models.rectangle_function import RectangleFunction
from edsteva.models.step_function import StepFunction
from edsteva.probes.base import BaseProbe
from edsteva.utils.typing import Data
from edsteva.viz.dashboards import normalized_probe_dashboard, probe_dashboard
from edsteva.viz.plots import (
    estimates_densities_plot,
    normalized_probe_plot,
    probe_plot,
)


@pytest.fixture(scope="session")
def tmp_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("Test")


pytestmark = pytest.mark.filterwarnings("ignore")

improve_performances()
data_step = SyntheticData(mean_visit=100, seed=41, mode="step").generate()
data_rect = SyntheticData(mean_visit=100, seed=41, mode="rect").generate()


class CustomProbe(BaseProbe):
    def __init__(
        self,
    ):
        self._index = ["care_site_id"]
        self._metrics = ["c", "n_visit"]
        super().__init__(
            index=self._index,
        )

    def compute_process(
        self,
        data: Data,
        **kwargs,
    ):
        predictor = data.visit_occurrence.copy()
        predictor["date"] = predictor.visit_start_datetime.dt.strftime("%Y-%m")
        return (
            predictor.groupby(["care_site_id", "date"])
            .agg({"visit_occurrence_id": "count"})
            .rename(columns={"visit_occurrence_id": "n_visit"})
            .reset_index()
            .assign(
                c=lambda pp: pp["n_visit"],
            )
        )


def test_base_viz_config(tmp_dir):
    probe = CustomProbe()
    probe.compute(
        data=data_step,
    )
    with pytest.raises(ValueError):
        probe.get_viz_config(viz_type="unknown_plot")
    rect_model = RectangleFunction()
    rect_model.fit(
        probe=probe,
    )
    model = StepFunction()
    model.fit(
        probe=probe,
    )

    normalized_probe_dashboard(
        probe=probe,
        fitted_model=model,
        save_path=tmp_dir / "test.html",
    )
    probe_dashboard(
        probe=probe,
        save_path=tmp_dir / "test.html",
    )
    probe_plot(
        probe=probe,
        care_site_id="1",
        save_path=tmp_dir / "test.html",
    )
    normalized_probe_plot(
        probe=probe,
        fitted_model=model,
        save_path=tmp_dir / "test.html",
    )
    estimates_densities_plot(
        fitted_model=model,
        probe=probe,
        save_path=tmp_dir / "test.html",
    )
    estimates_densities_plot(
        fitted_model=model,
        save_path=tmp_dir / "test.html",
    )
    model.estimates["care_site_level"] = "Hospital"
    estimates_densities_plot(
        fitted_model=model,
        save_path=tmp_dir / "test.html",
    )
