import altair as alt
import pytest

from edsteva import improve_performances
from edsteva.io import SyntheticData
from edsteva.models.rectangle_function import RectangleFunction
from edsteva.models.step_function import StepFunction
from edsteva.probes import BiologyProbe, ConditionProbe, NoteProbe, VisitProbe
from edsteva.probes.visit.viz_configs import viz_configs
from edsteva.viz.dashboards import normalized_probe_dashboard, probe_dashboard
from edsteva.viz.plots import (
    estimates_densities_plot,
    normalized_probe_plot,
    probe_plot,
)

pytestmark = pytest.mark.filterwarnings("ignore")

improve_performances()
data_step = SyntheticData(mean_visit=100, seed=41, mode="step").generate()
data_rect = SyntheticData(mean_visit=100, seed=41, mode="rect").generate()


@pytest.fixture(scope="session")
def tmp_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("Test")


@pytest.mark.parametrize(
    "data,Model",
    [
        (data_step, StepFunction),
    ],
)
@pytest.mark.parametrize(
    "Probe",
    [
        VisitProbe,
    ],
)
def test_viz_fail(data, Model, Probe, tmp_dir):
    probe = Probe()
    probe.compute(
        data=data,
        start_date=data.t_min,
        end_date=data.t_max,
        stay_types={"HC": "hospitalisés", "Urg": "urgences"},
        care_site_ids=["1", "2"],
    )
    model = Model()
    model.fit(
        probe=probe,
        start_date=data.t_min,
        end_date=data.t_max,
    )

    with pytest.raises(TypeError):
        probe_plot(
            probe=probe,
            care_site_level="Hospital",
            start_date=data.t_min,
            end_date=data.t_max,
            x_axis_title="x_axis",
            y_axis_title="y_axis",
            save_path=tmp_dir / "test.html",
            stay_type="fail",
        )
    with pytest.raises(AttributeError):
        probe_plot(
            probe=probe,
            care_site_level="fail",
            start_date=data.t_min,
            end_date=data.t_max,
            x_axis_title="x_axis",
            y_axis_title="y_axis",
            save_path=tmp_dir / "test.html",
        )
    with pytest.raises(Exception):
        model.estimates = model.estimates.iloc[:-1]  # Remove last row
        probe_plot(
            probe=probe,
            fitted_model=model,
            care_site_level="Hospital",
            start_date=data.t_min,
            end_date=data.t_max,
        )
    model.reset_estimates()


def test_custom_config(tmp_dir):
    probe = VisitProbe()
    probe.compute(
        data=data_step,
        start_date=data_step.t_min,
        end_date=data_step.t_max,
        stay_types={"HC": "hospitalisés", "Urg": "urgences"},
        care_site_ids=["1", "2"],
        care_site_short_names=["Hôpital-1", "Hôpital-2"],
        concepts_sets=None,
        note_types=None,
        length_of_stays=None,
    )
    model = StepFunction()
    model.fit(
        probe=probe,
        start_date=data_step.t_min,
        end_date=data_step.t_max,
    )
    model.estimates["error"] = 0
    model_line_config = dict(
        mark_line=dict(
            color="black",
            interpolate="step-after",
            strokeDash=[5, 5],
        ),
        encode=dict(
            y="model:Q",
            strokeWidth=alt.StrokeWidth(
                field="legend_model",
                title="Model line",
                legend=alt.Legend(
                    symbolType="stroke",
                    symbolStrokeColor="steelblue",
                    labelFontSize=12,
                    labelFontStyle="bold",
                    symbolDash=[2, 2],
                    orient="top",
                ),
            ),
        ),
        calculates=[
            dict(c_hat=alt.datum.error + 0.01),
        ],
    )
    normalized_probe_dashboard(
        probe=probe,
        fitted_model=model,
        model_line_config=model_line_config,
        care_site_level="Hospital",
        save_path=tmp_dir / "test.html",
    )
    catalogue_viz_probe_dashboard = viz_configs["probe_dashboard"]
    default_config = probe.get_viz_config("probe_dashboard")
    custom_filters_config = default_config.copy()
    custom_filters_config["main_chart"]["filters"] = [
        dict(filter=alt.datum.date <= "2023")
    ]

    @catalogue_viz_probe_dashboard.register("custom_filters")
    def get_custom_filters(self):
        return custom_filters_config

    probe_dashboard(
        probe=probe,
        care_site_level="Hospital",
        vertical_bar_charts_config={"x": [], "y": []},
        save_path=tmp_dir / "test.html",
    )
    probe_dashboard(
        probe=probe,
        care_site_level="Hospital",
        horizontal_bar_charts_config={"x": [], "y": []},
        save_path=tmp_dir / "test.html",
    )
    probe._viz_config["probe_dashboard"] = "custom_filters"
    probe_dashboard(
        probe=probe,
        care_site_level="Hospital",
        vertical_bar_charts_config={"x": [], "y": []},
        horizontal_bar_charts_config={"x": [], "y": []},
        save_path=tmp_dir / "test.html",
    )


@pytest.mark.parametrize(
    "data,Model",
    [
        (data_step, StepFunction),
        (data_rect, RectangleFunction),
    ],
)
@pytest.mark.parametrize(
    "Probe",
    [
        BiologyProbe,
        ConditionProbe,
        NoteProbe,
        VisitProbe,
    ],
)
def test_viz_probe(data, Model, Probe, tmp_dir):
    probe = Probe()
    for completness_predictor in probe.available_completeness_predictors():
        probe._completness_predictor = completness_predictor
        probe.compute(
            data=data,
            start_date=data.t_min,
            end_date=data.t_max,
            stay_types={"HC": "hospitalisés", "Urg": "urgences"},
            care_site_ids=["1", "2"],
            care_site_short_names=["Hôpital-1", "Hôpital-2"],
            concepts_sets=None,
            note_types=None,
            length_of_stays=None,
        )
        model = Model()
        model.fit(
            probe=probe,
            start_date=data.t_min,
            end_date=data.t_max,
        )
        with pytest.raises(ValueError):
            model.get_viz_config(viz_type="unknown_plot")
        viz_configs = [completness_predictor]
        if type(probe).__name__ == "BiologyProbe":
            viz_configs.append("n_measurement")
        elif type(probe).__name__ == "ConditionProbe":
            viz_configs.append("n_condition")
        elif type(probe).__name__ == "NoteProbe":
            viz_configs.append("n_note")
        elif type(probe).__name__ == "VisitProbe":
            viz_configs.append("n_visit")
        for viz_config in viz_configs:
            probe._viz_config["probe_plot"] = viz_config
            probe_plot(
                probe=probe,
                care_site_level="Hospital",
                start_date=data.t_min,
                end_date=data.t_max,
                x_axis_title="x_axis",
                y_axis_title="y_axis",
                save_path=str(tmp_dir.resolve()) + "/test.html",
            )

            model.reset_estimates()
            probe_plot(
                probe=probe,
                fitted_model=model,
                care_site_level="Hospital",
                start_date=data.t_min,
                end_date=data.t_max,
                save_path=tmp_dir / "test.html",
            )

            probe._viz_config["normalized_probe_plot"] = viz_config
            normalized_probe_plot(
                probe=probe,
                fitted_model=model,
                care_site_level="Hospital",
                start_date=data.t_min,
                end_date=data.t_max,
                stay_type="HC",
                care_site_id="1",
                care_site_short_name="Hôpital-1",
                t_min=-24,
                t_max=24,
                save_path=tmp_dir / "test.html",
            )

            probe._viz_config["estimates_densities_plot"] = viz_config
            estimates_densities_plot(
                probe=probe,
                fitted_model=model,
                save_path=tmp_dir / "test.html",
            )

            probe._viz_config["probe_dashboard"] = viz_config
            probe_dashboard(
                probe=probe,
                fitted_model=model,
                care_site_level="Hospital",
            )

            probe_dashboard(
                probe=probe,
                care_site_level="Hospital",
                save_path=tmp_dir / "test.html",
            )

            probe._viz_config["normalized_probe_dashboard"] = viz_config
            normalized_probe_dashboard(
                probe=probe,
                fitted_model=model,
                care_site_level="Hospital",
                save_path=tmp_dir / "test.html",
            )
