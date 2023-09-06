from datetime import datetime
from typing import Dict, List, Union

import pandas as pd
import pytest

from edsteva import improve_performances
from edsteva.io import SyntheticData
from edsteva.models.step_function import StepFunction
from edsteva.probes.base import BaseProbe
from edsteva.probes.visit.completeness_predictors import completeness_predictors
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
        completeness_predictor: str = "per_visit_default",
    ):
        self._index = [
            "care_site_id",
            "care_site_level",
            "care_sites_set",
            "care_site_specialty",
            "specialties_set",
            "stay_type",
            "stay_source",
            "length_of_stay",
            "provenance_source",
            "age_range",
            "drg_source",
        ]
        super().__init__(
            completeness_predictor=completeness_predictor,
            index=self._index,
        )

    def compute_process(
        self,
        data: Data,
        care_site_relationship: pd.DataFrame,
        start_date: datetime,
        end_date: datetime,
        care_site_ids: List[int] = None,
        care_site_short_names: List[str] = None,
        care_site_levels: Union[bool, str, List[str]] = True,
        care_sites_sets: Union[str, Dict[str, str]] = None,
        care_site_specialties: Union[bool, List[str]] = None,
        specialties_sets: Union[str, Dict[str, str]] = None,
        stay_types: Union[bool, str, Dict[str, str]] = True,
        stay_sources: Union[bool, str, Dict[str, str]] = None,
        length_of_stays: List[float] = None,
        provenance_sources: Union[bool, str, Dict[str, str]] = None,
        age_ranges: List[int] = None,
        condition_types: Union[str, Dict[str, str]] = None,
        drg_sources: Union[str, Dict[str, str]] = {"All": ".*"},
        **kwargs,
    ):
        if not care_site_levels and "care_site_level" in self._index:
            self._index.remove("care_site_level")
        if not care_sites_sets and "care_sites_set" in self._index:
            self._index.remove("care_sites_set")
        if not care_site_specialties and "care_site_specialty" in self._index:
            self._index.remove("care_site_specialty")
        if not specialties_sets and "specialties_set" in self._index:
            self._index.remove("specialties_set")
        if not stay_types and "stay_type" in self._index:
            self._index.remove("stay_type")
        if not stay_sources and "stay_source" in self._index:
            self._index.remove("stay_source")
        if not length_of_stays and "length_of_stay" in self._index:
            self._index.remove("length_of_stay")
        if condition_types is None and "condition_type" in self._index:
            self._index.remove("condition_type")
        if not provenance_sources and "provenance_source" in self._index:
            self._index.remove("provenance_source")
        if not age_ranges and "age_range" in self._index:
            self._index.remove("age_range")
        return completeness_predictors.get(self._completeness_predictor)(
            self,
            data=data,
            care_site_relationship=care_site_relationship,
            start_date=start_date,
            end_date=end_date,
            care_site_levels=care_site_levels,
            stay_types=stay_types,
            care_site_ids=care_site_ids,
            care_site_short_names=care_site_short_names,
            care_site_specialties=care_site_specialties,
            care_sites_sets=care_sites_sets,
            specialties_sets=specialties_sets,
            length_of_stays=length_of_stays,
            provenance_sources=provenance_sources,
            stay_sources=stay_sources,
            drg_sources=drg_sources,
            condition_types=condition_types,
            age_ranges=age_ranges,
            **kwargs,
        )

    def available_completeness_predictors(self):
        return list(completeness_predictors.get_all().keys())


def test_base_viz_config(tmp_dir):
    probe = CustomProbe()
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
    with pytest.raises(ValueError):
        probe.get_viz_config(viz_type="unknown_plot")
    model = StepFunction()
    model.fit(
        probe=probe,
        start_date=data_step.t_min,
        end_date=data_step.t_max,
    )

    normalized_probe_dashboard(
        probe=probe,
        fitted_model=model,
        care_site_level="Hospital",
        save_path=tmp_dir / "test.html",
    )
    probe_dashboard(
        probe=probe,
        care_site_level="Hospital",
        save_path=tmp_dir / "test.html",
    )
    probe_plot(
        probe=probe,
        care_site_level="Hospital",
        stay_type="HC",
        care_site_id="1",
        save_path=tmp_dir / "test.html",
    )
    normalized_probe_plot(
        probe=probe,
        fitted_model=model,
        care_site_level="Hospital",
        save_path=tmp_dir / "test.html",
    )
    estimates_densities_plot(
        fitted_model=model,
        save_path=tmp_dir / "test.html",
    )
    estimates_densities_plot(
        fitted_model=model,
        probe=probe,
        save_path=tmp_dir / "test.html",
    )
