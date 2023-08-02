from datetime import datetime
from pathlib import Path

import pytest

from edsteva import CACHE_DIR, improve_performances
from edsteva.io import SyntheticData
from edsteva.probes import BiologyProbe, ConditionProbe, NoteProbe, VisitProbe
from edsteva.probes.utils.filter_df import (
    filter_table_by_care_site,
    filter_valid_observations,
)
from edsteva.probes.utils.utils import CARE_SITE_LEVEL_NAMES
from edsteva.utils.framework import is_koalas

pytestmark = pytest.mark.filterwarnings("ignore")


improve_performances()
data_step = SyntheticData(mean_visit=100, seed=41, mode="step").generate()
data_rect = SyntheticData(mean_visit=100, seed=41, mode="rect").generate()

params = [
    dict(
        visit_predictor="per_visit_default",
        note_predictor="per_visit_default",
        condition_predictor="per_visit_default",
        biology_predictor="per_visit_default",
        care_site_levels=["Pole", "UF", "UC", "UH"],
        care_site_short_names=None,
        care_site_ids=None,
        care_site_specialties="PSYCHIATRIE",
        specialties_sets=None,
        care_sites_sets={"All": ".*"},
        length_of_stays=[1, 30],
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
        concept_codes=None,
        start_date=None,
        end_date=datetime(2020, 1, 1),
        test_save=False,
        module="koalas",
        stay_source={"MCO": "MCO", "MCO_PSY_SSR": "MCO|Psychiatrie|SSR"},
        provenance_source={"All": ".*"},
        age_list=[18],
    ),
    dict(
        visit_predictor="per_visit_default",
        note_predictor="per_note_default",
        condition_predictor="per_condition_default",
        biology_predictor="per_measurement_default",
        care_site_levels=["Hospital", "Pole", "UF"],
        care_site_ids="1",
        care_site_short_names="Hôpital-1",
        care_site_specialties=None,
        specialties_sets={"All": ".*"},
        care_sites_sets=None,
        concept_codes=["A0009", "A0209", "A3109"],
        length_of_stays=[1],
        note_types="CRH",
        stay_types="hospitalisés",
        diag_types="DP",
        condition_types="C",
        source_systems=["ORBIS"],
        concepts_sets={"All": ".*"},
        start_date="2010-01-03",
        end_date=None,
        test_save=False,
        stay_source={"MCO": "MCO"},
        provenance_source={"All": ".*"},
        age_list=[18],
        module="koalas",
    ),
    dict(
        visit_predictor="per_visit_default",
        note_predictor="per_visit_default",
        condition_predictor="per_visit_default",
        biology_predictor="per_visit_default",
        care_site_levels="Hôpital",
        care_site_ids=["1", "2"],
        care_site_short_names=["Hôpital-1", "Hôpital-2"],
        care_site_specialties=["REA ADULTE", "PSYCHIATRIE"],
        specialties_sets=None,
        care_sites_sets=None,
        length_of_stays=None,
        stay_types={"ALL": ".*", "HC": "hospitalisés", "Urg": "urgences"},
        note_types={"ALL": ".*", "CRH": "CRH", "Urg": "urg"},
        diag_types={"ALL": ".*", "DP/DR": "DP|DR"},
        condition_types={"ALL": ".*", "Cancer": "C"},
        source_systems=["ORBIS"],
        concepts_sets=None,
        concept_codes=["A0009", "A0209", "A3109"],
        start_date=datetime(2010, 5, 10),
        end_date=datetime(2020, 1, 1),
        test_save=True,
        stay_source=None,
        provenance_source={"All": ".*", "urgence": "service d'urgence"},
        age_list=None,
        module="pandas",
    ),
]


@pytest.mark.parametrize("data", [data_step])
def test_base_probe(data):
    visit = VisitProbe()
    with pytest.raises(Exception):
        filter_valid_observations(
            table=data.visit_occurrence, table_name="visit_occurrence"
        )
    with pytest.raises(AttributeError):
        visit.compute(data=data, care_site_levels=["fail"])
    with pytest.raises(TypeError):
        visit.compute(data=data, stay_types=45)
    with pytest.raises(Exception):
        visit.is_computed_probe()
    with pytest.raises(TypeError):
        visit.predictor = "fail"
        visit.is_computed_probe()
    visit.compute(data=data)
    with pytest.raises(Exception):
        visit.predictor = visit.predictor.iloc[0:0]
        visit.is_computed_probe()
    with pytest.raises(TypeError):
        visit.reset_predictor()
        visit.predictor.date = visit.predictor.date.astype(str)
        visit.predictor.date.iloc[0] = "not a date"
        visit.is_computed_probe()

    # Test cache saving
    visit.reset_predictor()
    visit.save()
    assert Path.is_file(CACHE_DIR / "edsteva" / "probes" / "visitprobe.pickle")
    visit = VisitProbe()
    with pytest.raises(FileNotFoundError):
        visit.load("fail.pkl")
    visit.load()
    visit.delete()
    visit.delete("fail.pkl")
    assert not Path.is_file(CACHE_DIR / "edsteva" / "probes" / "visitprobe.pickle")

    # Test target saving
    visit.save(
        name="TEst",
    )
    assert Path.is_file(CACHE_DIR / "edsteva" / "probes" / "test.pickle")
    visit.delete()
    assert not Path.is_file(CACHE_DIR / "edsteva" / "probes" / "test.pickle")
    visit.save(
        path="test.pickle",
    )
    assert Path.is_file(Path("test.pickle"))

    visit = VisitProbe()
    visit.load("test.pickle")
    predictor = visit.predictor.copy()
    visit.filter_care_site(care_site_ids="1")
    visit.filter_date_per_care_site(target_column="n_visit")
    assert visit.predictor.care_site_id.str.startswith("1").all()
    visit.filter_care_site(care_site_ids=["1", "2"], care_site_short_names="Hôpital-2")
    visit.reset_predictor()
    assert predictor.equals(visit.predictor)
    visit.delete()
    assert not Path.is_file(Path("test.pickle"))


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
        care_site_levels=params["care_site_levels"],
        start_date=params["start_date"],
        end_date=params["end_date"],
        stay_types=params["stay_types"],
        care_site_ids=params["care_site_ids"],
        care_site_short_names=params["care_site_short_names"],
        care_site_specialties=params["care_site_specialties"],
        care_sites_sets=params["care_sites_sets"],
        specialties_sets=params["specialties_sets"],
        length_of_stays=params["length_of_stays"],
        stay_source=params["stay_source"],
        provenance_source=params["provenance_source"],
        age_list=params["age_list"],
    )

    # Care site levels
    if params["care_site_levels"]:
        care_site_levels = []
        if isinstance(params["care_site_levels"], str):
            if params["care_site_levels"] in CARE_SITE_LEVEL_NAMES.keys():
                care_site_levels.append(
                    CARE_SITE_LEVEL_NAMES[params["care_site_levels"]]
                )
            elif params["care_site_levels"] in CARE_SITE_LEVEL_NAMES.values():
                care_site_levels.append(params["care_site_levels"])
        if isinstance(params["care_site_levels"], list):
            for care_site_level in params["care_site_levels"]:
                if care_site_level in CARE_SITE_LEVEL_NAMES.keys():
                    care_site_levels.append(CARE_SITE_LEVEL_NAMES[care_site_level])
                elif care_site_level in CARE_SITE_LEVEL_NAMES.values():
                    care_site_levels.append(care_site_level)
        assert set(visit.predictor.care_site_level.unique()).issubset(
            set(care_site_levels)
        )

    # Date
    if params["start_date"]:
        assert (visit.predictor.date >= params["start_date"]).all()
    if params["end_date"]:
        assert (visit.predictor.date < params["end_date"]).all()

    # Stay type
    if params["stay_types"]:
        if isinstance(params["stay_types"], dict):
            assert set(visit.predictor.stay_type.unique()).issubset(
                set(params["stay_types"].keys())
            )
        elif isinstance(params["stay_types"], str):
            assert set(visit.predictor.stay_type.unique()).issubset(
                set([params["stay_types"]])
            )
        elif isinstance(params["stay_types"], list):
            assert set(visit.predictor.stay_type.unique()).issubset(
                set(params["stay_types"])
            )

    # Care sites set
    if params["care_sites_sets"]:
        if isinstance(params["care_sites_sets"], dict):
            assert set(visit.predictor.care_sites_set.unique()).issubset(
                set(params["care_sites_sets"].keys())
            )
        elif isinstance(params["care_sites_sets"], str):
            assert set(visit.predictor.care_sites_set.unique()).issubset(
                set([params["care_sites_sets"]])
            )
        elif isinstance(params["care_sites_sets"], list):
            assert set(visit.predictor.care_sites_set.unique()).issubset(
                set(params["care_sites_sets"])
            )

    # Care site id
    if params["care_site_ids"] and not params["care_sites_sets"]:
        if isinstance(params["care_site_ids"], str):
            assert visit.predictor.care_site_id.str.startswith(
                params["care_site_ids"]
            ).all()
        elif isinstance(params["care_site_ids"], list):
            assert visit.predictor.care_site_id.str.startswith(
                tuple(params["care_site_ids"])
            ).all()

    # Care site name
    if params["care_site_short_names"]:
        if isinstance(params["care_site_short_names"], str):
            assert visit.predictor.care_site_id.str.startswith(
                params["care_site_short_names"].split("-")[-1]
            ).all()
        elif isinstance(params["care_site_short_names"], list):
            assert visit.predictor.care_site_id.str.startswith(
                tuple(map(lambda x: x.split("-")[-1], params["care_site_short_names"]))
            ).all()

    # Specialty sets
    if params["specialties_sets"]:
        if isinstance(params["specialties_sets"], dict):
            assert set(visit.predictor.specialties_set.unique()).issubset(
                set(params["specialties_sets"].keys())
            )
        elif isinstance(params["specialties_sets"], str):
            assert set(visit.predictor.specialties_set.unique()).issubset(
                set([params["specialties_sets"]])
            )
        elif isinstance(params["specialties_sets"], list):
            assert set(visit.predictor.specialties_set.unique()).issubset(
                set(params["specialties_sets"])
            )

    # Care site specialty
    if params["care_site_specialties"]:
        care_site_filters = filter_table_by_care_site(
            table_to_filter=data.care_site,
            care_site_relationship=visit.care_site_relationship,
            care_site_specialties=params["care_site_specialties"],
        ).care_site_id.unique()
        if is_koalas(data.care_site):
            care_site_filters = care_site_filters.to_list()
        assert visit.predictor.care_site_id.isin(care_site_filters).all()

    # Stay durations
    if params["length_of_stays"] and isinstance(params["length_of_stays"], list):
        min_duration = params["length_of_stays"][0]
        max_duration = params["length_of_stays"][-1]
        specialties_sets = [
            "Incomplete stay",
            "<= {} days".format(min_duration),
            ">= {} days".format(max_duration),
        ]
        n_duration = len(params["length_of_stays"])
        for i in range(0, n_duration - 1):
            min = params["length_of_stays"][i]
            max = params["length_of_stays"][i + 1]
            specialties_sets.append("{} days - {} days".format(min, max))
        assert set(visit.predictor.length_of_stay.unique()).issubset(
            set(specialties_sets)
        )

    # Viz config
    assert isinstance(visit.get_viz_config(viz_type="normalized_probe_plot"), dict)
    with pytest.raises(Exception):
        visit.get_viz_config(viz_type="unknown_plot")


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
        care_site_levels=params["care_site_levels"],
        start_date=params["start_date"],
        end_date=params["end_date"],
        stay_types=params["stay_types"],
        care_site_ids=params["care_site_ids"],
        care_site_short_names=params["care_site_short_names"],
        care_site_specialties=params["care_site_specialties"],
        care_sites_sets=params["care_sites_sets"],
        specialties_sets=params["specialties_sets"],
        length_of_stays=params["length_of_stays"],
        note_types=params["note_types"],
        stay_source=params["stay_source"],
        provenance_source=params["provenance_source"],
        age_list=params["age_list"],
    )

    # Care site levels
    if params["care_site_levels"]:
        assert set(note.predictor.care_site_level.unique()) == set(["Hôpital"])

    # Date
    if params["start_date"]:
        assert (note.predictor.date >= params["start_date"]).all()
    if params["end_date"]:
        assert (note.predictor.date < params["end_date"]).all()

    # Stay type
    if params["stay_types"]:
        if isinstance(params["stay_types"], dict):
            assert set(note.predictor.stay_type.unique()).issubset(
                set(params["stay_types"].keys())
            )
        elif isinstance(params["stay_types"], str):
            assert set(note.predictor.stay_type.unique()).issubset(
                set([params["stay_types"]])
            )
        elif isinstance(params["stay_types"], list):
            assert set(note.predictor.stay_type.unique()).issubset(
                set(params["stay_types"])
            )

    # Care sites set
    if params["care_sites_sets"]:
        if isinstance(params["care_sites_sets"], dict):
            assert set(note.predictor.care_sites_set.unique()).issubset(
                set(params["care_sites_sets"].keys())
            )
        elif isinstance(params["care_sites_sets"], str):
            assert set(note.predictor.care_sites_set.unique()).issubset(
                set([params["care_sites_sets"]])
            )
        elif isinstance(params["care_sites_sets"], list):
            assert set(note.predictor.care_sites_set.unique()).issubset(
                set(params["care_sites_sets"])
            )

    # Care site id
    if params["care_site_ids"]:
        if isinstance(params["care_site_ids"], str):
            assert note.predictor.care_site_id.str.startswith(
                params["care_site_ids"]
            ).all()
        elif isinstance(params["care_site_ids"], list):
            assert note.predictor.care_site_id.str.startswith(
                tuple(params["care_site_ids"])
            ).all()

    # Care site name
    if params["care_site_short_names"]:
        if isinstance(params["care_site_short_names"], str):
            assert note.predictor.care_site_id.str.startswith(
                params["care_site_short_names"].split("-")[-1]
            ).all()
        elif isinstance(params["care_site_short_names"], list):
            assert note.predictor.care_site_id.str.startswith(
                tuple(map(lambda x: x.split("-")[-1], params["care_site_short_names"]))
            ).all()

    # Specialty sets
    if params["specialties_sets"]:
        if isinstance(params["specialties_sets"], dict):
            assert set(note.predictor.specialties_set.unique()).issubset(
                set(params["specialties_sets"].keys())
            )
        elif isinstance(params["specialties_sets"], str):
            assert set(note.predictor.specialties_set.unique()).issubset(
                set([params["specialties_sets"]])
            )
        elif isinstance(params["specialties_sets"], list):
            assert set(note.predictor.specialties_set.unique()).issubset(
                set(params["specialties_sets"])
            )

    # Care site specialty
    if params["care_site_specialties"]:
        care_site_filters = filter_table_by_care_site(
            table_to_filter=data.care_site,
            care_site_relationship=note.care_site_relationship,
            care_site_specialties=params["care_site_specialties"],
        ).care_site_id.unique()
        if is_koalas(data.care_site):
            care_site_filters = care_site_filters.to_list()
        assert note.predictor.care_site_id.isin(care_site_filters).all()

    # Stay durations
    if params["length_of_stays"] and isinstance(params["length_of_stays"], list):
        min_duration = params["length_of_stays"][0]
        max_duration = params["length_of_stays"][-1]
        specialties_sets = [
            "Incomplete stay",
            "<= {} days".format(min_duration),
            ">= {} days".format(max_duration),
        ]
        n_duration = len(params["length_of_stays"])
        for i in range(0, n_duration - 1):
            min = params["length_of_stays"][i]
            max = params["length_of_stays"][i + 1]
            specialties_sets.append("{} days - {} days".format(min, max))
        assert set(note.predictor.length_of_stay.unique()).issubset(
            set(specialties_sets)
        )

    # Note type
    if params["note_types"]:
        if isinstance(params["note_types"], dict):
            assert set(note.predictor.note_type.unique()).issubset(
                set(params["note_types"].keys())
            )
        elif isinstance(params["note_types"], str):
            assert set(note.predictor.note_type.unique()).issubset(
                set([params["note_types"]])
            )
        elif isinstance(params["note_types"], list):
            assert set(note.predictor.note_type.unique()).issubset(
                set(params["note_types"])
            )

    # Viz config
    assert isinstance(note.get_viz_config(viz_type="normalized_probe_plot"), dict)
    with pytest.raises(Exception):
        note.get_viz_config(viz_type="unknown_plot")


@pytest.mark.parametrize("data", [data_step])
def test_condition(data):
    condition = ConditionProbe()
    with pytest.raises(AttributeError):
        condition.compute(data=data, source_systems=["AREN"])
    with pytest.raises(AttributeError):
        condition.compute(data=data, source_systems=[])
    with pytest.raises(AttributeError):
        condition.compute(data=data, source_systems=456)


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
        care_site_levels=params["care_site_levels"],
        start_date=params["start_date"],
        end_date=params["end_date"],
        stay_types=params["stay_types"],
        care_site_ids=params["care_site_ids"],
        care_site_short_names=params["care_site_short_names"],
        care_site_specialties=params["care_site_specialties"],
        care_sites_sets=params["care_sites_sets"],
        specialties_sets=params["specialties_sets"],
        length_of_stays=params["length_of_stays"],
        diag_types=params["diag_types"],
        condition_types=params["condition_types"],
        source_systems=params["source_systems"],
        stay_source=params["stay_source"],
        provenance_source=params["provenance_source"],
        age_list=params["age_list"],
    )

    # Care site levels
    if params["care_site_levels"]:
        care_site_levels = []
        care_site_level_names_condition = CARE_SITE_LEVEL_NAMES.copy()
        care_site_level_names_condition.pop("UH", None)
        care_site_level_names_condition.pop("UC", None)
        if isinstance(params["care_site_levels"], str):
            if params["care_site_levels"] in care_site_level_names_condition.keys():
                care_site_levels.append(
                    care_site_level_names_condition[params["care_site_levels"]]
                )
            elif params["care_site_levels"] in care_site_level_names_condition.values():
                care_site_levels.append(params["care_site_levels"])
        if isinstance(params["care_site_levels"], list):
            for care_site_level in params["care_site_levels"]:
                if care_site_level in care_site_level_names_condition.keys():
                    care_site_levels.append(
                        care_site_level_names_condition[care_site_level]
                    )
                elif care_site_level in care_site_level_names_condition.values():
                    care_site_levels.append(care_site_level)
        assert set(condition.predictor.care_site_level.unique()).issubset(
            set(care_site_levels)
        )

    # Date
    if params["start_date"]:
        assert (condition.predictor.date >= params["start_date"]).all()
    if params["end_date"]:
        assert (condition.predictor.date < params["end_date"]).all()

    # Stay type
    if params["stay_types"]:
        if isinstance(params["stay_types"], dict):
            assert set(condition.predictor.stay_type.unique()).issubset(
                set(params["stay_types"].keys())
            )
        elif isinstance(params["stay_types"], str):
            assert set(condition.predictor.stay_type.unique()).issubset(
                set([params["stay_types"]])
            )
        elif isinstance(params["stay_types"], list):
            assert set(condition.predictor.stay_type.unique()).issubset(
                set(params["stay_types"])
            )

    # Care sites set
    if params["care_sites_sets"]:
        if isinstance(params["care_sites_sets"], dict):
            assert set(condition.predictor.care_sites_set.unique()).issubset(
                set(params["care_sites_sets"].keys())
            )
        elif isinstance(params["care_sites_sets"], str):
            assert set(condition.predictor.care_sites_set.unique()).issubset(
                set([params["care_sites_sets"]])
            )
        elif isinstance(params["care_sites_sets"], list):
            assert set(condition.predictor.care_sites_set.unique()).issubset(
                set(params["care_sites_sets"])
            )

    # Care site id
    if params["care_site_ids"]:
        if isinstance(params["care_site_ids"], str):
            assert condition.predictor.care_site_id.str.startswith(
                params["care_site_ids"]
            ).all()
        elif isinstance(params["care_site_ids"], list):
            assert condition.predictor.care_site_id.str.startswith(
                tuple(params["care_site_ids"])
            ).all()

    # Care site name
    if params["care_site_short_names"]:
        if isinstance(params["care_site_short_names"], str):
            assert condition.predictor.care_site_id.str.startswith(
                params["care_site_short_names"].split("-")[-1]
            ).all()
        elif isinstance(params["care_site_short_names"], list):
            assert condition.predictor.care_site_id.str.startswith(
                tuple(map(lambda x: x.split("-")[-1], params["care_site_short_names"]))
            ).all()

    # Specialty sets
    if params["specialties_sets"]:
        if isinstance(params["specialties_sets"], dict):
            assert set(condition.predictor.specialties_set.unique()).issubset(
                set(params["specialties_sets"].keys())
            )
        elif isinstance(params["specialties_sets"], str):
            assert set(condition.predictor.specialties_set.unique()).issubset(
                set()[params["specialties_sets"]]
            )
        elif isinstance(params["specialties_sets"], list):
            assert set(condition.predictor.specialties_set.unique()).issubset(
                set(params["specialties_sets"])
            )

    # Care site specialty
    if params["care_site_specialties"]:
        care_site_filters = filter_table_by_care_site(
            table_to_filter=data.care_site,
            care_site_relationship=condition.care_site_relationship,
            care_site_specialties=params["care_site_specialties"],
        ).care_site_id.unique()
        if is_koalas(data.care_site):
            care_site_filters = care_site_filters.to_list()
        assert condition.predictor.care_site_id.isin(care_site_filters).all()

    # Stay durations
    if params["length_of_stays"] and isinstance(params["length_of_stays"], list):
        min_duration = params["length_of_stays"][0]
        max_duration = params["length_of_stays"][-1]
        length_of_stays = [
            "Incomplete stay",
            "<= {} days".format(min_duration),
            ">= {} days".format(max_duration),
        ]
        n_duration = len(params["length_of_stays"])
        for i in range(0, n_duration - 1):
            min = params["length_of_stays"][i]
            max = params["length_of_stays"][i + 1]
            length_of_stays.append("{} days - {} days".format(min, max))
        assert set(condition.predictor.length_of_stay.unique()).issubset(
            set(length_of_stays)
        )

    # Diag type
    if params["diag_types"]:
        if isinstance(params["diag_types"], dict):
            assert set(condition.predictor.diag_type.unique()).issubset(
                set(params["diag_types"].keys())
            )
        elif isinstance(params["diag_types"], str):
            assert set(condition.predictor.diag_type.unique()).issubset(
                set([params["diag_types"]])
            )
        elif isinstance(params["diag_types"], list):
            assert set(condition.predictor.diag_type.unique()).issubset(
                set(params["diag_types"])
            )

    # Condition type
    if params["condition_types"]:
        if isinstance(params["condition_types"], dict):
            assert set(condition.predictor.condition_type.unique()).issubset(
                set(params["condition_types"].keys())
            )
        elif isinstance(params["condition_types"], str):
            assert set(condition.predictor.condition_type.unique()).issubset(
                set([params["condition_types"]])
            )
        elif isinstance(params["condition_types"], list):
            assert set(condition.predictor.condition_type.unique()).issubset(
                set(params["condition_types"])
            )

    # Viz config
    assert isinstance(condition.get_viz_config(viz_type="normalized_probe_plot"), dict)
    with pytest.raises(Exception):
        condition.get_viz_config(viz_type="unknown_plot")


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
        care_site_levels=params["care_site_levels"],
        start_date=params["start_date"],
        end_date=params["end_date"],
        stay_types=params["stay_types"],
        care_site_ids=params["care_site_ids"],
        care_site_short_names=params["care_site_short_names"],
        care_site_specialties=params["care_site_specialties"],
        care_sites_sets=params["care_sites_sets"],
        specialties_sets=params["specialties_sets"],
        length_of_stays=params["length_of_stays"],
        concepts_sets=params["concepts_sets"],
        concept_codes=params["concept_codes"],
        stay_source=params["stay_source"],
        provenance_source=params["provenance_source"],
        age_list=params["age_list"],
    )

    # Care site levels
    if params["care_site_levels"]:
        assert set(biology.predictor.care_site_level.unique()) == set(["Hôpital"])

    # Date
    if params["start_date"]:
        assert (biology.predictor.date >= params["start_date"]).all()
    if params["end_date"]:
        assert (biology.predictor.date < params["end_date"]).all()

    # Stay type
    if params["stay_types"]:
        if isinstance(params["stay_types"], dict):
            assert set(biology.predictor.stay_type.unique()).issubset(
                set(params["stay_types"].keys())
            )
        elif isinstance(params["stay_types"], str):
            assert set(biology.predictor.stay_type.unique()).issubset(
                set([params["stay_types"]])
            )
        elif isinstance(params["stay_types"], list):
            assert set(biology.predictor.stay_type.unique()).issubset(
                set(params["stay_types"])
            )

    # Care sites set
    if params["care_sites_sets"]:
        if isinstance(params["care_sites_sets"], dict):
            assert set(biology.predictor.care_sites_set.unique()).issubset(
                set(params["care_sites_sets"].keys())
            )
        elif isinstance(params["care_sites_sets"], str):
            assert set(biology.predictor.care_sites_set.unique()).issubset(
                set([params["care_sites_sets"]])
            )
        elif isinstance(params["care_sites_sets"], list):
            assert set(biology.predictor.care_sites_set.unique()).issubset(
                set(params["care_sites_sets"])
            )

    # Care site id
    if params["care_site_ids"]:
        if isinstance(params["care_site_ids"], str):
            assert biology.predictor.care_site_id.str.startswith(
                params["care_site_ids"]
            ).all()
        elif isinstance(params["care_site_ids"], list):
            assert biology.predictor.care_site_id.str.startswith(
                tuple(params["care_site_ids"])
            ).all()

    # Care site name
    if params["care_site_short_names"]:
        if isinstance(params["care_site_short_names"], str):
            assert biology.predictor.care_site_id.str.startswith(
                params["care_site_short_names"].split("-")[-1]
            ).all()
        elif isinstance(params["care_site_short_names"], list):
            assert biology.predictor.care_site_id.str.startswith(
                tuple(map(lambda x: x.split("-")[-1], params["care_site_short_names"]))
            ).all()

    # Specialty sets
    if params["specialties_sets"]:
        if isinstance(params["specialties_sets"], dict):
            assert set(biology.predictor.specialties_set.unique()).issubset(
                set(params["specialties_sets"].keys())
            )
        elif isinstance(params["specialties_sets"], str):
            assert set(biology.predictor.specialties_set.unique()).issubset(
                set([params["specialties_sets"]])
            )
        elif isinstance(params["specialties_sets"], list):
            assert set(biology.predictor.specialties_set.unique()).issubset(
                set(params["specialties_sets"])
            )

    # Care site specialty
    if params["care_site_specialties"]:
        care_site_filters = filter_table_by_care_site(
            table_to_filter=data.care_site,
            care_site_relationship=biology.care_site_relationship,
            care_site_specialties=params["care_site_specialties"],
        ).care_site_id.unique()
        if is_koalas(data.care_site):
            care_site_filters = care_site_filters.to_list()
        assert biology.predictor.care_site_id.isin(care_site_filters).all()

    # Stay durations
    if params["length_of_stays"] and isinstance(params["length_of_stays"], list):
        min_duration = params["length_of_stays"][0]
        max_duration = params["length_of_stays"][-1]
        length_of_stays = [
            "Incomplete stay",
            "<= {} days".format(min_duration),
            ">= {} days".format(max_duration),
        ]
        n_duration = len(params["length_of_stays"])
        for i in range(0, n_duration - 1):
            min = params["length_of_stays"][i]
            max = params["length_of_stays"][i + 1]
            length_of_stays.append("{} days - {} days".format(min, max))
        assert set(biology.predictor.length_of_stay.unique()).issubset(
            set(length_of_stays)
        )

    # Concepts sets
    if params["concepts_sets"]:
        if isinstance(params["concepts_sets"], dict):
            assert set(biology.predictor.concepts_set.unique()).issubset(
                set(params["concepts_sets"].keys())
            )
        elif isinstance(params["concepts_sets"], str):
            assert set(biology.predictor.concepts_set.unique()).issubset(
                set([params["concepts_sets"]])
            )
        elif isinstance(params["concepts_sets"], list):
            assert set(biology.predictor.concepts_set.unique()).issubset(
                set(params["concepts_sets"])
            )

    # Viz config
    assert isinstance(biology.get_viz_config(viz_type="normalized_probe_plot"), dict)
    with pytest.raises(Exception):
        biology.get_viz_config(viz_type="unknown_plot")
