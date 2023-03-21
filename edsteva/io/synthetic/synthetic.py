from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Tuple, Union

import numpy as np
import pandas as pd
from databricks import koalas as ks
from loguru import logger

from edsteva.io.synthetic.utils import (
    generate_care_site_tables,
    generate_events_after_t0,
    generate_events_after_t1,
    generate_events_around_t0,
    generate_events_around_t1,
    generate_events_before_t0,
    recursive_items,
)

DataFrame = Union[ks.DataFrame, pd.DataFrame]

CARE_SITE_STRUCTURE = {
    "Hôpital-1": {
        "Pôle/DMU-11": {
            "Unité Fonctionnelle (UF)-111": None,
            "Unité Fonctionnelle (UF)-112": None,
            "Unité Fonctionnelle (UF)-113": {"Unité de consultation (UC)-1131": None},
        },
        "Pôle/DMU-12": {
            "Unité Fonctionnelle (UF)-121": None,
            "Unité Fonctionnelle (UF)-122": {"Unité de consultation (UC)-1221": None},
        },
    },
    "Hôpital-2": {
        "Pôle/DMU-21": {
            "Unité Fonctionnelle (UF)-211": None,
            "Unité Fonctionnelle (UF)-212": {
                "Unité de consultation (UC)-2121": {
                    "Unité de consultation (UC)-21211": None
                },
            },
        },
        "Pôle/DMU-22": {
            "Unité Fonctionnelle (UF)-221": None,
            "Unité Fonctionnelle (UF)-222": None,
        },
    },
    "Hôpital-3": {
        "Pôle/DMU-31": {
            "Unité Fonctionnelle (UF)-311": None,
            "Unité Fonctionnelle (UF)-312": {
                "Unité de consultation (UC)-3121": {
                    "Unité de consultation (UC)-31211": None
                },
            },
            "Unité Fonctionnelle (UF)-313": {"Unité de consultation (UC)-3131": None},
        },
    },
}

OTHER_VISIT_COLUMNS = dict(
    visit_source_value=[
        ("hospitalisés", 0.6),
        ("urgences", 0.2),
        ("consultation", 0.2),
    ],
    row_status_source_value=[
        ("Actif", 0.999),
        ("supprimé", 0.001),
    ],
)

OTHER_CONDITION_COLUMNS = dict(
    condition_source_value=[
        ("A001", 0.6),
        ("C001", 0.25),
        ("D001", 0.15),
    ],
    condition_status_source_value=[
        ("DP", 0.2),
        ("DR", 0.3),
        ("DAS", 0.4),
        ("DAD", 0.1),
    ],
    row_status_source_value=[
        ("Actif", 0.999),
        ("supprimé", 0.001),
    ],
    cdm_source=[
        ("ORBIS", 1.0),
    ],
)

OTHER_DETAIL_COLUMNS = dict(
    visit_detail_type_source_value=[
        ("PASS UF", 0.4),
        ("SSR", 0.1),
        ("RUM", 0.5),
    ],
    row_status_source_value=[
        ("Actif", 0.999),
        ("supprimé", 0.001),
    ],
)

OTHER_NOTE_COLUMNS = dict(
    note_text=[
        ("Losem Ipsum", 0.999),
        (None, 0.001),
    ],
    row_status_source_value=[
        ("Actif", 0.999),
        ("supprimé", 0.001),
    ],
)

OTHER_MEASUREMENT_COLUMNS = dict(
    row_status_source_value=[
        ("Validé", 0.9),
        ("Discontinué", 0.02),
        ("Disponible", 0.02),
        ("Attendu", 0.02),
        ("Confirmé", 0.02),
        ("Initial", 0.02),
    ],
)


def add_other_columns(table: pd.DataFrame, other_columns: Dict):
    for name, params in other_columns.items():
        options, prob = list(zip(*params))
        table[name] = pd.Series(np.random.choice(options, size=len(table), p=prob))
    return table


def generate_stays_step(
    t_start: int,
    t_end: int,
    n_events: int,
    increase_time: int,
    increase_ratio: float,
    care_site_id: int,
    date_col: str,
):
    t0 = np.random.randint(t_start + increase_time, t_end - increase_time)
    params = dict(
        t_start=t_start,
        t_end=t_end,
        n_events=n_events,
        t0=t0,
        increase_ratio=increase_ratio,
        increase_time=increase_time,
    )
    df = pd.concat(
        [
            generate_events_before_t0(**params),
            generate_events_after_t0(**params),
            generate_events_around_t0(**params),
        ]
    ).to_frame()
    df.columns = [date_col]
    df["care_site_id"] = care_site_id
    df["t_0_min"] = t0 - increase_time / 2
    df["t_0_max"] = t0 + increase_time / 2

    return df


def generate_stays_rect(
    t_start: int,
    t_end: int,
    n_events: int,
    increase_time: int,
    increase_ratio: float,
    care_site_id: int,
    date_col: str,
):
    t0 = np.random.randint(
        t_start + increase_time, (t_end + t_start) / 2 - increase_time
    )
    t1 = np.random.randint((t_end + t_start) / 2 + increase_time, t_end - increase_time)
    t0_params = dict(
        t_start=t_start,
        t_end=t1 - increase_time / 2,
        n_events=n_events,
        t0=t0,
        increase_ratio=increase_ratio,
        increase_time=increase_time,
    )
    before_t0 = generate_events_before_t0(**t0_params)
    around_t0 = generate_events_around_t0(**t0_params)
    # Raise n_visit to enforce a rectangle shape
    between_t0_t1 = generate_events_after_t0(**t0_params)
    t1_params = dict(
        t_start=t_start,
        t_end=t_end,
        n_events=n_events,
        t1=t1,
        increase_time=increase_time,
        increase_ratio=increase_ratio,
    )
    around_t1 = generate_events_around_t1(**t1_params)
    after_t1 = generate_events_after_t1(**t1_params)

    df = pd.concat(
        [
            before_t0,
            around_t0,
            between_t0_t1,
            around_t1,
            after_t1,
        ]
    ).to_frame()

    df.columns = [date_col]
    df["care_site_id"] = care_site_id
    df["t_0_min"] = t0 - increase_time / 2
    df["t_0_max"] = t0 + increase_time / 2
    df["t_1_min"] = t1 - increase_time / 2
    df["t_1_max"] = t1 + increase_time / 2

    return df


def generate_note_step(
    visit_care_site,
    note_type,
    care_site_id,
    date_col,
    id_visit_col,
    note_type_col,
    t0_visit,
    t_end,
):
    t0 = np.random.randint(t0_visit, t_end)
    c_before = np.random.uniform(0, 0.2)
    c_after = np.random.uniform(0.8, 1)

    note_before_t0_visit = visit_care_site[visit_care_site[date_col] <= t0_visit][
        [id_visit_col]
    ].sample(frac=c_before)

    # Stratify visit between t0_visit and t0 to
    # ensure that these elements are represented
    # in the final notes dataset.
    note_before_t0 = visit_care_site[
        (visit_care_site[date_col] <= t0) & (visit_care_site[date_col] > t0_visit)
    ][[id_visit_col]].sample(frac=c_before)

    note_after_t0 = visit_care_site[visit_care_site[date_col] > t0][
        [id_visit_col]
    ].sample(frac=c_after)

    note = pd.concat([note_before_t0_visit, note_before_t0, note_after_t0])

    note[note_type_col] = note_type
    note["care_site_id"] = care_site_id
    note["t_0"] = t0

    return note


def generate_note_rect(
    visit_care_site: pd.DataFrame,
    note_type,
    care_site_id,
    date_col,
    id_visit_col,
    note_type_col,
    t0_visit,
    t1_visit,
):
    t0 = np.random.randint(t0_visit, t0_visit + (t1_visit - t0_visit) / 3)
    t1 = np.random.randint(t0_visit + 2 * (t1_visit - t0_visit) / 3, t1_visit)
    c_out = np.random.uniform(0, 0.1)
    c_in = np.random.uniform(0.8, 1)

    note_before_t0 = visit_care_site[visit_care_site[date_col] <= t0][
        [id_visit_col]
    ].sample(frac=c_out)

    note_between_t0_t1 = visit_care_site[
        (visit_care_site[date_col] > t0) & (visit_care_site[date_col] <= t1)
    ][[id_visit_col]].sample(frac=c_in)

    note_after_t1 = visit_care_site[(visit_care_site[date_col] > t1)][
        [id_visit_col]
    ].sample(frac=c_out)

    note = pd.concat(
        [
            note_before_t0,
            note_between_t0_t1,
            note_after_t1,
        ]
    )

    note[note_type_col] = note_type
    note["care_site_id"] = care_site_id
    note["t_0"] = t0
    note["t_1"] = t1

    return note


def generate_bio_step(
    t_start: int,
    t_end: int,
    n_events: int,
    increase_time: int,
    increase_ratio: float,
    bio_date_col: str,
    unit: str,
    concept_code: str,
):
    t0 = np.random.randint(t_start + increase_time, t_end - increase_time)
    params = dict(
        t_start=t_start,
        t_end=t_end,
        n_events=n_events,
        t0=t0,
        increase_ratio=increase_ratio,
        increase_time=increase_time,
    )
    df = pd.concat(
        [
            generate_events_before_t0(**params),
            generate_events_after_t0(**params),
            generate_events_around_t0(**params),
        ]
    ).to_frame()
    df.columns = [bio_date_col]
    df["unit_source_value"] = unit
    df["measurement_source_concept_id"] = concept_code
    df["t_0_min"] = t0 - increase_time / 2
    df["t_0_max"] = t0 + increase_time / 2

    return df


def generate_bio_rect(
    t_start: int,
    t_end: int,
    n_events: int,
    increase_time: int,
    increase_ratio: float,
    bio_date_col: str,
    unit: str,
    concept_code: str,
):
    t0 = np.random.randint(
        t_start + increase_time, (t_end + t_start) / 2 - increase_time
    )
    t1 = np.random.randint((t_end + t_start) / 2 + increase_time, t_end - increase_time)
    t0_params = dict(
        t_start=t_start,
        t_end=t1 - increase_time / 2,
        n_events=n_events,
        t0=t0,
        increase_ratio=increase_ratio,
        increase_time=increase_time,
    )
    before_t0 = generate_events_before_t0(**t0_params)
    around_t0 = generate_events_around_t0(**t0_params)
    # Raise n_visit to enforce a rectangle shape
    between_t0_t1 = generate_events_after_t0(**t0_params)
    t1_params = dict(
        t_start=t_start,
        t_end=t_end,
        n_events=n_events,
        t1=t1,
        increase_time=increase_time,
        increase_ratio=increase_ratio,
    )
    around_t1 = generate_events_around_t1(**t1_params)
    after_t1 = generate_events_after_t1(**t1_params)

    df = pd.concat(
        [
            before_t0,
            around_t0,
            between_t0_t1,
            around_t1,
            after_t1,
        ]
    ).to_frame()

    df.columns = [bio_date_col]
    df["unit_source_value"] = unit
    df["measurement_source_concept_id"] = concept_code
    df["t_0_min"] = t0 - increase_time / 2
    df["t_0_max"] = t0 + increase_time / 2
    df["t_1_min"] = t1 - increase_time / 2
    df["t_1_max"] = t1 + increase_time / 2

    return df


@dataclass
class SyntheticData:
    module: str = "pandas"
    mean_visit: int = 1000
    id_visit_col: str = "visit_occurrence_id"
    id_visit_source_col: str = "visit_occurrence_source_value"
    id_condition_col: str = "condition_occurrence_id"
    id_detail_col: str = "visit_detail_id"
    id_note_col: str = "note_id"
    id_bio_col: str = "measurement_id"
    note_type_col: str = "note_class_source_value"
    date_col: str = "visit_start_datetime"
    end_date_col: str = "visit_end_datetime"
    detail_date_col: str = "visit_detail_start_datetime"
    bio_date_col: str = "measurement_datetime"
    t_min: datetime = datetime(2010, 1, 1)
    t_max: datetime = datetime(2020, 1, 1)
    other_visit_columns: Dict = field(default_factory=lambda: OTHER_VISIT_COLUMNS)
    other_detail_columns: Dict = field(default_factory=lambda: OTHER_DETAIL_COLUMNS)
    other_condition_columns: Dict = field(
        default_factory=lambda: OTHER_CONDITION_COLUMNS
    )
    other_note_columns: Dict = field(default_factory=lambda: OTHER_NOTE_COLUMNS)
    other_measurement_columns: Dict = field(
        default_factory=lambda: OTHER_MEASUREMENT_COLUMNS
    )
    seed: int = None
    mode: str = "step"

    def generate(self):
        if self.seed:
            np.random.seed(seed=self.seed)

        care_site, fact_relationship, hospital_ids = self._generate_care_site_tables()
        visit_occurrence = self._generate_visit_occurrence(hospital_ids)
        visit_detail = self._generate_visit_detail(visit_occurrence)
        condition_occurrence = self._generate_condition_occurrence(visit_detail)
        note = self._generate_note(hospital_ids, visit_occurrence)
        concept, concept_relationship, src_concept_name = self._generate_concept()
        measurement = self._generate_measurement(
            visit_occurrence=visit_occurrence,
            hospital_ids=hospital_ids,
            src_concept_name=src_concept_name,
        )

        self.care_site = care_site
        self.visit_occurrence = visit_occurrence
        self.condition_occurrence = condition_occurrence
        self.fact_relationship = fact_relationship
        self.visit_detail = visit_detail
        self.note = note
        self.concept = concept
        self.concept_relationship = concept_relationship
        self.measurement = measurement

        self.list_available_tables()

        if self.module == "koalas":
            self.convert_to_koalas()
        return self

    def _generate_care_site_tables(self):
        care_site, fact_relationship = generate_care_site_tables(CARE_SITE_STRUCTURE)
        hospital_ids = list(
            care_site[care_site.care_site_type_source_value == "Hôpital"].care_site_id
        )
        return care_site, fact_relationship, hospital_ids

    def _generate_visit_occurrence(self, hospital_ids):
        visit_occurrence = []
        t_min = self.t_min.timestamp()
        t_max = self.t_max.timestamp()
        for care_site_id in hospital_ids:
            t_start = t_min + np.random.randint(0, (t_max - t_min) / 20)
            t_end = t_max - np.random.randint(0, (t_max - t_min) / 20)
            n_visits = np.random.normal(self.mean_visit, self.mean_visit / 5)
            increase_time = np.random.randint(
                (t_end - t_start) / 100, (t_end - t_start) / 10
            )
            increase_ratio = np.random.uniform(150, 200)
            params = dict(
                t_start=t_start,
                t_end=t_end,
                n_events=n_visits,
                increase_ratio=increase_ratio,
                increase_time=increase_time,
                care_site_id=care_site_id,
                date_col=self.date_col,
            )
            if self.mode == "step":
                vo_stays = generate_stays_step(**params)
            elif self.mode == "rect":
                vo_stays = generate_stays_rect(**params)
            else:
                raise ValueError(
                    f"Unknown mode {self.mode}, options are ('step', 'rect')"
                )
            visit_occurrence.append(vo_stays)

        visit_occurrence = pd.concat(visit_occurrence).reset_index(drop=True)
        visit_occurrence[self.end_date_col] = visit_occurrence[
            self.date_col
        ] + pd.to_timedelta(
            pd.Series(
                np.random.choice([None] * 50 + list(range(100)), len(visit_occurrence))
            ),
            unit="days",
        )
        visit_occurrence[self.id_visit_col] = range(visit_occurrence.shape[0])
        visit_occurrence[self.id_visit_source_col] = range(visit_occurrence.shape[0])
        visit_occurrence = add_other_columns(visit_occurrence, self.other_visit_columns)

        return visit_occurrence

    def _generate_visit_detail(self, visit_occurrence):
        t_min = self.t_min.timestamp()
        t_max = self.t_max.timestamp()
        visit_detail = []
        cols = visit_occurrence.columns
        col_to_idx = dict(zip(cols, range(len(cols))))

        for visit in visit_occurrence.values:
            n_visit_detail = np.random.randint(1, 5)
            visit_date_start = visit[col_to_idx[self.date_col]].timestamp()
            visit_date_end = min(visit_date_start + (t_max - t_min) / 50, t_max)
            visit_id = visit[col_to_idx["visit_occurrence_id"]]
            hospital_id = visit[col_to_idx["care_site_id"]]
            uf_ids = [
                uf.split("-")[1]
                for uf in recursive_items(
                    CARE_SITE_STRUCTURE["Hôpital-{}".format(hospital_id)]
                )
            ]
            care_site_ids = np.random.choice(uf_ids, n_visit_detail)
            detail = pd.DataFrame(
                {
                    self.id_visit_col: visit_id,
                    self.detail_date_col: pd.to_datetime(
                        np.random.randint(
                            visit_date_start, visit_date_end, n_visit_detail
                        ),
                        unit="s",
                    ),
                    "care_site_id": care_site_ids,
                }
            )
            visit_detail.append(detail)

        visit_detail = pd.concat(visit_detail).reset_index(drop=True)
        visit_detail[self.id_detail_col] = range(visit_detail.shape[0])
        visit_detail = add_other_columns(
            visit_detail,
            self.other_detail_columns,
        )

        return visit_detail

    def _generate_condition_occurrence(self, visit_detail):
        visit_detail = visit_detail[
            visit_detail.visit_detail_type_source_value == "RUM"
        ]
        condition_occurrence = []
        cols = visit_detail.columns
        col_to_idx = dict(zip(cols, range(len(cols))))

        for visit in visit_detail.values:
            n_condition = np.random.randint(1, 5)
            detail_id = visit[col_to_idx[self.id_detail_col]]
            visit_id = visit[col_to_idx[self.id_visit_col]]
            condition = pd.DataFrame(
                {
                    self.id_detail_col: [detail_id] * n_condition,
                    self.id_visit_col: [visit_id] * n_condition,
                }
            )
            condition_occurrence.append(condition)

        condition_occurrence = pd.concat(condition_occurrence).reset_index(drop=True)
        condition_occurrence[self.id_condition_col] = range(
            condition_occurrence.shape[0]
        )
        condition_occurrence = add_other_columns(
            condition_occurrence, self.other_condition_columns
        )

        return condition_occurrence

    def _generate_note(
        self,
        hospital_ids,
        visit_occurrence,
        note_types: Tuple[str] = ("CRH", "URGENCE", "ORDONNANCE"),
    ):
        date_col = self.date_col
        id_visit_col = self.id_visit_col
        id_note_col = self.id_note_col
        note_type_col = self.note_type_col

        notes = []
        for care_site_id in hospital_ids:
            visit_care_site = visit_occurrence[
                visit_occurrence.care_site_id == care_site_id
            ].reset_index(drop=True)
            visit_care_site[date_col] = (
                visit_care_site[date_col].view("int64") // 10**9
            )
            t0_visit = visit_care_site["t_0_min"].max()
            params = dict(
                care_site_id=care_site_id,
                date_col=date_col,
                id_visit_col=id_visit_col,
                note_type_col=note_type_col,
                t0_visit=t0_visit,
            )

            for note_type in note_types:
                params["note_type"] = note_type
                if self.mode == "step":
                    params["t_end"] = visit_care_site[date_col].max()
                    note = generate_note_step(visit_care_site, **params)
                elif self.mode == "rect":
                    params["t1_visit"] = visit_care_site["t_1_min"].max()
                    note = generate_note_rect(visit_care_site, **params)
                else:
                    raise ValueError(
                        f"Unknown mode {self.mode}, options: ('step', 'rect')"
                    )
                notes.append(note)

        notes = pd.concat(notes).reset_index(drop=True)
        notes[id_note_col] = range(notes.shape[0])
        notes = add_other_columns(notes, self.other_note_columns)

        return notes

    def _generate_concept(
        self, n_entity: int = 5, units: List[str] = ["g", "g/l", "mol", "s"]
    ):
        loinc_concept_id = []
        loinc_itm_concept_id = []
        loinc_concept_code = []
        loinc_itm_concept_code = []
        anabio_concept_id = []
        anabio_itm_concept_id = []
        anabio_concept_code = []
        anabio_itm_concept_code = []
        src_concept_code = []
        loinc_concept_name = []
        loinc_itm_concept_name = []
        anabio_concept_name = []
        anabio_itm_concept_name = []
        src_concept_name = []
        concept_id_1 = []
        concept_id_2 = []
        relationship_id = []
        for i in range(n_entity):
            n_loinc = np.random.randint(1, 4)
            loinc_codes = [str(i) + str(j) + "-0" for j in range(n_loinc)]
            loinc_concept_code.extend(loinc_codes)
            loinc_concept_id.extend(loinc_codes)
            unit_values = np.random.choice(units, n_loinc)
            for loinc_code in loinc_codes:
                unit_value = np.random.choice(unit_values)
                loinc_concept_name.append("LOINC_" + loinc_code + "_" + unit_value)
                has_loinc_itm = np.random.random() >= 0.5
                if has_loinc_itm:
                    loinc_id_itm = loinc_code + "_ITM"
                    loinc_itm_concept_code.append(loinc_code)
                    loinc_itm_concept_id.append(loinc_id_itm)
                    loinc_itm_concept_name.append(
                        "LOINC_" + loinc_id_itm + "_" + unit_value
                    )
                n_anabio = np.random.randint(1, 3)
                supp_code = "9" if len(str(i)) == 1 else ""
                anabio_codes = [
                    "A" + loinc_code.split("-")[0] + str(j) + supp_code
                    for j in range(n_anabio)
                ]
                anabio_concept_code.extend(anabio_codes)
                anabio_concept_id.extend(anabio_codes)
                for anabio_code in anabio_codes:
                    anabio_concept_name.append(
                        "ANABIO_" + anabio_code + "_" + unit_value
                    )
                    has_anabio_itm = np.random.random() >= 0.5
                    if has_anabio_itm:
                        anabio_id_itm = anabio_code + "_ITM"
                        anabio_itm_concept_code.append(anabio_code)
                        anabio_itm_concept_id.append(anabio_id_itm)
                        anabio_itm_concept_name.append(
                            "ANABIO_" + anabio_id_itm + "_" + unit_value
                        )
                        concept_id_1.extend([anabio_id_itm, anabio_code])
                        concept_id_2.extend([anabio_code, anabio_id_itm])
                        relationship_id.extend(["Maps to", "Mapped from"])
                        if has_loinc_itm:
                            concept_id_1.extend([anabio_id_itm, loinc_id_itm])
                            concept_id_2.extend([loinc_id_itm, anabio_id_itm])
                            relationship_id.extend(["Maps to", "Mapped from"])
                    n_src = np.random.randint(1, 3)
                    src_codes = [
                        loinc_code + "-" + anabio_code + "-" + str(j)
                        for j in range(n_src)
                    ]
                    src_concept_code.extend(src_codes)
                    src_concept_name.extend(
                        ["SRC_" + src_code + "_" + unit_value for src_code in src_codes]
                    )
                    for src_code in src_codes:
                        concept_id_1.extend(
                            [src_code, src_code, anabio_code, loinc_code]
                        )
                        concept_id_2.extend(
                            [anabio_code, loinc_code, src_code, src_code]
                        )
                        relationship_id.extend(
                            ["Maps to", "Maps to", "Mapped from", "Mapped from"]
                        )

        src_vocabulary_id = ["Analyses Laboratoire"] * len(src_concept_code)
        glims_anabio_vocabulary_id = ["GLIMS XXX Anabio"] * len(anabio_concept_id)
        itm_anabio_vocabulary_id = ["ITM - ANABIO"] * len(anabio_itm_concept_id)
        glims_loinc_vocabulary_id = ["GLIMS XXX LOINC"] * len(loinc_concept_id)
        itm_loinc_vocabulary_id = ["ITM - LOINC"] * len(loinc_itm_concept_id)

        concept_id = (
            src_concept_code
            + anabio_concept_id
            + anabio_itm_concept_id
            + loinc_concept_id
            + loinc_itm_concept_id
        )
        concept_code = (
            src_concept_code
            + anabio_concept_code
            + anabio_itm_concept_code
            + loinc_concept_code
            + loinc_itm_concept_code
        )
        concept_name = (
            src_concept_name
            + anabio_concept_name
            + anabio_itm_concept_name
            + loinc_concept_name
            + loinc_itm_concept_name
        )
        vocabulary_id = (
            src_vocabulary_id
            + glims_anabio_vocabulary_id
            + itm_anabio_vocabulary_id
            + glims_loinc_vocabulary_id
            + itm_loinc_vocabulary_id
        )

        concept = pd.DataFrame(
            {
                "concept_id": concept_id,
                "concept_code": concept_code,
                "concept_name": concept_name,
                "vocabulary_id": vocabulary_id,
            }
        )

        concept_relationship = pd.DataFrame(
            {
                "concept_id_1": concept_id_1,
                "concept_id_2": concept_id_2,
                "relationship_id": relationship_id,
            }
        )

        return concept, concept_relationship, src_concept_name

    def _generate_measurement(
        self,
        visit_occurrence: pd.DataFrame,
        hospital_ids: List[int],
        src_concept_name: List[str],
        mean_measurement: int = 1000,
        units: List[str] = ["g", "g/l", "mol", "s"],
    ):
        t_min = self.t_min.timestamp()
        t_max = self.t_max.timestamp()
        measurements = []
        for concept_name in src_concept_name:
            concept_code = concept_name.split("_")[1]
            unit = concept_name.split("_")[-1]
            mean_value = (1 + units.index(unit)) * 2
            std_value = 1
            for care_site_id in hospital_ids:
                t_start = t_min + np.random.randint(0, (t_max - t_min) / 20)
                t_end = t_max - np.random.randint(0, (t_max - t_min) / 20)
                valid_measurements = int(
                    np.random.normal(mean_measurement, mean_measurement / 5)
                )
                missing_value = int(np.random.uniform(1, valid_measurements / 10))
                n_measurements = valid_measurements + missing_value
                increase_time = np.random.randint(
                    (t_end - t_start) / 100, (t_end - t_start) / 10
                )
                increase_ratio = np.random.uniform(150, 200)
                concept_code = concept_name.split("_")[1]
                unit = concept_name.split("_")[-1]
                mean_value = (1 + units.index(unit)) * 2
                std_value = 1
                params = dict(
                    t_start=t_start,
                    t_end=t_end,
                    n_events=n_measurements,
                    increase_ratio=increase_ratio,
                    increase_time=increase_time,
                    bio_date_col=self.bio_date_col,
                    unit=unit,
                    concept_code=concept_code,
                )
                if self.mode == "step":
                    measurement = generate_bio_step(**params)
                elif self.mode == "rect":
                    measurement = generate_bio_rect(**params)
                else:
                    raise ValueError(
                        f"Unknown mode {self.mode}, options are ('step', 'rect')"
                    )
                visit_care_site = visit_occurrence[
                    visit_occurrence.care_site_id == care_site_id
                ]
                measurement[self.id_visit_col] = (
                    visit_care_site[self.id_visit_col]
                    .sample(measurement.shape[0], replace=True)
                    .reset_index(drop=True)
                )
                measurement["value_as_number"] = [None] * missing_value + list(
                    np.random.normal(
                        mean_value, std_value, measurement.shape[0] - missing_value
                    )
                )
                measurements.append(measurement)

        measurements = pd.concat(measurements).reset_index(drop=True)
        measurements["value_source_value"] = (
            measurements["value_as_number"].astype(str)
            + " "
            + measurements["unit_source_value"].astype(str)
        )
        measurements[self.id_bio_col] = range(measurements.shape[0])
        measurements = add_other_columns(measurements, self.other_measurement_columns)

        return measurements

    def convert_to_koalas(self):
        if isinstance(self.care_site, ks.frame.DataFrame):
            print("Module is already Koalas!")
            return
        self.care_site = ks.DataFrame(self.care_site)
        self.visit_occurrence = ks.DataFrame(self.visit_occurrence)
        self.condition_occurrence = ks.DataFrame(self.condition_occurrence)
        self.fact_relationship = ks.DataFrame(self.fact_relationship)
        self.visit_detail = ks.DataFrame(self.visit_detail)
        self.note = ks.DataFrame(self.note)
        self.module = "koalas"

    def reset_to_pandas(self):
        if isinstance(self.care_site, pd.core.frame.DataFrame):
            print("Module is already Pandas!")
            return
        self.care_site = self.care_site.to_pandas()
        self.visit_occurrence = self.visit_occurrence.to_pandas()
        self.condition_occurrence = self.condition_occurrence.to_pandas()
        self.fact_relationship = self.fact_relationship.to_pandas()
        self.visit_detail = self.visit_detail.to_pandas()
        self.note = self.note.to_pandas()
        self.module = "pandas"

    def delete_table(self, table_name: str) -> None:
        if hasattr(self, table_name):
            delattr(self, table_name)
            logger.info("Table {} has been deleted", table_name)
        else:
            logger.info("Table {} does not exist", table_name)
        self.list_available_tables()

    def list_available_tables(self) -> List[str]:
        available_tables = []
        for key, item in self.__dict__.items():
            if isinstance(item, DataFrame.__args__):
                available_tables.append(key)
        self.available_tables = available_tables
