from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Tuple

import numpy as np
import pandas as pd
from databricks import koalas as ks

from edsteva.io.synthetic.care_site import generate_care_site_tables
from edsteva.io.synthetic.utils import recursive_items
from edsteva.io.synthetic.visit import (
    generate_after_t0,
    generate_after_t1,
    generate_around_t0,
    generate_around_t1,
    generate_before_t0,
)

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

OTHER_DETAIL_COLUMNS = dict(
    visit_detail_type_source_value=[
        ("PASS", 0.8),
        ("SSR", 0.1),
        ("RUM", 0.1),
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


def add_other_columns(table: pd.DataFrame, other_columns: Dict):
    for name, params in other_columns.items():
        options, prob = list(zip(*params))
        table[name] = pd.Series(np.random.choice(options, size=len(table), p=prob))
    return table


def generate_stays_step(
    t_start: int,
    t_end: int,
    n_visit: int,
    increase_time: int,
    increase_ratio: float,
    care_site_id: int,
    date_col: str,
):
    t0 = np.random.randint(t_start + increase_time, t_end - increase_time)
    params = dict(
        t_start=t_start,
        t_end=t_end,
        n_visit=n_visit,
        t0=t0,
        increase_ratio=increase_ratio,
        increase_time=increase_time,
    )
    df = pd.concat(
        [
            generate_before_t0(**params),
            generate_after_t0(**params),
            generate_around_t0(**params),
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
    n_visit: int,
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
        n_visit=n_visit,
        t0=t0,
        increase_ratio=increase_ratio,
        increase_time=increase_time,
    )
    before_t0 = generate_before_t0(**t0_params)
    around_t0 = generate_around_t0(**t0_params)
    # Raise n_visit to enforce a rectangle shape
    t0_params["n_visit"] *= 5
    between_t0_t1 = generate_after_t0(**t0_params)
    t1_params = dict(
        t_start=t_start,
        t_end=t_end,
        n_visit=n_visit,
        t1=t1,
        increase_time=increase_time,
        increase_ratio=increase_ratio,
    )
    around_t1 = generate_around_t1(**t1_params)
    after_t1 = generate_after_t1(**t1_params)

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
    visit_care_site,
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


@dataclass
class SyntheticData:
    module: str = "pandas"
    mean_visit: int = 1000
    id_visit_col: str = "visit_occurrence_id"
    id_detail_col: str = "visit_detail_id"
    id_note_col: str = "note_id"
    note_type_col: str = "note_class_source_value"
    date_col: str = "visit_start_datetime"
    detail_date_col: str = "visit_detail_start_datetime"
    t_min: datetime = datetime(2010, 1, 1)
    t_max: datetime = datetime(2020, 1, 1)
    other_visit_columns: Dict = field(default_factory=lambda: OTHER_VISIT_COLUMNS)
    other_detail_columns: Dict = field(default_factory=lambda: OTHER_DETAIL_COLUMNS)
    other_note_columns: Dict = field(default_factory=lambda: OTHER_NOTE_COLUMNS)
    seed: int = None
    mode: str = "step"

    def generate(self):
        if self.seed:
            np.random.seed(seed=self.seed)

        care_site, fact_relationship, hospital_ids = self._generate_care_site_tables()
        visit_occurrence = self._generate_visit_occurrence(hospital_ids)
        visit_detail = self._generate_visit_detail(visit_occurrence)
        note = self._generate_note(hospital_ids, visit_occurrence)

        self.available_tables = [
            "care_site",
            "visit_occurrence",
            "fact_relationship",
            "visit_detail",
            "note",
        ]
        self.care_site = care_site
        self.visit_occurrence = visit_occurrence
        self.fact_relationship = fact_relationship
        self.visit_detail = visit_detail
        self.note = note

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
            n_visit = np.random.normal(self.mean_visit, self.mean_visit / 5)
            increase_time = np.random.randint(
                (t_end - t_start) / 100, (t_end - t_start) / 10
            )
            increase_ratio = np.random.uniform(150, 200)
            params = dict(
                t_start=t_start,
                t_end=t_end,
                n_visit=n_visit,
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

        visit_occurrence = pd.concat(visit_occurrence)
        visit_occurrence = add_other_columns(
            visit_occurrence, self.other_visit_columns
        ).reset_index(drop=True)
        visit_occurrence[self.id_visit_col] = range(visit_occurrence.shape[0])

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
        visit_detail = add_other_columns(
            pd.concat(visit_detail),
            self.other_detail_columns,
        ).reset_index(drop=True)
        visit_detail[self.id_detail_col] = range(visit_detail.shape[0])

        return visit_detail

    def _generate_note(
        self, hospital_ids, visit_occurrence, note_types: Tuple[str] = ("CRH", "URG")
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

        notes = pd.concat(notes)
        notes = add_other_columns(notes, self.other_note_columns).reset_index(drop=True)
        notes[id_note_col] = range(notes.shape[0])

        return notes

    def convert_to_koalas(self):
        if isinstance(self.care_site, ks.frame.DataFrame):
            print("Module is already koalas!")
            return
        self.care_site = ks.DataFrame(self.care_site)
        self.visit_occurrence = ks.DataFrame(self.visit_occurrence)
        self.fact_relationship = ks.DataFrame(self.fact_relationship)
        self.visit_detail = ks.DataFrame(self.visit_detail)
        self.note = ks.DataFrame(self.note)
        self.module = "koalas"

    def reset_to_pandas(self):
        if isinstance(self.care_site, pd.core.frame.DataFrame):
            print("Module is already pandas!")
            return
        self.care_site = self.care_site.to_pandas()
        self.visit_occurrence = self.visit_occurrence.to_pandas()
        self.fact_relationship = self.fact_relationship.to_pandas()
        self.visit_detail = self.visit_detail.to_pandas()
        self.note = self.note.to_pandas()
        self.module = "pandas"
