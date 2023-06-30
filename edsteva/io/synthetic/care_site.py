import pandas as pd


def generate_care_site_tables(structure, parent=None, final=True):
    cs = []
    fr = []
    for key, value in structure.items():
        this_cs = _split_name_id(key)
        cs.append(this_cs)

        if parent is not None:
            this_fr = dict(
                fact_id_1=this_cs["care_site_id"],
                fact_id_2=parent["care_site_id"],
            )
            fr.append(this_fr)

        if value is not None:
            next_cs, next_fr = generate_care_site_tables(
                value, parent=this_cs, final=False
            )
            cs.extend(next_cs)
            fr.extend(next_fr)

    if final:
        cs = pd.DataFrame(cs)
        fr = pd.DataFrame(fr)
        fr["domain_concept_id_1"] = 57
        fr["relationship_concept_id"] = 46233688

    return cs, fr


def _split_name_id(string):
    splitted = string.split("-")

    # Name - Type - ID
    return dict(
        care_site_short_name=string,
        care_site_type_source_value=splitted[0],
        care_site_id=splitted[1],
    )
