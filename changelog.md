# Changelog
## Unreleased - 03-11-2023

## v0.2.8 - 03-11-2023
- Add condition filtering on biology
- Add by-concept-code predictor
- Add UF, UH, UC care sites in biology probes
- Add gender as filtering features in all probes
- Fix partition columns issue, now each probes have specific partition columns attribute
## v0.2.7 - 20-09-2023

- Add new test for BiologyProbe
- Custom probe is simplified
- New parameters GHM and Condition for probe computation
## v0.2.6 - 09-08-2023

- Update LocalData for EDS-Tuto.
## v0.2.5 - 09-08-2023

- New Probe parameters:
  - Age range: Age of patient at visit.
  - Provenance source: Where the patient came from before the visit (emergency, consultation, etc.).
  - stay source: Type of care (MCO, PSY, SSY).
- Refacto the params type to make it more uniform.
## v0.2.4 - 28-07-2023

- Viz: Simplify normalized probe plot
## v0.2.3 - 04-07-2023

- Viz: Fix dashboards
## v0.2.2 - 03-07-2023

- Viz: Fix normalized probe
## v0.2.1 - 03-07-2023

- Linting: Improve code style with ruff.
## v0.2.0 - 30-06-2023

- BiologyProbe: Create a brand new probe for biology data.
- Registry: Introduce registry for completeness predictor and visualization.
- Altair: Upgrade to v5.
- Tests: Improve coverage to 99%.
## v0.1.4 - 02-02-2023

- ConditionProbe: Take UF and Pole into account for ORBIS source system.
- Binder: Presentation available.
## v0.1.3 - 22-12-2022

- ConditionProbe: Update, computed as a proportion of number of visit.
## v0.1.2 - 14-12-2022

- ConditionProbe computes the availability of administrative data related to visits with at least one ICD-10 code recorded.
## v0.1.1 - 03-12-2022

- Binder Demo available
## v0.1.0 - 29-11-2022

- Initial release
