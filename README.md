# Pixar Analytics

![dbt Core](https://img.shields.io/badge/dbt-Core%201.10-orange?logo=dbt&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Cloud-29B5E8?logo=snowflake&logoColor=white)
![ELT](https://img.shields.io/badge/Pattern-ELT-0A66C2)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)

Snowflake + dbt ELT project that converts raw Pixar film data into governed analytical models for finance, ratings, awards, genre, talent, and trend analysis.

## Executive Overview

This repository implements a layered ELT analytics stack on Snowflake using dbt. The project ingests raw CSV-backed film data into a RAW schema, standardizes and enriches it through staging and intermediate models, and publishes dimensional/fact/reporting tables in MARTS for downstream analysis.

The design emphasizes:

- SQL-first transformations pushed down to Snowflake compute
- Clear model layering and dependency management via dbt
- Reusable macros and package-driven conventions
- Explicit data quality checks (schema tests + custom SQL tests)
- Traceable build artifacts committed under target for reproducibility evidence

## Architecture And ELT Flow

### End-to-end flow

```text
CSV files
	-> Snowflake stage + COPY INTO (RAW)
	-> dbt staging views (cleaning/typing/standardization)
	-> dbt intermediate views (business logic)
	-> dbt marts tables (dimensions/facts/reports)
	-> analytics consumption in Snowflake
```

### Warehouse and schema strategy

- Warehouse: PIXAR_WH
- Database: PIXAR_DB
- Schemas: RAW, STAGING, MARTS
- Role model: PIXAR_ROLE granted ownership/usage in setup SQL

The provisioning and load sequence is codified in snowflake_ui_code.sql.

## Repository Structure

```text
pixar_analytics/
├─ dbt_project.yml
├─ packages.yml
├─ profiles.yml
├─ snowflake_ui_code.sql
├─ macros/
│  ├─ generate_surrogate_key.sql
│  ├─ get_current_timestamp.sql
│  └─ get_custom_schema.sql
├─ models/
│  ├─ staging/         (7 SQL models + sources)
│  ├─ intermediate/    (3 SQL models)
│  └─ marts/           (15 SQL models + schema tests)
├─ tests/              (4 custom singular SQL tests)
├─ analyses/
├─ seeds/
├─ snapshots/
└─ target/             (manifest, run_results, compiled/run SQL)
```

## Dataset Coverage

Source coverage is defined in models/staging/_sources.yml and loaded in snowflake_ui_code.sql.

Raw entities:

- pixar_films: film metadata, release date, runtime, rating, plot
- pixar_people: people by film and role type
- genre: categorical descriptors by film
- box_office: budget and regional/worldwide grosses
- public_response: Rotten Tomatoes, Metacritic, IMDb, CinemaScore fields
- academy: award category and nomination/win status
- pixar_data_dictionary: metadata dictionary for source fields

Analytical domains covered by marts:

- Financial performance and budget efficiency
- Critical reception and cross-platform score consistency
- Awards outcomes and performance correlations
- Genre and franchise behavior
- Seasonal, runtime, and competition positioning trends
- Director performance analytics

## Major Features And Why They Exist

| Feature | Implemented In | What It Does | Why It Exists |
|---|---|---|---|
| Layered ELT modeling | models/staging, models/intermediate, models/marts | Separates cleaning, business logic, and serving models | Keeps transformations maintainable and auditable |
| Surrogate key strategy | macros/generate_surrogate_key.sql + dim models | Builds stable hashed keys (film_key, person_key) | Decouples analytics keys from mutable source text |
| Financial classification | int_film_financials, rpt_financial_analysis | Derives ROI and performance tiers | Enables portfolio-level profitability comparisons |
| Cross-platform rating normalization | int_film_ratings | Harmonizes critic metrics (including IMDb scale conversion) | Prevents invalid averaging across incompatible score scales |
| Awards aggregation and correlation | int_film_awards, fact_film_awards, rpt_awards_analysis | Summarizes wins/nominations and links to commercial performance | Supports impact analysis of awards vs outcomes |
| Talent analytics | dim_people, rpt_people_analysis | Aggregates director/producer career-level metrics | Enables people-centric performance assessment |
| Strategic report marts | rpt_* models | Produces decision-ready views (genre, franchise, seasonal, runtime, competition) | Provides focused business questions without repeated ad hoc SQL |
| Data quality assertions | models/marts/_schema.yml + tests/*.sql | Validates keys, ranges, relationships, and business rules | Detects regressions before downstream consumption |

## Snowflake + dbt Implementation Details

### dbt project behavior

Configured in dbt_project.yml:

- staging: materialized as view in schema staging
- intermediate: materialized as view in schema staging
- marts: materialized as table in schema marts
- vars: start_date=1995-01-01, end_date=2024-12-31

### Macro usage

- generate_surrogate_key.sql wraps dbt_utils generate_surrogate_key with adapter dispatch
- get_custom_schema.sql overrides generate_schema_name to honor explicit schema names
- get_current_timestamp.sql centralizes timestamp expression

### Package usage

Configured in packages.yml:

- dbt-labs/dbt_utils (v1.3.0)
- metaplane/dbt_expectations (v0.10.9)

Usage patterns in this project include surrogate key generation and expectation-style numeric range checks.

## Problems Faced And What Was Solved

1. Key consistency across heterogeneous sources

- Problem: Source joins rely heavily on film names from multiple CSVs.
- Solution: staging models normalize text with trim and enforce null filtering on critical key columns; dimensional models then apply surrogate keys.

2. Metric comparability across rating systems

- Problem: Rotten Tomatoes and Metacritic are 100-point scales while IMDb is 10-point.
- Solution: int_film_ratings rescales IMDb by x10 before computing avg_critic_score.

3. Governance of business logic in a growing analytics surface

- Problem: Logic for finance, awards, genre, and trends can sprawl if implemented ad hoc.
- Solution: domain-specific rpt_* marts encapsulate reusable analytical logic with explicit classifications and ranking windows.

4. Data quality risk in warehouse-native ELT

- Problem: Loading first and transforming later can propagate bad records quickly.
- Solution: schema tests and custom singular tests assert ranges, date logic, financial consistency, and positive box office constraints.

5. Schema targeting across environments

- Problem: dbt default schema behavior can produce inconsistent object locations across targets.
- Solution: custom schema macro enforces deterministic schema naming behavior.

## Setup And Run Instructions

### Prerequisites

- Python 3.9+
- dbt Core + dbt-snowflake adapter
- Snowflake account with privileges to create warehouse/database/schema/role

### 1) Install dbt

```bash
pip install dbt-snowflake
dbt --version
```

### 2) Provision Snowflake objects and load RAW data

Run snowflake_ui_code.sql in Snowflake (worksheet/UI) in reviewable sections:

- warehouse/database/schema creation
- role grants
- file format + stage creation
- RAW table DDL
- COPY INTO commands for CSV ingestion

### 3) Configure profile

Set the dbt profile named pixar_analytics to your environment values (account, user, role, warehouse, database, schema, threads).

Security note: use secret management (environment variables or local-only profiles) for credentials instead of committing sensitive values.

### 4) Install dbt dependencies

```bash
dbt deps
```

### 5) Validate, run, and test

```bash
dbt debug
dbt run --full-refresh
dbt test
```

### 6) Build docs

```bash
dbt docs generate
dbt docs serve
```

## Configuration And Package Notes

- dbt package lock artifacts are present under dbt_packages and package lock files.
- dbt_utils is used for key generation and shared utility macros.
- dbt_expectations is used for bound checks in mart schema tests (for example year and percentage ranges).
- The model selection graph is available in target/manifest.json and target/graph_summary.json.

## Results And Evidence (Repository-Backed)

This section only cites tracked repository artifacts.

- target/run_results.json exists and records a dbt run with:
	- dbt version: 1.10.5
	- invocation command: dbt run --full-refresh
	- generated_at: 2025-07-29T22:45:55.117216Z
	- result count: 25 nodes, status success=25
- target/manifest.json and target/semantic_manifest.json are committed, proving a compiled project graph and metadata snapshot were generated.
- target/compiled/pixar_analytics and target/run/pixar_analytics contain compiled and executed SQL artifacts for models/tests.
- snowflake_ui_code.sql includes post-load and post-transform verification queries that demonstrate intended validation workflow in Snowflake.

What is intentionally not claimed:

- No business KPI values are asserted in this README from untracked query output screenshots.
- No CI/CD pass badge or deployment claim is made because such evidence is not committed.

## Evaluation And Data Quality

Quality controls implemented in the project:

- Generic/schema tests in models/marts/_schema.yml:
	- unique and not_null constraints
	- relationships tests between facts and dimensions
	- accepted_values tests for categorical outputs
	- dbt_expectations numeric bound assertions
- Custom singular SQL tests in tests:
	- assert_positive_box_office.sql
	- assert_score_ranges.sql
	- assert_release_date_logic.sql
	- assert_financial_consistency.sql

Evaluation guidance:

```bash
dbt test
dbt test --select fact_film_performance
dbt test --store-failures
```

## Limitations

- Credentials are currently represented in repository profile configuration; this should be externalized for secure collaboration.
- Several joins are title-based, so title variant mismatches can affect link rates unless additional canonical keys are introduced.
- target artifacts are a historical execution snapshot, not a guarantee of current-environment reproducibility.
- seeds and snapshots directories are present but not actively used in the committed implementation.

## Conclusion

Pixar Analytics demonstrates a practical, warehouse-native dbt implementation: clear layering, reusable macros, domain-focused report marts, and explicit quality checks. It is positioned as an analytics engineering portfolio project with credible implementation depth and reproducible SQL assets.

## Future Improvements

1. Externalize credentials and role/account config via environment-based profile templating.
2. Add source freshness checks and exposures to formalize SLA-style data contracts.
3. Introduce snapshots for slowly changing entities (for example people-role history).
4. Add slim CI for dbt parse/build/test against a dev Snowflake target with sanitized sample data.
5. Publish sample query result extracts or dashboard screenshots with commit-linked provenance.

## License

This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.
