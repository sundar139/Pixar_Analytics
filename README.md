# Pixar Analytics ETL Pipeline

A comprehensive data transformation pipeline for Pixar film analysis using dbt and Snowflake, providing deep insights into the studio's 29-year journey from 1995 to 2024.

## Project Overview

This ETL pipeline transforms raw Pixar data into actionable business intelligence, covering:

- **Financial Performance**: Budget allocation, box office returns, ROI analysis
- **Critical Reception**: Multi-platform rating aggregation and consistency analysis
- **Awards Recognition**: Academy Awards tracking and correlation analysis
- **Industry Evolution**: Trend analysis across decades and genres
- **Talent Analysis**: Director and producer performance metrics

## Architecture Overview

### Data Flow

```
Raw CSV Files → Snowflake Raw Tables → dbt Transformations → Analytics Marts
```

### Layer Structure

- **Raw Layer**: Untransformed CSV data loaded into Snowflake
- **Staging Layer**: Data cleaning, standardization, and basic transformations
- **Intermediate Layer**: Business logic implementation and feature engineering
- **Marts Layer**: Final analytical tables optimized for reporting and analytics

## Dataset Overview

### Core Data Coverage

- **28 Pixar Films** (1995-2024): Complete theatrical releases
- **$2.8B+ Total Budget**: Production investment tracking
- **$14B+ Worldwide Revenue**: Box office performance
- **15,000+ Professional Reviews**: Critical reception analysis
- **2M+ Audience Ratings**: Public sentiment tracking
- **89 Academy Award Records**: Complete Oscar history
- **260+ Personnel Records**: Key talent across all roles

### Data Sources

1. **pixar_films.csv**: Core film information and metadata
2. **pixar_people.csv**: Directors, producers, and key talent
3. **genre.csv**: Content classification and taxonomy
4. **box_office.csv**: Complete financial performance data
5. **public_response.csv**: Multi-platform ratings and reviews
6. **academy.csv**: Awards nominations and wins
7. **pixar_data_dictionary.csv**: Self-documenting metadata

## Quick Start Guide

### Prerequisites

- Snowflake account with appropriate permissions
- Python 3.9+ installed
- VS Code or preferred IDE

### Step 1: Environment Setup

```bash
# Install dbt with Snowflake adapter
pip install dbt-snowflake

# Verify installation
dbt --version
```

### Step 2: Snowflake Configuration

#### A. Create Infrastructure

Execute the following in Snowflake Web Interface:

```sql
-- Create warehouse
CREATE OR REPLACE WAREHOUSE PIXAR_WH
WITH WAREHOUSE_SIZE = 'X-SMALL'
AUTO_SUSPEND = 300
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE;

-- Create database and schemas
CREATE OR REPLACE DATABASE PIXAR_DB;
CREATE OR REPLACE SCHEMA PIXAR_DB.RAW;
CREATE OR REPLACE SCHEMA PIXAR_DB.STAGING;
CREATE OR REPLACE SCHEMA PIXAR_DB.MARTS;

-- Use database and warehouse
USE DATABASE PIXAR_DB;
USE WAREHOUSE PIXAR_WH;
```

#### B. Create Role and User Setup

```sql
-- Create role
CREATE OR REPLACE ROLE PIXAR_ROLE;

-- Grant privileges
GRANT USAGE ON WAREHOUSE PIXAR_WH TO ROLE PIXAR_ROLE;
GRANT USAGE ON DATABASE PIXAR_DB TO ROLE PIXAR_ROLE;
GRANT ALL ON SCHEMA PIXAR_DB.RAW TO ROLE PIXAR_ROLE;
GRANT ALL ON SCHEMA PIXAR_DB.STAGING TO ROLE PIXAR_ROLE;
GRANT ALL ON SCHEMA PIXAR_DB.MARTS TO ROLE PIXAR_ROLE;

-- Grant role to user (replace YOUR_USERNAME)
GRANT ROLE PIXAR_ROLE TO USER YOUR_USERNAME;
ALTER USER YOUR_USERNAME SET DEFAULT_ROLE = PIXAR_ROLE;
ALTER USER YOUR_USERNAME SET DEFAULT_WAREHOUSE = PIXAR_WH;
ALTER USER YOUR_USERNAME SET DEFAULT_DATABASE = PIXAR_DB;
```

#### C. Create File Format and Stage

```sql
-- Create CSV file format
CREATE OR REPLACE FILE FORMAT PIXAR_DB.RAW.CSV_FORMAT
TYPE = 'CSV'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TRIM_SPACE = TRUE
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
NULL_IF = ('NULL', 'null', '', 'N/A', 'n/a');

-- Create internal stage
CREATE OR REPLACE STAGE PIXAR_DB.RAW.PIXAR_STAGE
FILE_FORMAT = PIXAR_DB.RAW.CSV_FORMAT;
```

#### D. Create Raw Tables

```sql
-- Pixar Films Table
CREATE OR REPLACE TABLE PIXAR_DB.RAW.PIXAR_FILMS (
    number INT,
    film STRING,
    release_date STRING,
    run_time INT,
    film_rating STRING,
    plot STRING
);

-- Pixar People Table
CREATE OR REPLACE TABLE PIXAR_DB.RAW.PIXAR_PEOPLE (
    film STRING,
    role_type STRING,
    name STRING
);

-- Genre Table
CREATE OR REPLACE TABLE PIXAR_DB.RAW.GENRE (
    film STRING,
    category STRING,
    value STRING
);

-- Box Office Table
CREATE OR REPLACE TABLE PIXAR_DB.RAW.BOX_OFFICE (
    film STRING,
    budget FLOAT,
    box_office_us_canada INT,
    box_office_other INT,
    box_office_worldwide INT
);

-- Public Response Table
CREATE OR REPLACE TABLE PIXAR_DB.RAW.PUBLIC_RESPONSE (
    film STRING,
    rotten_tomatoes_score INT,
    rotten_tomatoes_counts INT,
    metacritic_score INT,
    metacritic_counts INT,
    cinema_score STRING,
    imdb_score FLOAT,
    imdb_counts INT
);

-- Academy Table
CREATE OR REPLACE TABLE PIXAR_DB.RAW.ACADEMY (
    film STRING,
    award_type STRING,
    status STRING
);

-- Data Dictionary Table
CREATE OR REPLACE TABLE PIXAR_DB.RAW.PIXAR_DATA_DICTIONARY (
    table_name STRING,
    field STRING,
    description STRING
);
```

#### E. Load Data

1. **Upload CSV files** to Snowflake stage using Web UI or SnowSQL
2. **Execute COPY commands**:

```sql
-- Copy data from stage to tables
COPY INTO PIXAR_DB.RAW.PIXAR_FILMS FROM @PIXAR_DB.RAW.PIXAR_STAGE/pixar_films.csv;
COPY INTO PIXAR_DB.RAW.PIXAR_PEOPLE FROM @PIXAR_DB.RAW.PIXAR_STAGE/pixar_people.csv;
COPY INTO PIXAR_DB.RAW.GENRE FROM @PIXAR_DB.RAW.PIXAR_STAGE/genre.csv;
COPY INTO PIXAR_DB.RAW.BOX_OFFICE FROM @PIXAR_DB.RAW.PIXAR_STAGE/box_office.csv;
COPY INTO PIXAR_DB.RAW.PUBLIC_RESPONSE FROM @PIXAR_DB.RAW.PIXAR_STAGE/public_response.csv;
COPY INTO PIXAR_DB.RAW.ACADEMY FROM @PIXAR_DB.RAW.PIXAR_STAGE/academy.csv;
COPY INTO PIXAR_DB.RAW.PIXAR_DATA_DICTIONARY FROM @PIXAR_DB.RAW.PIXAR_STAGE/pixar_data_dictionary.csv;
```

### Step 3: dbt Project Setup

#### A. Initialize Project

```bash
# Initialize project
dbt init pixar_analytics
cd pixar_analytics
```

#### B. Configure Connection

Create or update `~/.dbt/profiles.yml`:

```yaml
pixar_analytics:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: YOUR_ACCOUNT_IDENTIFIER
      user: YOUR_USERNAME
      password: YOUR_PASSWORD
      role: PIXAR_ROLE
      database: PIXAR_DB
      warehouse: PIXAR_WH
      schema: staging
      threads: 4
      keepalives_idle: 600
```

#### C. Project Configuration

Update `dbt_project.yml`:

```yaml
name: "pixar_analytics"
version: "1.0.0"
config-version: 2

profile: "pixar_analytics"

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  pixar_analytics:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: view
      +schema: staging
    marts:
      +materialized: table
      +schema: marts

vars:
  start_date: "1995-01-01"
  end_date: "2024-12-31"
```

#### D. Create Custom Schema Macro

Create `macros/get_custom_schema.sql`:

```sql
-- This macro ensures schemas are created exactly as specified
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

#### E. Install Dependencies

Create `packages.yml`:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.0
  - package: calogica/dbt_expectations
    version: 0.10.9
```

```bash
# Install packages
dbt deps
```

### Step 4: Execute Pipeline

```bash
# Test connection
dbt debug

# Run all transformations
dbt run

# Execute data quality tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Project Structure

```
pixar_analytics/
├── dbt_project.yml              # Main project configuration
├── profiles.yml                 # Connection configuration
├── packages.yml                 # dbt package dependencies
├── README.md                    # This file
├── macros/
│   ├── get_custom_schema.sql    # Schema naming override
│   └── get_current_timestamp.sql # Utility macro
├── models/
│   ├── staging/                 # Data cleaning layer
│   │   ├── _sources.yml         # Source definitions
│   │   ├── stg_pixar_films.sql
│   │   ├── stg_pixar_people.sql
│   │   ├── stg_genre.sql
│   │   ├── stg_box_office.sql
│   │   ├── stg_public_response.sql
│   │   ├── stg_academy.sql
│   │   └── stg_data_dictionary.sql
│   ├── intermediate/            # Business logic layer
│   │   ├── int_film_financials.sql
│   │   ├── int_film_ratings.sql
│   │   └── int_film_awards.sql
│   └── marts/                   # Analytics layer
│       ├── _schema.yml          # Model documentation & tests
│       ├── dim_films.sql        # Film dimension
│       ├── dim_people.sql       # People dimension
│       ├── fact_film_performance.sql    # Performance facts
│       ├── fact_film_awards.sql         # Awards facts
│       ├── rpt_financial_analysis.sql   # Financial analytics
│       ├── rpt_critical_analysis.sql    # Critical reception
│       ├── rpt_evolution_analysis.sql   # Trend analysis
│       ├── rpt_genre_performance.sql    # Genre insights
│       ├── rpt_people_analysis.sql      # Talent analysis
│       ├── rpt_awards_analysis.sql      # Awards correlation
│       ├── rpt_franchise_analysis.sql   # Franchise performance
│       ├── rpt_seasonal_analysis.sql    # Release timing
│       ├── rpt_competition_analysis.sql # Market positioning
│       ├── rpt_runtime_analysis.sql     # Runtime optimization
│       └── rpt_budget_efficiency.sql    # Budget effectiveness
├── tests/                       # Custom data quality tests
│   ├── assert_positive_box_office.sql
│   ├── assert_score_ranges.sql
│   ├── assert_release_date_logic.sql
│   └── assert_financial_consistency.sql
└── target/                      # Generated artifacts
```

## 🔍 Data Models Explained

### Staging Layer (`PIXAR_DB.STAGING`)

#### **stg_pixar_films**

- **Purpose**: Clean and standardize core film information
- **Key Transformations**: Date parsing, text normalization, sequence numbering
- **Output**: 28 films with validated metadata

#### **stg_pixar_people**

- **Purpose**: Normalize talent and crew information
- **Key Transformations**: Role standardization, name deduplication
- **Output**: 260+ personnel records across all films

#### **stg_genre**

- **Purpose**: Categorize films by genre and content type
- **Key Transformations**: Genre taxonomy standardization
- **Output**: Multi-genre classifications for content analysis

#### **stg_box_office**

- **Purpose**: Financial performance data preparation
- **Key Transformations**: Revenue calculations, ROI derivation
- **Output**: Complete financial metrics with regional breakdowns

#### **stg_public_response**

- **Purpose**: Multi-platform rating aggregation
- **Key Transformations**: Score normalization, platform consistency
- **Output**: Unified critical and audience reception metrics

#### **stg_academy**

- **Purpose**: Awards data standardization
- **Key Transformations**: Category classification, win/loss indicators
- **Output**: Complete Oscar nomination and win history

### Intermediate Layer (`PIXAR_DB.STAGING`)

#### **int_film_financials**

- **Purpose**: Financial performance categorization
- **Business Logic**: ROI tiers, market success categories
- **Key Metrics**: Financial performance classification

#### **int_film_ratings**

- **Purpose**: Critical reception analysis
- **Business Logic**: Cross-platform score averaging, consistency detection
- **Key Metrics**: Composite rating scores and variance analysis

#### **int_film_awards**

- **Purpose**: Awards aggregation and analysis
- **Business Logic**: Major category identification, win rate calculations
- **Key Metrics**: Awards success metrics and prestige scoring

### Marts Layer (`PIXAR_DB.MARTS`)

#### Core Dimensions & Facts

**dim_films**

- Master film registry with comprehensive metadata
- Surrogate keys for referential integrity
- Genre classifications and temporal attributes

**dim_people**

- Talent directory with career span analytics
- Role aggregations and collaboration patterns
- Performance history and achievement tracking

**fact_film_performance**

- Integrated performance metrics combining financial, critical, and awards data
- Primary analytical table for most business intelligence queries
- Comprehensive KPIs and calculated metrics

**fact_film_awards**

- Detailed awards transaction history
- Granular nomination and win tracking
- Category-specific performance analysis

#### Advanced Analytics Reports

**rpt_financial_analysis**

- Market performance with decade comparisons
- Financial tier classifications and ROI analysis
- Budget efficiency and profitability insights

**rpt_critical_analysis**

- Multi-platform reception analysis with score consistency metrics
- Critical reception categories and ranking systems
- Audience vs critic correlation analysis

**rpt_evolution_analysis**

- Year-over-year trends with 3-year moving averages
- Industry evolution tracking across multiple dimensions
- Technology and market impact analysis

**rpt_genre_performance**

- Genre-based success rate analysis
- Content strategy optimization insights
- Market preference evolution tracking

**rpt_people_analysis**

- Director and producer career performance metrics
- Talent ROI and critical success correlation
- Collaboration pattern analysis and career trajectory insights

**rpt_awards_analysis**

- Academy Awards correlation with commercial success
- Awards strategy effectiveness and ROI measurement
- Prestige vs profitability analysis

**rpt_franchise_analysis**

- Franchise portfolio performance and health metrics
- Sequel effectiveness and brand value analysis
- Long-term franchise strategy insights

**rpt_seasonal_analysis**

- Release timing optimization and market window analysis
- Seasonal performance patterns and competitive landscape
- Calendar strategy recommendations

**rpt_competition_analysis**

- Market positioning and competitive benchmarking
- Year-over-year market share analysis
- Industry performance context and relative success metrics

**rpt_runtime_analysis**

- Content length optimization and audience engagement
- Runtime trend analysis and market preferences
- Efficiency metrics for storytelling and engagement

**rpt_budget_efficiency**

- Cost-effectiveness analysis across budget tiers
- Investment optimization and risk-adjusted returns
- Production efficiency benchmarking

## Data Quality & Testing

### Automated Quality Checks

#### Schema Tests

- **Uniqueness**: Primary and foreign key constraints
- **Not Null**: Critical field completeness validation
- **Accepted Values**: Categorical field validation
- **Relationships**: Referential integrity across dimensions

#### Custom Business Logic Tests

```sql
-- Examples of custom tests included:

-- Financial consistency validation
assert_positive_box_office.sql
assert_financial_consistency.sql

-- Score range validation
assert_score_ranges.sql

-- Temporal logic validation
assert_release_date_logic.sql
```

#### dbt Expectations Integration

- Statistical data profiling and anomaly detection
- Distribution analysis and outlier identification
- Data freshness and completeness monitoring

### Test Execution

```bash
# Run all tests
dbt test

# Run specific test categories
dbt test --select tag:financial
dbt test --select tag:critical_scores

# Test specific models
dbt test --select fact_film_performance
```

## Key Business Intelligence Queries

### Executive Dashboard Queries

#### Studio Performance Scorecard

```sql
-- Key Performance Indicators
SELECT
    COUNT(*) as total_films,
    ROUND(AVG(box_office_worldwide), 0) as avg_box_office,
    ROUND(AVG(roi_ratio), 2) as avg_roi,
    ROUND(AVG(rotten_tomatoes_score), 1) as avg_rt_score,
    SUM(box_office_worldwide) as total_revenue,
    COUNT(CASE WHEN financial_performance_category = 'High Success' THEN 1 END) as blockbusters
FROM PIXAR_DB.MARTS.FACT_FILM_PERFORMANCE;
```

#### Top Performing Films

```sql
-- Financial and critical leaders
SELECT
    film_title,
    release_year,
    box_office_worldwide,
    roi_ratio,
    rotten_tomatoes_score,
    financial_performance_category,
    critical_reception
FROM PIXAR_DB.MARTS.FACT_FILM_PERFORMANCE
ORDER BY box_office_worldwide DESC
LIMIT 10;
```

### Operational Analytics

#### Director Performance Analysis

```sql
-- Director success metrics
SELECT
    director_name,
    films_directed,
    avg_box_office,
    avg_rt_score,
    career_span_years,
    total_oscar_wins
FROM PIXAR_DB.MARTS.RPT_PEOPLE_ANALYSIS
WHERE films_directed >= 1
ORDER BY avg_box_office DESC;
```

#### Genre Portfolio Strategy

```sql
-- Genre performance and market opportunities
SELECT
    genre,
    films_count,
    success_rate_pct,
    avg_roi,
    avg_box_office
FROM PIXAR_DB.MARTS.RPT_GENRE_PERFORMANCE
ORDER BY success_rate_pct DESC;
```

### Financial Intelligence

#### Budget Optimization Analysis

```sql
-- ROI by budget tier for investment planning
SELECT
    budget_tier,
    films_count,
    avg_budget,
    avg_roi,
    high_roi_success_rate,
    critical_success_rate
FROM PIXAR_DB.MARTS.RPT_BUDGET_EFFICIENCY
ORDER BY avg_roi DESC;
```

#### Franchise Value Assessment

```sql
-- Franchise portfolio performance
SELECT
    franchise,
    total_franchise_revenue,
    total_films,
    franchise_trajectory,
    sequel_performance_pct
FROM PIXAR_DB.MARTS.RPT_FRANCHISE_ANALYSIS
WHERE franchise != 'Standalone'
ORDER BY total_franchise_revenue DESC;
```

## Advanced Features & Extensions

### Machine Learning Integration

The pipeline includes ML-ready feature tables:

```sql
SELECT
    film_title,
    release_year,
    runtime_minutes,
    budget,
    director_experience,
    is_sequel,
    is_adventure,
    is_comedy,
    is_financial_success,
    is_critical_success,
    is_blockbuster
FROM PIXAR_DB.MARTS.ML_FEATURES_FILM_SUCCESS;
```
