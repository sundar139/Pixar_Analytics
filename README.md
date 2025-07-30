# Pixar Analytics ELT Pipeline

A comprehensive data transformation pipeline for Pixar film analysis using dbt and Snowflake, providing deep insights into the studio's 29-year journey from 1995 to 2024.

## Project Overview

This ELT pipeline transforms raw Pixar data into actionable business intelligence, covering:

- **Financial Performance**: Budget allocation, box office returns, ROI analysis
- **Critical Reception**: Multi-platform rating aggregation and consistency analysis
- **Awards Recognition**: Academy Awards tracking and correlation analysis
- **Industry Evolution**: Trend analysis across decades and genres
- **Talent Analysis**: Director and producer performance metrics

## Architecture Overview

### ELT Data Flow

```
Raw CSV Files → Extract & Load → Snowflake Raw Tables → Transform (dbt) → Analytics Marts
```

The pipeline follows the modern ELT (Extract, Load, Transform) pattern:

1. **Extract**: Raw CSV files are collected from various data sources
2. **Load**: Data is loaded directly into Snowflake raw tables without transformation
3. **Transform**: All transformations happen within Snowflake using dbt's SQL-based approach

### Layer Structure

- **Raw Layer**: Untransformed CSV data loaded directly into Snowflake (preserves original data structure)
- **Staging Layer**: Data cleaning, standardization, and basic transformations within Snowflake
- **Intermediate Layer**: Business logic implementation and feature engineering using dbt
- **Marts Layer**: Final analytical tables optimized for reporting and analytics

### ELT Architecture Benefits

- **Leverages Snowflake's compute power**: All transformations utilize cloud warehouse processing
- **Data preservation**: Original raw data maintained for auditing and re-processing
- **Flexible transformations**: Schema-on-read approach allows for iterative data modeling
- **Scalable processing**: Cloud-native architecture handles large data volumes efficiently

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

### Step 2: Snowflake Infrastructure Setup

Execute the infrastructure setup scripts in Snowflake Web Interface to create:

- Data warehouse with appropriate sizing for ELT processing
- Database and schema structure (RAW, STAGING, MARTS)
- User roles and permissions for data access
- File formats and internal stages for data loading
- Raw data tables with proper schemas (no transformation at load time)

### Step 3: Data Loading Process (ELT Extract & Load Phase)

1. Upload CSV files to Snowflake internal stage
2. Execute COPY commands to load raw data into tables (no transformations applied)
3. Verify data integrity and completeness of loaded data
4. Raw data is now ready for transformation within the warehouse

### Step 4: dbt Project Configuration

#### A. Initialize Project

```bash
# Initialize project
dbt init pixar_analytics
cd pixar_analytics
```

#### B. Configure Connection Profile

Set up connection details in `~/.dbt/profiles.yml` with your Snowflake credentials

#### C. Install Dependencies

```bash
# Install required packages
dbt deps
```

### Step 5: Execute ELT Pipeline (Transform Phase)

```bash
# Test connection
dbt debug

# Run all transformations within Snowflake
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

## Data Models Overview

### Staging Layer (`PIXAR_DB.STAGING`)

**Core staging models** clean and standardize raw data:

- **stg_pixar_films**: Clean film information with date parsing and text normalization
- **stg_pixar_people**: Normalize talent information with role standardization
- **stg_genre**: Categorize films with standardized genre taxonomy
- **stg_box_office**: Financial data with revenue calculations and ROI derivation
- **stg_public_response**: Multi-platform rating aggregation and score normalization
- **stg_academy**: Awards data with category classification and win/loss indicators

### Intermediate Layer (`PIXAR_DB.STAGING`)

**Business logic implementation**:

- **int_film_financials**: Financial performance categorization and ROI tiers
- **int_film_ratings**: Cross-platform score averaging and consistency analysis
- **int_film_awards**: Awards aggregation with win rate calculations

### Marts Layer (`PIXAR_DB.MARTS`)

#### Core Dimensions & Facts

- **dim_films**: Master film registry with comprehensive metadata
- **dim_people**: Talent directory with career analytics
- **fact_film_performance**: Primary analytical table with integrated performance metrics
- **fact_film_awards**: Detailed awards transaction history

#### Advanced Analytics Reports

- **rpt_financial_analysis**: Market performance and profitability insights
- **rpt_critical_analysis**: Multi-platform reception analysis
- **rpt_evolution_analysis**: Year-over-year trends and industry evolution
- **rpt_genre_performance**: Genre-based success analysis
- **rpt_people_analysis**: Director and producer performance metrics
- **rpt_awards_analysis**: Academy Awards correlation analysis
- **rpt_franchise_analysis**: Franchise portfolio performance
- **rpt_seasonal_analysis**: Release timing optimization
- **rpt_competition_analysis**: Market positioning analysis
- **rpt_runtime_analysis**: Content length optimization
- **rpt_budget_efficiency**: Cost-effectiveness analysis

## Data Quality & Testing

### Automated Quality Framework

The pipeline includes comprehensive testing across multiple dimensions:

#### Schema Tests

- Primary and foreign key uniqueness validation
- Critical field completeness checks
- Categorical field value validation
- Cross-table referential integrity

#### Custom Business Logic Tests

- Financial consistency validation across related tables
- Score range validation for rating platforms
- Temporal logic validation for release dates
- Revenue calculation accuracy checks

#### Statistical Data Quality

- Distribution analysis and outlier detection
- Data freshness monitoring
- Completeness trending analysis

### Test Execution Process

```bash
# Run all tests
dbt test

# Run specific test categories
dbt test --select tag:financial
dbt test --select tag:critical_scores

# Test specific models
dbt test --select fact_film_performance
```

## Business Value & Use Cases

### Strategic Decision Support

- **Investment Planning**: Historical ROI patterns guide budget allocation
- **Talent Acquisition**: Performance benchmarking for hiring decisions
- **Genre Strategy**: Market opportunity identification
- **Release Planning**: Optimal timing based on seasonal analysis

### Operational Excellence

- **Performance Benchmarking**: Film success measurement against studio averages
- **Risk Assessment**: Budget vs expected return modeling
- **Competitive Analysis**: Market positioning within animation industry
- **Resource Optimization**: Production efficiency insights

### Marketing Intelligence

- **Audience Segmentation**: Critical vs commercial appeal analysis
- **Awards Strategy**: Oscar campaign ROI measurement
- **Brand Positioning**: Market evolution analysis
- **Content Strategy**: Genre optimization and franchise development

## Troubleshooting Guide

### Common Issues & Solutions

**Schema Naming Problems**: Verify custom schema macro exists and run with full refresh

**Connection Issues**:

```bash
dbt debug
```

Check Snowflake credentials, warehouse status, and role permissions

**Data Quality Failures**:

```bash
dbt test --store-failures
```

Investigate specific test failures and data inconsistencies

**Performance Issues**: Review warehouse sizing, query execution history, and consider optimization strategies
