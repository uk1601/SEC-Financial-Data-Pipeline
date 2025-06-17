# SECData - Enterprise Financial Data Pipeline

## Project Introduction

**SECData ** is an enterprise-grade **Extract, Load, Transform (ELT)** data pipeline designed to process SEC quarterly financial statement data at scale. This platform transforms complex XBRL filings from 100,000+ public companies into structured, analysis-ready datasets for quantitative finance and fundamental analysis.

### Advanced Data Engineering Concepts Implemented

- **Medallion Architecture (Bronze → Silver → Gold)**: Progressive data refinement with S3 raw storage, dbt staging models, and business fact tables
- **Event-Driven Orchestration**: Apache Airflow DAGs with dynamic task generation and XCom communication
- **Columnar Storage Optimization**: Parquet format with Snappy compression for efficient storage and querying
- **Massively Parallel Processing (MPP)**: Snowflake's elastic compute with auto-scaling warehouse capabilities
- **Dimensional Modeling**: Kimball methodology with star schema fact tables for financial statements
- **Infrastructure as Code (IaC)**: Containerized deployment with Docker orchestration and compose files
- **Multi-Format Data Processing**: Simultaneous CSV, Parquet, and JSON format handling for different use cases
- **Time-Based Data Partitioning**: Hierarchical partitioning by year/quarter/format for optimal query performance
- **Serverless Architecture**: Cloud-native scaling with Google Cloud Run for API services
- **Data Quality Engineering**: Automated constraint validation and duplicate detection using dbt tests
- **Dynamic SQL Generation**: dbt with Jinja templating for parameterized transformations
- **External Stage Integration**: Snowflake external stages with S3 for seamless data loading
- **RESTful API Design**: FastAPI microservices architecture for secure data access
- **Interactive Data Exploration**: Streamlit dashboard with real-time pipeline triggering capabilities

## System Architecture

![Application Workflow Diagram](assets/architecture_diagram.png)

### Technology Stack

| **Layer** | **Technology** | **Purpose** | **Pattern** |
|-----------|----------------|-------------|-------------|
| **Orchestration** | Apache Airflow | DAG-based workflow management | Event-driven scheduling |
| **Transformation** | dbt (Data Build Tool) | SQL-based ELT transformations | Declarative data modeling |
| **Data Warehouse** | Snowflake | MPP columnar analytics | Elastic compute scaling |
| **Data Lake** | AWS S3 | Raw data persistence | Time-based partitioning |
| **API Gateway** | FastAPI | RESTful data access | Microservices architecture |
| **Presentation** | Streamlit | Interactive analytics dashboard | Real-time data visualization |
| **Containerization** | Docker + Docker Compose | Application packaging | Infrastructure as Code |
| **Cloud Platform** | Google Cloud Run | Serverless container hosting | Auto-scaling deployment |

## Data Architecture

### Medallion Architecture Implementation

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SEC Data Bridge - Data Architecture                      │
└─────────────────────────────────────────────────────────────────────────────┘

SEC.gov/DERA          S3 Data Lake         Snowflake DW           Analytics Layer
┌─────────────┐      ┌─────────────────┐   ┌──────────────────┐   ┌─────────────────┐
│             │      │  Bronze Layer   │   │   Silver Layer   │   │   Gold Layer    │
│ Quarterly   │────▶ │                 │──▶│                  │──▶│                 │
│ ZIP Files   │      │ • Raw CSV       │   │ • Staged Models  │   │ • Fact Tables   │
│ 2009-2025   │      │ • Raw Parquet   │   │ • Type Casting   │   │ • Aggregations  │
│             │      │ • Raw JSON      │   │ • Data Cleaning  │   │ • Business KPIs │
│             │      │ • Partitioned   │   │ • Validations    │   │ • Analysis Ready│
└─────────────┘      └─────────────────┘   └──────────────────┘   └─────────────────┘
      │                      │                      │                      │
      ▼                      ▼                      ▼                      ▼
 Web Scraping        Object Storage         Dimensional            RESTful APIs
 + Retry Logic       + Lifecycle Mgmt       Modeling              + Query Interface
```

### Data Flow Patterns

#### **Bronze Layer** (Raw Data Ingestion)
- **Source Systems**: SEC EDGAR database quarterly datasets from 2009-2025
- **Ingestion Pattern**: Batch processing with configurable parallelism across multiple tables
- **Storage Format**: Multi-format support (CSV, Parquet, JSON) with Snappy compression
- **Partitioning Strategy**: Hierarchical by `year/quarter/format/table` for optimal query performance
- **Schema Handling**: Schema-on-read with automatic type inference from raw text files
- **Error Handling**: Retry mechanisms with exponential backoff for failed downloads

#### **Silver Layer** (Staging & Standardization)
- **Transformation Engine**: dbt with Jinja templating for dynamic SQL generation
- **Data Quality Framework**: Automated constraint validation and referential integrity checks
- **Type System**: Robust casting from VARCHAR to appropriate data types with null handling
- **Deduplication Logic**: Primary key constraints and unique validations
- **Audit Trails**: Systematic addition of year/quarter metadata for data lineage
- **Column Standardization**: Consistent naming conventions and data type normalization

#### **Gold Layer** (Business-Ready Analytics)
- **Dimensional Modeling**: Star schema with fact tables for Balance Sheet, Income Statement, and Cash Flow
- **Aggregation Engine**: SUM aggregations grouped by company, period, and financial tags
- **Business Logic**: Financial statement categorization and preferred label mapping
- **Performance Optimization**: Table materialization with company_id and period clustering
- **Data Products**: Domain-specific marts for financial statement analysis

## Data Engineering Implementation

### ELT Pipeline Design Patterns

#### **1. RAW Data Pipeline** (High-Volume Batch Processing)
- **Data Volume**: Processes 16 years of SEC data (2009-2025) with over 120 million data points
- **Storage Efficiency**: Handles 2GB+ ZIP files per quarter with intelligent compression
- **Pattern**: Fan-out → Transform → Fan-in with parallel processing
- **Fault Tolerance**: Automatic retry logic with comprehensive error logging

**Data Flow:**
```
SEC API → ZIP Extraction → Tab-Delimited Parsing → CSV Transformation → 
S3 Landing → Snowflake COPY → Raw Table Creation → Data Validation
```

#### **2. JSON Data Pipeline** (Semi-Structured Processing)
- **Data Format**: Nested JSON with VARIANT columns for flexible querying
- **Enhancement**: Ticker symbol enrichment via SEC reference data integration
- **Serialization**: Company-specific document creation for each submission
- **Parallel Processing**: Task group implementation for concurrent JSON generation

**Data Flow:**
```
SEC Files → Parquet Conversion → Ticker Enrichment → JSON Serialization →
S3 Storage → Snowflake VARIANT Loading → Semi-Structured Querying
```

#### **3. FACT Data Pipeline** (Analytical Processing)
- **Architecture**: Batch processing with dbt orchestration
- **Transformation**: Multi-stage dbt DAG with dependency resolution
- **Output**: Kimball-style dimensional model with proper grain definition
- **Testing**: Comprehensive data quality validation using dbt test framework

**Data Flow:**
```
Raw Tables → dbt Staging → Business Rules → Fact Generation →
Quality Testing → Mart Publication → API Consumption
```

### Advanced Features

#### **Event-Driven Orchestration**
- **Dynamic DAG Generation**: Parameterized workflows with runtime task creation based on year/quarter
- **XCom Communication**: Inter-task metadata propagation for year/quarter parameters
- **Conditional Logic**: Branch operations in JSON pipeline based on S3 file availability
- **Task Groups**: Parallel execution with dependency management for JSON processing

#### **Data Quality Engineering**
- **Duplicate Detection**: Automated duplicate checking across all mart tables
- **Business Rule Validation**: Revenue and liability sign validation for financial metrics
- **Referential Integrity**: Foreign key relationship enforcement between staging tables
- **Completeness Checks**: Not-null constraints on critical business fields

#### **Performance Optimization**
- **Columnar Storage**: Parquet format with predicate pushdown capabilities
- **Time-Based Partitioning**: S3 partitioning by year/quarter for efficient data retrieval
- **Snowflake Clustering**: Multi-column clustering on company_id and period
- **File Format Optimization**: CSV file formats with proper delimiters and null handling

## Data Models

### Source Schema Analysis

| **Entity** | **Description** | **Business Meaning** |
|------------|-----------------|---------------------|
| `SUB` (Submissions) | Company submission metadata | Filing details, company identifiers, fiscal periods |
| `NUM` (Numbers) | Financial metric values | Reported amounts, units of measure, time periods |
| `PRE` (Presentation) | Report structure hierarchy | Statement organization, line numbers, display logic |
| `TAG` (Tags) | XBRL taxonomy definitions | Accounting concepts, data types, financial categories |

### Staging Models (Silver Layer)

**stg_sub** - Company Submission Standardization
- submission_id: Unique identifier for each SEC submission (Primary Key)
- company_id: Central Index Key with NUMBER casting and validation
- company_name: Standardized company name for reporting
- filing_date: DATE conversion from YYYYMMDD format with validation
- fiscal_year: INTEGER casting for proper temporal operations
- fiscal_period: Categorical validation (Q1, Q2, Q3, Q4, FY)

**stg_num** - Financial Facts Normalization
- submission_id: Foreign key relationship to stg_sub
- tag: Financial concept identifier from XBRL taxonomy
- reported_amount: DECIMAL conversion with precision handling for financial calculations
- period_end_date: DATE conversion with business day logic
- unit: Standardized unit of measure codes
- num_quarters_covered: INTEGER for period duration calculations

**stg_pre** - Presentation Hierarchy Flattening
- submission_id: Foreign key relationship to stg_sub
- statement_type: Enumerated values (BS, IS, CF, EQ, CI, SI, UN, CP)
- line: INTEGER conversion for presentation ordering
- directly_reported: BOOLEAN conversion for data source identification
- preferred_label: Human-readable labels for financial concepts

**stg_tag** - Taxonomy Metadata Enrichment
- tag: Financial concept identifier matching stg_num
- custom: BOOLEAN flag for standard vs company-specific tags
- abstract: BOOLEAN indicator for summary vs detail concepts
- data_type: Standardized classification for proper handling
- balance_type: Credit/debit indicator for accounting rules

### Mart Models (Gold Layer)

**balance_sheet** - Statement of Financial Position
- Grain: Unique combination of company_id + period + tag + unit
- Aggregation: SUM of reported_amount for comprehensive totals
- Dimensions: Company name, fiscal year, fiscal period for analysis
- Joins: Four-way join across all staging models for complete context

**income_statement** - Profit & Loss Statement
- Grain: Unique combination of company_id + period + tag + unit
- Aggregation: SUM of reported_amount for income statement items
- Filter: Statement type 'IS' for income statement specific data
- Business Logic: Revenue and expense categorization

**cash_flow** - Statement of Cash Flows
- Grain: Unique combination of company_id + period + tag + unit
- Aggregation: SUM of reported_amount for cash flow activities
- Filter: Statement type 'CF' for cash flow specific data
- Categories: Operating, investing, and financing activities

## Apache Airflow Implementation

### DAG Architecture

**dag_sec_raw_pipeline.py** - Raw Data Processing
- **Web Scraping**: Automated SEC.gov ZIP file downloads with proper User-Agent headers
- **Format Conversion**: Tab-delimited to CSV transformation with encoding handling
- **S3 Upload**: Hierarchical upload with year/quarter/format partitioning structure
- **Snowflake Integration**: External stage creation and COPY command execution
- **Dynamic Tables**: Runtime table creation based on year/quarter parameters
- **Error Handling**: Comprehensive logging and retry mechanisms for failed operations

**dag_sec_json_pipeline.py** - Semi-Structured Processing
- **File Validation**: S3 file existence checking with conditional branch operations
- **Parallel Processing**: Task groups for concurrent JSON document generation
- **Data Enrichment**: Ticker symbol integration from SEC reference data
- **VARIANT Loading**: JSON data loading into Snowflake semi-structured columns
- **Scalability**: Configurable parallelism with multi-worker task distribution

**dag_sec_fact_pipeline.py** - Analytical Processing
- **Raw Data Loading**: Integration with raw pipeline for source data availability
- **dbt Orchestration**: Automated dbt run and test execution
- **Quality Validation**: Comprehensive testing framework with automated failure handling
- **Dependency Management**: Proper task sequencing for data pipeline integrity

### Orchestration Features

- **Dynamic Task Generation**: Runtime creation of processing tasks using Python loops
- **XCom Communication**: Year/quarter parameter passing between upstream and downstream tasks
- **Branch Operations**: Conditional workflow execution based on data availability in JSON pipeline
- **Parameterized Execution**: DAG run configuration for flexible year/quarter processing
- **Comprehensive Logging**: Structured logging with task context and error details

## Streamlit Frontend

### User Interface Components

**Pipeline Execution Interface**
- **Time Period Selection**: Year dropdown (2009-2024) and quarter selection (Q1-Q4)
- **Pipeline Type Selection**: Three processing options (RAW/JSON/FACT)
- **Airflow Integration**: REST API calls to trigger DAG runs with parameter passing
- **Status Monitoring**: Real-time feedback on pipeline execution success/failure

**Data Exploration Dashboard**
- **Availability Verification**: SQL queries against INFORMATION_SCHEMA for table existence
- **Schema Inspection**: Dynamic table structure display based on selected data source
- **Query Interface**: Text area for SQL input with result set display using dataframes
- **Session Management**: Persistent state across page navigation and user interactions

### Technical Implementation

- **Multi-Page Navigation**: Streamlit option menu for clean interface organization
- **API Communication**: HTTP requests to FastAPI backend for data operations
- **Dynamic Content**: Runtime schema generation based on user selections
- **Error Handling**: User-friendly error messages with actionable feedback
- **State Persistence**: Session state management for improved user experience

## FastAPI Backend

### API Architecture

**Microservices Design**
- **RESTful Endpoints**: Clean API design with proper HTTP methods and status codes
- **Snowflake Integration**: SQLAlchemy engine with environment-based connection management
- **Environment Configuration**: Secure credential handling via environment variables
- **Error Handling**: Comprehensive exception handling with appropriate HTTP responses

**Core Endpoints**
- **`/check-availability`**: INFORMATION_SCHEMA queries for data existence verification
- **`/query-data`**: Secure SQL execution with read-only operation filtering
- **Query Validation**: Whitelist-based filtering (SELECT, SHOW, DESCRIBE operations only)

### Security Implementation

- **SQL Injection Prevention**: Query validation and operation type filtering
- **Credential Security**: Environment variable-based configuration management
- **Error Sanitization**: Safe error messages without sensitive information exposure
- **Connection Management**: Proper database connection handling and cleanup

## Cloud Deployment

### Infrastructure Architecture

**Containerization Strategy**
- **Multi-Stage Builds**: Optimized Docker images with separate build and runtime stages
- **Security Hardening**: Non-root user execution and minimal base images
- **Health Checks**: Endpoint monitoring for container orchestration platforms
- **Environment Injection**: Runtime configuration via environment variables

**Google Cloud Run Deployment**
- **Serverless Hosting**: Automatic scaling based on request volume
- **Container Registry**: Image storage and version management
- **Regional Deployment**: Latency optimization through strategic region selection
- **SSL Termination**: Automatic HTTPS with managed certificates

**Snowflake Integration**
- **Role-Based Access**: dbt_role with appropriate warehouse and database permissions
- **Auto-Scaling Warehouse**: X-SMALL warehouse with auto-suspend for cost optimization
- **External Stages**: S3 integration for seamless data loading
- **Connection Pooling**: Efficient database connection management

**AWS S3 Configuration**
- **Hierarchical Storage**: Year/quarter/format partitioning for optimal organization
- **Access Control**: IAM-based permissions for secure data access
- **Multi-Format Storage**: Simultaneous CSV, Parquet, and JSON file storage
- **Integration**: Seamless connectivity with Snowflake external stages

### Performance Characteristics

**Data Scale & Processing**
- **Historical Coverage**: 16 years of SEC data (2009-2025) representing comprehensive market coverage
- **Data Volume**: Over 120 million data points processed across all quarterly filings
- **Storage Efficiency**: 2GB+ compressed ZIP files per quarter with intelligent format optimization
- **Multi-Format Processing**: Simultaneous CSV, Parquet, and JSON generation for different analytical needs

**System Performance**
- **Batch Processing**: Quarterly data ingestion with full historical backfill capability
- **Compression Ratios**: Significant storage optimization through Parquet columnar format
- **Scalable Architecture**: Elastic compute scaling in Snowflake for variable workloads
- **API Response**: Fast query execution through proper indexing and clustering strategies

### Deployment Pipeline

1. **Local Development**: Docker Compose for full-stack development and testing
2. **Image Building**: Multi-stage Docker builds for production optimization
3. **Container Registry**: Google Container Registry for image storage and versioning
4. **Cloud Deployment**: Automated deployment to Google Cloud Run with health checks
5. **Configuration Management**: Environment-based configuration for different deployment stages

## References

- [SEC Financial Statement Data Sets](https://www.sec.gov/dera/data/financial-statement-data-sets)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [dbt Developer Hub](https://docs.getdbt.com/)
- [Snowflake Documentation](https://docs.snowflake.com/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Docker Documentation](https://docs.docker.com/)
- [Google Cloud Run Documentation](https://cloud.google.com/run/docs)
- [AWS S3 Developer Guide](https://docs.aws.amazon.com/s3/)

---

*Built with ❤️ for data engineering pipelines*