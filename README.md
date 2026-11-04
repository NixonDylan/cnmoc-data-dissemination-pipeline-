# CNMOC Data Dissemination SBIR Project: High-Velocity ELT Pipeline

An optimized, real-time data engineering pipeline designed to ingest, validate, transform, and store high-velocity marine telemetry streams, delivering data via a CF-1.6 compliant ERDDAP interoperability layer. 

This architecture was built from scratch to demonstrate production-grade data engineering strategies engineered directly against the Commander, Naval Meteorology and Oceanography Command (CNMOC) technical infrastructure.

## Blueprint

1. **Velocity Layer (Ingestion):** Absorbs real-time JSON packets asynchronously from deployed marine assets into an event stream framework.
2. **Compute Layer (Validation & Transformation):** Bypasses Python’s single-threaded CPU limits by utilizing **Polars' multi-threaded Rust execution engine** and lazy evaluation.
3. **Security Gatekeeper (Schema Enforcement):** Enforces strict runtime data boundaries using **Pydantic** to block malformed inputs or impossible coordinates (e.g., GPS drift) before database contact.
4. **Storage Layer (Bulk Ingestion):** Maximizes write performance using `psycopg` to stream byte buffers directly into memory using the native **PostgreSQL `COPY` protocol**, eliminating slow row-by-row `INSERT` statement overhead.
5. **Interoperability Layer (Data Delivery):** Standardizes PostGIS geospatial records into a **CF-1.6 and ACDD-1.3 compliant TimeSeries format** via ERDDAP (`datasets.xml`) for instant downstream operational access.

## Key Engineering Decisions

* **Polars Lazy Evaluation:** Built-in optimization maps an in-memory execution plan before data execution occurs, squeezing maximum performance out of parallel multi-core architectures.
* **Stream-to-Memory Bulk Writing:** Serializes data straight to an unheadered CSV byte stream (`BytesIO`), feeding the PostgreSQL storage engine at near-disk speeds.
* **Enterprise Security First Mindset:** Designed explicitly to support DISA STIG guidelines by validating all inputs at the data boundary layer, isolating read-only ERDDAP users, and maintaining transactional database integrity via automatic context managers.
* **Future Scalability Roadmap:** Includes explicit hooks to transition the post-ingestion spatial synchronization into a native database trigger or generated column, eliminating table scans at production scale.

## Stack & Dependencies
* **Core Processing:** Python 3.11+, Polars, Pydantic v2
* **Database Infrastructure:** PostgreSQL, PostGIS, Psycopg 3
* **Dissemination Engine:** ERDDAP (DataHub/Amundsen adjacent mapping formats)
