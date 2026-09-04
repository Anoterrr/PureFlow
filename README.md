# PureFlow-Arch: Modern & Modular Data Lakehouse
### Data Engineering Capstone Project (TCC) - Medallion Architecture

**PureFlow-Arch** is a high-performance, **metadata-driven** data engineering platform. It implements a full Medallion Architecture using a 100% open-source stack, featuring a custom **Factory Pattern** for pipeline generation, integrated Data Quality (GX), and a robust DataOps CI/CD lifecycle.

---

## 🏗️ Architecture & Design Principles

The project is built on **Clean Architecture** and **SOLID** principles, ensuring that infrastructure, execution logic, and business rules are strictly decoupled.

### 1. Medallion Layers (S3-Native)
Data evolves through progressive layers in **MinIO (S3)**:
*   **Landing (Raw):** Source files (CSV/JSON) in their original format.
*   **Bronze (Standardized):** Technical ingestion (Schema enforcement) using **DuckDB**.
*   **Silver (Validated & Clean):** High-quality data after **Great Expectations** gates and SQL transformations.
*   **Gold (Business Ready):** Aggregated datasets managed by **dbt**, ready for BI consumption.

### 2. The DataPipeline Factory (Innovation)
Instead of hardcoding every step, the project uses a **Factory Pattern** (`DataPipelineFactory`). This abstraction allows developers to define complex pipelines in Python using a simple, declarative DSL:
*   **Automatic Lineage:** Dependencies are inferred and mapped natively in **Dagster**.
*   **Decoupled SQL:** Business logic is stored in pure `.sql` files, separate from the execution engine.
*   **Dynamic Quality Gates:** Validation rules are injected into the pipeline at runtime.
*   **Zero-touch registration:** `orchestration.py` auto-discovers every module under `src/pipelines/` (via Dagster's `load_assets_from_package_module`) and supplies a default execution date to every asset (via `ExecutionDateResource`) — adding a domain never requires editing `orchestration.py`.

### 3. Adding a New Pipeline
To wire up a new domain (e.g. `products`), only two things are needed:
1.  **SQL transforms** — `src/sql/products/stg_products_bronze.sql` (Landing→Bronze) and `products_silver.sql` (Bronze→Silver).
2.  **Pipeline definition** — `src/pipelines/products.py`, calling `DataPipelineFactory.create_asset(...)` for Bronze and Silver (see `src/pipelines/sales.py` for the pattern: `source`/`target` paths+format, `sql_transform`, optional `source_expectations`/`target_expectations` for GX gates, `depends_on` for lineage).

That's it — drop the file in `src/pipelines/` and it's picked up automatically; no changes to `orchestration.py`. (A Gold-layer dbt model is optional and separate: add a source with `meta.s3_path` to a `dbt/models/**/sources.yml` and a model `.sql` file — `dbt/macros/setup_sources.sql` registers any source with `meta.s3_path` automatically.)

---

## 🛠️ Tech Stack & Ecosystem

*   **Orchestration:** [Dagster](https://dagster.io/) (Asset-Based, Modern Orchestrator)
*   **Storage:** [MinIO](https://min.io/) (High-performance S3-Compatible Storage)
*   **Processing:** [DuckDB](https://duckdb.org/) (The "SQLite for Analytics" - Local-first OLAP)
*   **Quality:** [Great Expectations](https://greatexpectations.io/) (Dynamic Data Validation)
*   **Modeling:** [dbt](https://www.getdbt.com/) (Modular SQL transformations for the Gold Layer)
*   **DataOps:** GitHub Actions + pre-commit (Automated Linting, Security, and Testing)
*   **Code Quality:** [Ruff](https://docs.astral.sh/ruff/) (lint + format) and [Bandit](https://bandit.readthedocs.io/) (security), enforced via pre-commit hooks
*   **Environment:** Docker & [uv](https://docs.astral.sh/uv/) (Python 3.12)

---

## 📂 Project Structure

```text
PureFlow-Arch/
├── .github/workflows/      # CI/CD DataOps Pipelines
├── dbt/                    # dbt Project (Gold Layer Models)
├── src/
│   ├── core/               # Shared Engine, Factory, Connection & Quality logic
│   ├── pipelines/          # Declarative Pipeline Definitions (Sales, Customers)
│   ├── sql/                # Pure SQL Transformation Logic (Separated from code)
│   ├── validation/         # Generic GX Validation Wrapper
│   └── orchestration.py    # Dagster Entrypoint & UI Definitions
├── tests/                  # Unit tests for core logic and generators
└── pyproject.toml          # Centralized Project Metadata & Config
```

---

## 🚀 Getting Started

### 1. Prerequisites
*   Docker & Docker Compose
*   [uv](https://docs.astral.sh/uv/getting-started/installation/) (optional — only needed for local development outside Docker, e.g. running pre-commit hooks)

### 2. Configure Environment
Copy the example environment file (required — MinIO will fail to start without it):
```bash
cp .env.example .env
```

### 3. Launch the Platform
```bash
docker-compose up -d --build
```

> **Note (Apple Silicon / low-RAM machines):** the DuckDB resource limits (`DUCKDB_MEMORY_LIMIT`, `DUCKDB_THREADS` in `.env`) default to conservative values. If you have more RAM allocated to Docker Desktop, feel free to raise them.

> **Note (Security):** all published ports are bound to `127.0.0.1` — the stack is reachable only from the host machine, not from your local network, even though the default MinIO credentials are weak (fine for local dev, never expose these ports beyond localhost).

### 4. Monitoring & Access
| Tool | Endpoint | Description |
| :--- | :--- | :--- |
| **Dagster UI** | [http://localhost:3000](http://localhost:3000) | Pipeline Lineage & Execution |
| **Streamlit** | [http://localhost:8501](http://localhost:8501) | Business Insights Dashboard |
| **dbt Docs** | [http://localhost:8081](http://localhost:8081) | Data Documentation & Catalog |
| **GX Reports** | [http://localhost:8082](http://localhost:8082) | Data Quality HTML Reports |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | S3 Object Browser |

---

## 📂 Documentation & Visuals

The project architecture and interface previews:

### 🏗️ Architecture Diagram
![PureFlow Architecture](docs/pureflow_architecture.png)
*High-level overview of the Medallion flow and technology stack.*

### 🚀 Dagster UI (Orchestration)
![Dagster UI](docs/dagster_ui.png)
*Preview of the asset-based orchestration, lineage, and Metadata Plots.*

### 🧪 Data Quality Reports (GX)
![GX Reports](docs/gx_report_ui.png)
*Detailed HTML reports generated by Great Expectations, served via HTTP.*

### 📖 dbt Documentation
![dbt Docs](docs/dbt_docs_ui.png)
*Model lineage and documentation generated by dbt.*

### 📦 MinIO Console (S3 Storage)
![MinIO Console](docs/minio_ui.png)
*S3-compatible storage browser showing the medallion buckets.*

---

## 💻 Local Development (Optional)

For editing code outside Docker (IDE support, running the quality hooks before you commit):
```bash
uv sync                # installs deps into .venv, pinned via uv.lock
uv run pre-commit install   # activates the git hook (runs automatically on every commit)
uv run pytest tests/
```

---

## 🛡️ DataOps & Quality Control

The project implements a mandatory **Quality Gate** before any data reaches the Silver/Gold layers. Locally, this gate runs as a **pre-commit** hook (`uv run pre-commit run --all-files` to run it on demand); in CI, the exact same hooks run via GitHub Actions so there's zero drift between what you see locally and what blocks a PR.
1.  **Linting & Formatting:** `ruff` (lint + format) and `sqlfluff` ensure Python and SQL standards.
2.  **Security:** `bandit` scans for vulnerabilities in the code.
3.  **Validation:** Every asset is checked by **Great Expectations**.
    *   **Observations:** Failed validations are recorded as `AssetObservation` in Dagster, preserving the report link even on failure.
    *   **Plots:** Numerical validation scores enable historical tracking via Dagster's Metadata Plots.
4.  **Testing:** `pytest` validates the underlying generators and core utilities.

---

## 📏 Scope & Known Limitations

Deliberate design boundaries — not bugs, but worth stating explicitly (e.g. for the capstone defense):
*   **Single-node processing:** DuckDB is an embedded, single-machine OLAP engine (not distributed). Fine for the dataset sizes this project targets; a production-scale lakehouse would need Spark/Trino-class distributed processing.
*   **Dagster metadata storage:** run/event history is stored in local SQLite (`dagster_home/dagster.yaml`), which is appropriate for a single dev/demo instance but not for multi-user or HA deployments (Dagster supports Postgres for that).
*   **GX validation loads data into Pandas:** `validate_data()` (`src/validation/gx_validator.py`) pulls the full batch into memory via DuckDB → Pandas before validating. Works well at this project's scale (~1M rows with the default 4GB DuckDB memory limit); a larger dataset would need GX's SQL-native execution engine instead of the Pandas one.

---
*Developed as a Modular Data Platform for Senior Engineering Capstone.*
