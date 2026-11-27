# Medallion Data Pipeline — REST API ELT to a Delta Lake Lakehouse

[Ler em português](README.md)

[![CI](https://github.com/lsergiotm/medallion-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/lsergiotm/medallion-data-pipeline/actions/workflows/ci.yml)

ELT pipeline that consumes a REST e-commerce API and populates a **Medallion (Bronze/Silver/Gold)** lakehouse using **PySpark + Delta Lake**, with idempotent incremental loads (MERGE), data quality gates and a BI-ready Gold layer.

---

## Problem Context

A retail company receives order and catalog data from a marketplace partner through a **paginated REST API**. The BI team needs that data consolidated, reliable and historical — but querying the API directly from dashboards is unfeasible: slow, no history, no deduplication, no quality guarantees.

The pipeline's job: **turn a fragile integration into a trustworthy lakehouse** — Bronze ("as-is"), Silver (clean, deduplicated) and Gold (consumption-ready) — the same pattern used on real Fabric/Databricks environments.

## Objective

1. Extract from the API with correct pagination and audit metadata (batch, timestamp).
2. Guarantee **idempotency**: reprocessing a batch never duplicates data (MERGE by key).
3. Standardize and type the data (API camelCase -> analytical snake_case).
4. Validate quality through explicit rules (keys, nulls, values, referential integrity).
5. Publish Gold aggregates (sales by category, general KPIs) ready for Power BI/SQL.

## Data Source

**DummyJSON** (`https://dummyjson.com`) — stable public demo API, no authentication.

- Endpoints: `/products`, `/carts`, `/users` — standard `limit`/`skip` pagination with `total`.
- Simulated business scenario: the marketplace "partner data" to consolidate into the retailer's lakehouse.
- No confidential data. The API is trivially replaceable by an internal one (set `API_BASE_URL` in `src/config.py`).

## Architecture / Methodology

```mermaid
flowchart TB
    subgraph EXTRACT["Extract (Python + requests)"]
        A[DummyJSON REST API\nproducts | carts | users\nlimit/skip pagination]
    end
    subgraph LAKEHOUSE["Delta Lake Lakehouse (data/lakehouse)"]
        B[(BRONZE\nraw JSON payload + audit\nmetadata - immutable append)]
        C[(SILVER\ntyped, snake_case,\nMERGE-deduplicated)]
        D[(GOLD\ndim_produto, fato_pedido_item,\nsales/KPI aggregates)]
    end
    E{{Quality Gate\nquality rules\n-> quality_report.json}}
    F[BI: Power BI / SQL Analytics]

    A --> B --> C --> D --> F
    C --> E
    D --> E
    E --> |critical failure: abort| X((Pipeline stops))
    E --> |ok| F
```

| Layer | Format | Role | Write pattern |
|---|---|---|---|
| **Bronze** | Delta | Immutable "as-is" history (raw JSON + `record_id`, `batch_id`, `ingested_at`) | Append-only |
| **Silver** | Delta | Trustworthy analytical data: typed, `snake_case`, deduplicated | **MERGE by key** — idempotent |
| **Gold** | Delta | Consumption model: dimensions, facts, KPIs | Overwrite (rebuilt per run) |

**Incremental strategy:** each run MERGEs only new/changed records into Silver (payload-hash comparison — `updated_at` moves only on real change); Bronze accumulates batch history for traceability. Running the pipeline twice leaves Silver/Gold identical — the same MERGE semantics used in Fabric/Databricks pipelines.

## Technologies

- **Python 3.11 + PySpark 3.5**, **Delta Lake (delta-spark 3.2)**, **requests**, **Spark SQL**

Detailed diagrams, data contracts and design decisions: [`docs/arquitetura.md`](docs/arquitetura.md).

## Data Preparation (Bronze -> Silver)

| Aspect | Decision |
|---|---|
| Raw payload | Bronze keeps the original JSON (**time travel**, reprocessing without new API calls) |
| Typing | `from_json` with an explicit schema per endpoint — type errors become auditable nulls, never silent failures |
| Naming | API camelCase -> analytical snake_case |
| Deduplication | MERGE by `id`: insert new, update only when the record hash changes |
| Auditability | `record_id` (deterministic hash), `batch_id`, `ingested_at` (Bronze), `updated_at` (Silver) |
| Keys | one `id` per entity; `fato_pedido_item` links orders and products (array explode) |

## Gold Layer

| Table | Contents |
|---|---|
| `gold.dim_produto` | Standardized catalog: id, title, brand, category, price, rating, stock |
| `gold.fato_pedido_item` | One row per order item: order, customer, product, quantity, revenue |
| `gold.agg_vendas_categoria` | Revenue, items, orders and average rating by category |
| `gold.agg_kpis_gerais` | Total revenue, AOV, #orders, #products, #customers |

## Metrics

- Revenue (sum of `quantity x price`), average order value, coverage of orders/products/customers.
- Revenue and rating by category (catalog prioritization).
- Pipeline metrics: records per batch, new vs. updated in the MERGE, quality rules OK/failed per run.

## Results

Real local run (Python 3.11, PySpark 3.5.1, Delta 3.2, Java 11) — reproduce with `python -m src.run_pipeline --source all`:

| Layer | Result |
|---|---|
| Bronze | 610 records ingested in the batch (products 194, carts 208, users 208) with raw payload + `batch_id`/`ingested_at` |
| Silver | 194 + 208 + 208 typed, deduplicated records via MERGE |
| Gold | `dim_produto` 194 · `fato_pedido_item` 800 · `agg_vendas_categoria` 24 · `agg_kpis_gerais` 1 |
| Quality gate | 10/10 rules passing (0 critical) — `data/lakehouse/quality/quality_report.json` |

**Proven idempotency:** a second run (`--incremental`) reprocessed the same 610 records and wrote **0 changes** to Silver (`novos_ou_alterados: 0` across all three sources), with Gold rebuilt identically.

**KPIs in Gold after the load (API demo data):**

| KPI | Value |
|---|---|
| Total revenue (ordered items) | 3,834,278.63 |
| Average order value | 18,434.03 |
| Orders / customers | 208 / 208 |
| Distinct products ordered | 189 (out of 194 in the catalog) |

Top categories by revenue: `vehicle` (1.88M), `motorcycle` (670k) and `mens-watches` (645k) — full ranking in `agg_vendas_categoria`.

## Business Insights (sample Gold readings)

1. **Leading categories** by revenue and rating guide homepage placement.
2. **AOV and discount depth** per order show whether promotions concentrate revenue or erode margin.
3. **Ordered products missing from the catalog** would break fact-x-dim integrity — the quality gate blocks them, signaling API/pipeline desynchronization.
4. **Zero-damage reprocessing:** batches can be rerun for recovery without double-counting revenue — a board-reporting reliability requirement.

## Diagrams / Screenshots

- Full architecture: Mermaid above + [`docs/arquitetura.md`](docs/arquitetura.md).

> **Screenshot (placeholder):** replace with a real print of the Delta tables in your lakehouse (Databricks Catalog / Fabric Lakehouse Explorer).

## How to Run

Prerequisites: **Python 3.11** and **Java 11/17** (required by Spark).

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# Full load (all endpoints) or single source
python -m src.run_pipeline --source all
python -m src.run_pipeline --source products --incremental
```

Outputs: lakehouse in `data/lakehouse/{bronze,silver,gold}`, quality report in `data/lakehouse/quality/quality_report.json`.

> **Windows note:** local Spark also needs `winutils.exe` (download the 3.3.x build from [cdarlint/winutils](https://github.com/cdarlint/winutils) and point `HADOOP_HOME` to the folder containing `bin\winutils.exe`). On corporate networks that block the Maven repository, drop the Delta JARs (`delta-spark_2.12` and `delta-storage` for your version) into `jars/` — `src/config.py` injects them via `spark.jars` automatically. Use **Python 3.11**: PySpark 3.5.x does not support 3.12+ yet.

## Next Steps

- Scheduled orchestration (Airflow, Fabric Data Pipeline, Databricks Workflows).
- Unit tests with `pytest` (payload fixture already in `tests/fixtures/`).
- SCD Type 2 on dim_produto (price history).
- Alerting (e-mail/Teams) on quality report failures.
- Fabric deployment with OneLake/Shortcut for direct Power BI consumption.

---

**Author:** [Luís Sérgio Teixeira Mesquita](https://www.linkedin.com/in/luis-sergiotm/) · [github.com/lsergiotm](https://github.com/lsergiotm)
