# Project Notes — E-commerce Lakehouse Platform

This document captures the technical decisions, discoveries, debugging lessons, and production considerations behind the Databricks lakehouse project.

It complements the recruiter-facing `README.md`.

## 1. Project Scope

The project was built to demonstrate a realistic Databricks data-engineering workflow rather than a single notebook transformation.

Main goals:

- incremental file ingestion
- Bronze / Silver / Gold medallion architecture
- Spark Structured Streaming
- Delta Lake
- business-aware data quality
- quarantine
- incremental Gold aggregation
- late-arriving data handling
- workflow orchestration
- automated data-quality checks
- retry-safe processing
- production-style debugging
- analytical serving through Databricks AI/BI Dashboards

Final dataset scale:

```text
42.4M+ e-commerce events
```

## 2. Databricks Organization

Catalog:

```text
ecommerce_lakehouse
```

Schemas:

```text
raw
bronze
silver
gold
```

Source Volume:

```text
/Volumes/ecommerce_lakehouse/raw/source_files/
```

Pipeline metadata:

```text
/Volumes/ecommerce_lakehouse/raw/pipeline_metadata/
```

This metadata location contains Auto Loader schema state and Structured Streaming checkpoints.

## 3. Bronze Design

Bronze ingestion uses:

```python
spark.readStream
    .format("cloudFiles")
```

Mental model:

```text
HOW   = cloudFiles / Auto Loader
WHAT  = CSV
WHERE = source path
```

Auto Loader discovers new files.

Structured Streaming executes the incremental pipeline.

The checkpoint remembers query progress.

These are related but separate concepts.

## 4. Bronze Metadata

Added:

```text
_ingested_at
_source_file
```

`_ingested_at` records when data enters Bronze.

`_source_file` preserves source lineage.

Auto Loader's `_rescued_data` column is retained for schema-mismatch investigation.

## 5. Silver Design

Silver reads Bronze directly as a streaming Delta source:

```python
spark.readStream.table(...)
```

Auto Loader is not needed after Bronze because the source is already a Delta table.

Silver performs:

```text
standardization
validation
quality flagging
clean / quarantine split
event_date derivation
```

## 6. Zero-Price Investigation

The first validation rule used:

```text
price <= 0 → invalid
```

This quarantined:

```text
6,822 rows
```

Investigation showed:

```text
price = 0 → 6,822
negative price → 0
```

By event type:

```text
view     → 6,819
cart     → 3
purchase → 0
```

Engineering conclusion:

A zero price was an attribute-quality issue, not necessarily an invalid event.

Final rule:

```text
price < 0  → quarantine
price = 0  → keep + ZERO_PRICE flag
price > 0  → VALID price
```

This preserves useful behavioral analytics while protecting monetary metrics.

## 7. Duplicate Decision

The source does not provide a trustworthy unique `event_id`.

Possible duplicate detection fields include:

```text
event_time
event_type
product_id
category_id
category_code
brand
price
user_id
user_session
```

Technical metadata should not define a business duplicate:

```text
_ingested_at
_source_file
_silver_processed_at
```

Because identical business rows could represent legitimate repeated user actions, the project intentionally avoids blind deduplication.

If the source later supplied a reliable unique event identifier, deterministic deduplication would become straightforward.

## 8. Checkpoint Reset During Development

A checkpoint tracks already processed streaming input.

When Silver validation logic changed, already processed historical rows needed to be replayed.

During early development it was acceptable to:

```text
drop/rebuild Silver outputs
remove Silver checkpoints
reprocess from Bronze
```

Important production lesson:

Checkpoint deletion should **not** be routine.

Production historical reprocessing should use controlled backfills, migrations, or replay strategies.

A code change only requires checkpoint reset when historical input must be intentionally reprocessed and the old checkpoint would otherwise skip it.

## 9. Gold Table Grain

Three Gold tables were designed.

### Daily funnel/event metrics

```text
grain = one row per event_date
```

### Daily revenue metrics

```text
grain = one row per event_date
```

### Product daily performance

```text
grain = one row per event_date + product_id
```

Grain defines what one row represents and therefore defines the natural `MERGE` key.

## 10. Why Gold Cannot Blindly Append

Suppose Gold contains:

```text
2019-10-05 | purchases = 20,000
```

Later data for the same day arrives.

Blind append could create:

```text
2019-10-05 | 20,000
2019-10-05 |    500
```

This violates one-row-per-day grain.

Correct behavior is to recompute the date and update the existing row.

Therefore:

```text
Bronze / Silver → append-style incremental events
Gold            → incremental MERGE / upsert
```

## 11. Why Gold Recomputes Affected Dates

Some Gold metrics use:

```python
countDistinct(...)
```

Distinct counts are not additive.

Example:

```text
Batch 1 users = A, B, C → 3
Batch 2 users = A, D    → 2
```

Adding:

```text
3 + 2 = 5
```

is wrong.

Actual distinct users:

```text
A, B, C, D = 4
```

Therefore Gold:

```text
identifies affected dates
→ re-reads complete Silver rows for those dates
→ recalculates exact aggregates
→ MERGEs the result
```

This also supports late-arriving data.

## 12. `foreachBatch`

Gold uses:

```python
silver_stream.writeStream.foreachBatch(process_gold_batch)
```

For each streaming micro-batch, Spark calls:

```python
process_gold_batch(microbatch_df, batch_id)
```

Inside this function normal batch operations such as Delta `MERGE` can be used.

## 13. Delta MERGE

Concept:

```text
existing key → UPDATE
new key      → INSERT
```

Daily tables use:

```text
event_date
```

Product daily table uses:

```text
event_date + product_id
```

This preserves Gold grain during incremental updates.

## 14. Idempotency

A key production principle is that retries should not create duplicate final results.

Recomputing complete affected dates and using Delta `MERGE` gives a retry-safe final state.

This is preferable to blind append when `foreachBatch` logic may be retried.

## 15. Event Ratios vs True Conversion Rates

Gold initially contained fields such as:

```text
view_to_cart_rate
cart_to_purchase_rate
view_to_purchase_rate
```

A quality rule originally required each to be between 0 and 1.

It failed.

Examples:

```text
2019-10-01
views      = 1,208,280
carts      = 16,658
purchases  = 19,307
cart_to_purchase_rate = 1.159
```

and:

```text
2019-10-02
views      = 1,154,591
carts      = 17,268
purchases  = 19,469
cart_to_purchase_rate = 1.1275
```

The calculation was correct.

The assumption was wrong.

These values divide independent event counts; they do not guarantee that the same user/session followed a strict:

```text
view → cart → purchase
```

funnel.

Therefore they are better understood as event-count ratios.

A true user/session conversion metric would require a clearly defined cohort, for example:

```text
sessions that viewed and later purchased
----------------------------------------
sessions that viewed
```

Lesson:

Data-quality rules must match business semantics.

## 16. Revenue Semantics

The source does not contain a reliable `order_id`.

Therefore:

```python
avg(price)
```

for purchase events is:

```text
average purchased-item price
```

not:

```text
average order value
```

Do not claim a metric at a grain the source cannot support.

## 17. Data Quality Notebook

The final quality notebook validates persisted tables automatically.

Checks include:

```text
freshness
null keys
grain uniqueness
non-negative metrics
event-ratio sanity
Silver-to-Gold reconciliation
```

This differs from development inspection.

Development may use:

```text
display()
printSchema()
COUNT(*)
sampling
manual profiling
```

Production should prefer automated assertions and monitoring.

## 18. Compute-Cost Trade-Off

Not every quality check should scan full history forever.

At larger scale:

Run frequently:

```text
latest partition freshness
latest partition grain
latest partition reconciliation
critical null checks
```

Run less frequently:

```text
full historical reconciliation
full duplicate scans
large anomaly profiles
```

Engineering principle:

```text
quality assurance
vs
compute cost
```

must be balanced.

## 19. Serverless Cache Failure

The Gold notebook originally reused one intermediate DataFrame across three Gold transformations and added:

```python
.cache()
```

The Lakeflow Job failed with:

```text
[NOT_SUPPORTED_WITH_SERVERLESS]
PERSIST TABLE is not supported on serverless compute.
```

`cache()` internally called `persist()`.

Fix:

```text
remove cache()
remove persist()
remove unpersist()
```

Important lesson:

Spark optimization techniques must be compatible with the actual Databricks compute environment.

## 20. Notebook Execution-State Failure

After editing out `.cache()`, the same stack trace appeared again.

Reason:

The running Python session could still contain the previous function definition if the edited function cell had not been executed again.

Debugging lesson:

```text
edit function
↓
rerun function-definition cell
↓
rerun dependent cells
```

or during debugging:

```text
Run all from top
```

A Job already waiting to retry may still be executing an old run, so cancelling it and launching a fresh run can be appropriate after a code fix.

## 21. Lakeflow Job

Final DAG:

```text
bronze_ingestion
      ↓
silver_processing
      ↓
gold_analytics
      ↓
data_quality_checks
```

The workflow demonstrates:

```text
task dependencies
retry behavior
failure propagation
checkpointed incremental execution
```

Quality checks use deterministic assertions and therefore do not benefit from blind retry when the underlying data itself is invalid.


## 22. Databricks AI/BI Dashboard

After the core pipeline and Lakeflow Job were working, an interactive **Databricks AI/BI Dashboard** was built directly on the curated Gold tables.

Dashboard sources:

```text
ecommerce_lakehouse.gold.daily_funnel_metrics
ecommerce_lakehouse.gold.daily_revenue_metrics
ecommerce_lakehouse.gold.product_daily_performance
```

This preserves the intended serving architecture:

```text
Bronze   → raw / replayable data
Silver   → trusted granular events
Gold     → business-ready analytical datasets
Dashboard → consumption / visualization
```

The dashboard does not query Bronze or Silver directly.

### Overview metrics

The Overview page includes:

```text
Total Events
Cart Events
Purchase Events
Purchased-Item Revenue
Daily Event Volume
Daily Carts & Purchases
```

An interactive `event_date` range filter allows analysis over a selected period.

### Product analysis

The product-level Gold dataset supports rankings such as:

```text
Top products by purchased-item revenue
Top products by purchases
Top products by views
```

### Dashboard metric semantics

The dashboard keeps business labels aligned with the actual source grain.

For example:

```text
Purchased-Item Revenue
```

is preferred over an unsupported order-level metric because the source does not contain a reliable `order_id`.

Event-count ratios are also not presented as strict user/session conversion probabilities.

### Why use Gold as the dashboard source?

Gold tables provide:

```text
stable grain
curated business metrics
smaller analytical datasets
consistent semantics
lower query complexity for visualization
```

This keeps BI consumers away from raw ingestion details and Silver validation logic.

A portfolio screenshot is stored as:

```text
assets/gold_dashboard.png
```

## 23. Incremental Day-5 Validation

Before new input:

```text
Bronze = 4,980,066
Silver = 4,980,066
```

After adding one new daily file:

```text
Bronze = 6,310,405
Silver = 6,310,405
```

New events:

```text
1,330,339
```

Gold for the new date:

```text
2019-10-05
total_events = 1,330,339
```

Gold grain check:

```text
2019-10-01 → 1 row
2019-10-02 → 1 row
2019-10-03 → 1 row
2019-10-04 → 1 row
2019-10-05 → 1 row
```

This proved:

```text
Auto Loader incremental discovery
Bronze incremental append
Silver incremental propagation
Gold MERGE behavior
Gold grain preservation
```

## 24. Important Interview Wording

Prefer:

> Bronze and Silver process new event records incrementally, while Gold uses affected-date recomputation and Delta MERGE because aggregate keys may need updates.

Avoid:

> Everything appends.

Prefer:

> The source lacked a reliable event ID, so I avoided unsafe deduplication.

Avoid:

> I forgot to remove duplicates.

Prefer:

> Zero price was treated as a quality attribute rather than automatically invalidating the full behavioral event.

Avoid:

> Bad rows were ignored.

Prefer:

> The Gold event ratios are not strict cohort conversion probabilities.

Avoid:

> Purchases greater than carts means the pipeline is wrong.

## 25. Skills Demonstrated

```text
Databricks
Unity Catalog
Volumes
PySpark
Spark DataFrames
lazy evaluation
Structured Streaming
Auto Loader
cloudFiles
checkpointing
availableNow
Delta Lake
Delta MERGE
foreachBatch
upsert
idempotency
late-arriving data
medallion architecture
Bronze / Silver / Gold
data lineage
schema rescue
data validation
quarantine
quality flags
table grain
reconciliation
freshness
Lakeflow Jobs
task dependencies
retry handling
failure propagation
serverless compute
production debugging
compute-cost awareness
Databricks AI/BI Dashboards
Gold serving layer
interactive analytical filtering
```

## 26. Repository Presentation

Recommended:

```text
README.md
project-notes.md
notebooks/
assets/
  lakehouse_architecture.png
  lakeflow_job_dag.png
  gold_dashboard.png
.gitignore
LICENSE
```

Do not commit:

```text
42M+ row dataset
CSV files
checkpoints
credentials
Databricks secrets
development stack traces
temporary notebook exports
.ipynb_checkpoints/
```

## 27. Suggested `.gitignore`

```gitignore
# Dataset / generated data
data/
*.csv
*.parquet

# Databricks exports
*.dbc

# Notebook temp files
.ipynb_checkpoints/

# Python
__pycache__/
*.pyc
.venv/
venv/

# Environment / secrets
.env
.env.*
*.pem
*.key

# OS
.DS_Store
Thumbs.db
```

## 28. Final Architecture Summary

```text
42.4M+ Daily E-commerce Events
            ↓
Databricks Volume
            ↓
Auto Loader
            ↓
Structured Streaming
            ↓
Bronze Delta
            ↓
Silver Validation + Quarantine
            ↓
Gold affected-date recomputation
            ↓
foreachBatch + Delta MERGE
            ├────────→ Databricks AI/BI Dashboard
            │          Business analytics / consumption
            ↓
Automated Data Quality

Lakeflow Job orchestrates:
Bronze → Silver → Gold → Data Quality
```

The project demonstrates not only Databricks syntax, but the reasoning required to build, validate, debug, incrementally operate, and serve analytics from a modern lakehouse pipeline.
