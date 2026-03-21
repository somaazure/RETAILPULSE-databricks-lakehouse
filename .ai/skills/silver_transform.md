# Silver Transform

# Skill: Silver Transformation

Generate Silver-layer cleaning and curation logic for `retailpulse.silver` tables.

## Core Responsibilities

- Remove duplicates
- Handle nulls
- Apply data type casting
- Filter invalid records
- Standardize raw Bronze records before publishing curated Silver output
- Separate valid records from quarantine handling when data is operationally recoverable

## Preferred Pattern

Use a two-step transformation pattern:

1. `standardized`
   - cast columns
   - normalize strings
   - preserve `_rescued_data` and ingestion metadata
2. `curated`
   - apply business validation
   - deduplicate
   - publish the final Silver table

This standardized to curated pattern keeps validation logic easier to reason about and makes quarantine routing explicit.

## DLT Guidance

When Silver is implemented in DLT:

- use `@dlt.expect(...)` for quality metrics that should be tracked without automatically rejecting rows
- use `@dlt.expect_all(...)` when multiple DQ rules should be defined together on the curated output
- use explicit filtering or separate quarantine tables for invalid or unresolved records
- keep `_rescued_data` available in intermediate standardized logic, not as a required final curated-table column

## Project Pattern

For this project, prefer:

- a `standardized` internal view or DataFrame
- a `curated` Silver table for valid records
- a quarantine table with `dq_reason` for invalid records

## Example DLT Shape

```python
@dlt.view(name="silver_orders_standardized")
def silver_orders_standardized():
    ...

@dlt.table(name="retailpulse.silver.orders")
@dlt.expect_all(
    {
        "valid_order_id": "order_id IS NOT NULL AND TRIM(order_id) <> ''",
        "valid_customer_id": "customer_id IS NOT NULL",
        "valid_product_id": "product_id IS NOT NULL",
    }
)
def silver_orders_curated():
    return dlt.read("silver_orders_standardized").filter(...)

@dlt.table(name="retailpulse.ops.silver_orders_quarantine")
def silver_orders_quarantine():
    return dlt.read("silver_orders_standardized").filter(...)
```

## Operational Notes

- Prefer quarantine tables for recoverable bad records instead of silently dropping everything.
- If the deduplication strategy uses aggregate-plus-join logic on a persisted Delta source, prefer triggered batch DLT reads over streaming reads.
- Keep validation rules close to the curated Silver publication step.

## Target Outputs

- Curated Silver tables in `retailpulse.silver`
- Quarantine tables in `retailpulse.ops` where applicable
