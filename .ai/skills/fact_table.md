# Skill: Fact Table Builder

Create PySpark code to build gold-layer fact tables with support for both current and historical SCD dimension join strategies.

## Objective

Build fact tables at the correct transaction grain, resolve dimension keys correctly, and write the output as Delta tables in the gold layer.

## Supported Join Modes

- `current_mode` (default)
  - Join facts to dimensions using the business key and `is_current = true`
  - Use this mode unless a historical SCD join is explicitly requested

- `historical_mode`
  - Join facts to dimensions using the business key and the fact timestamp between `effective_start_ts` and `effective_end_ts`
  - Use this mode when facts must resolve to the historically correct dimension version

## Default Behavior

Use `current_mode` unless the request explicitly asks for historical SCD alignment.

## Requirements

- Use surrogate keys where available
- Maintain correct grain: one row per transaction
- Ensure referential integrity across joined dimensions
- Write output to gold layer Delta tables
- When using DLT, decide explicitly whether referential failures should fail the pipeline or be routed to quarantine

## Join Strategy Guidance

### current_mode

Use when the fact table should resolve to the latest active dimension version.

Pattern:

```python
fact_df.join(
    dim_df.filter("is_current = true"),
    fact_df["business_key"] == dim_df["business_key"],
    "inner",
)
```

### historical_mode

Use when the fact table should resolve to the dimension row that was valid at the time of the event.

Pattern:

```python
fact_df.join(
    dim_df,
    (fact_df["business_key"] == dim_df["business_key"])
    & (fact_df["fact_ts"] >= dim_df["effective_start_ts"])
    & (fact_df["fact_ts"] < dim_df["effective_end_ts"]),
    "inner",
)
```

## Recommended Build Pattern

1. Read the source transaction table at the required grain.
2. Preserve one row per business transaction.
3. Resolve each dimension using the appropriate join mode.
4. Select surrogate keys where available.
5. Keep measures and degenerate transaction identifiers in the fact.
6. Write the result to a gold Delta table.

## Modeling Notes

- Prefer surrogate keys such as `product_sk` when the dimension provides one.
- If a dimension does not yet have a surrogate key, use the natural key temporarily and document the limitation.
- Do not accidentally duplicate fact rows through non-unique dimension joins.
- Validate the final row count against the expected transaction grain.
- For this project's DLT build, use inner joins for curated fact output and a separate left-join quarantine flow for unresolved dimension matches.
- Prefer quarantine tables over `expect_or_fail` for recoverable data quality issues such as missing dimension matches.

## Target Examples

- `retailpulse.gold.fact_sales`
- `retailpulse.gold.fact_inventory`

## Validation Checks

- One output row per source transaction
- No unexpected row multiplication after joins
- Surrogate keys populated where available
- Fact measures such as `quantity`, `price`, and derived amounts are not null when expected
- Output written as Delta in the gold layer

DLT notes for this project:

- Curated DLT fact output: `retailpulse.dlt.fact_sales_dlt`
- Quarantine output: `retailpulse.dlt.fact_sales_quarantine`
- Missing `product_sk`, `customer_id`, or `date_id` should be preserved in quarantine with `dq_reason` rather than aborting the whole pipeline
