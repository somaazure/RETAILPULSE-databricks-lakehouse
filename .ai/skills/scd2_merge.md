# Skill: SCD Type 2 Merge

Create a Databricks SCD Type 2 implementation plan for `retailpulse.gold.dim_product` using `retailpulse.silver.products` as the source.

## Objective

Maintain `retailpulse.gold.dim_product` as a historical product dimension where changes to product attributes create new versions instead of overwriting existing rows.

## Source And Target

- Source table: `retailpulse.silver.products`
- Target table: `retailpulse.gold.dim_product`
- Business key: `product_id`
- Tracked attributes: `product_name`, `category_id`, `price`

## SCD2 Target Design

The dimension should contain:

- `product_sk` bigint generated as identity
- `product_id` int
- `product_name` string
- `category_id` int
- `current_price` decimal(12,2)
- `start_date` timestamp
- `end_date` timestamp
- `is_current` boolean

## Implementation Plan

1. Create the target catalog and schema.
   - Ensure `retailpulse.gold` exists before the merge job runs.

2. Create the SCD2 dimension table.
   - Build `retailpulse.gold.dim_product` as a Delta table with a surrogate key and SCD2 tracking columns.

3. Read and standardize the source snapshot.
   - Load `retailpulse.silver.products`.
   - Cast `product_id`, `category_id`, and `price` into the expected target types.
   - Remove rows with null business keys.

4. Prepare the staged source dataset.
   - Add `start_date` using the pipeline run timestamp.
   - Rename `price` to `current_price` for clarity in the dimension model.

5. Detect changes against the current target rows.
   - Match on `product_id` where `is_current = true`.
   - Treat a row as changed if any tracked attribute differs:
     - `product_name`
     - `category_id`
     - `current_price`

6. Expire old current rows.
   - For matched current rows with attribute changes, set:
     - `end_date = source.start_date`
     - `is_current = false`

7. Insert new dimension versions.
   - Insert rows for:
     - brand new `product_id` values
     - changed products that need a new current version
   - New rows should have:
     - `start_date = source.start_date`
     - `end_date = TIMESTAMP('9999-12-31 00:00:00')`
     - `is_current = true`

8. Validate the dimension after load.
   - Confirm only one current row per `product_id`.
   - Confirm changed products have both expired and current versions.
   - Confirm unchanged products were not duplicated.

## Best Practices

- During the initial load, align `effective_start_ts` with historical fact timing when possible.
- Improper timestamp alignment can cause fact-to-dimension join mismatches in historical SCD joins.

## Recommended Setup SQL

```sql
CREATE CATALOG IF NOT EXISTS retailpulse;
CREATE SCHEMA IF NOT EXISTS retailpulse.gold;

CREATE TABLE IF NOT EXISTS retailpulse.gold.dim_product (
  product_sk BIGINT GENERATED ALWAYS AS IDENTITY,
  product_id INT,
  product_name STRING,
  category_id INT,
  current_price DECIMAL(12,2),
  start_date TIMESTAMP,
  end_date TIMESTAMP,
  is_current BOOLEAN
)
USING DELTA;
```

## Recommended Source Preparation

```python
from pyspark.sql import functions as F

source_df = (
    spark.table("retailpulse.silver.products")
    .filter(F.col("product_id").isNotNull())
    .select(
        F.col("product_id").cast("int").alias("product_id"),
        F.col("product_name").cast("string").alias("product_name"),
        F.col("category_id").cast("int").alias("category_id"),
        F.col("price").cast("decimal(12,2)").alias("current_price"),
        F.current_timestamp().alias("start_date"),
    )
)

source_df.createOrReplaceTempView("stg_dim_product")
```

## Recommended Expire Step

```sql
MERGE INTO retailpulse.gold.dim_product AS target
USING stg_dim_product AS source
ON target.product_id = source.product_id
AND target.is_current = true
WHEN MATCHED AND (
  target.product_name <> source.product_name
  OR target.category_id <> source.category_id
  OR target.current_price <> source.current_price
) THEN
  UPDATE SET
    target.end_date = source.start_date,
    target.is_current = false;
```

## Recommended Insert Step

```sql
INSERT INTO retailpulse.gold.dim_product (
  product_id,
  product_name,
  category_id,
  current_price,
  start_date,
  end_date,
  is_current
)
SELECT
  source.product_id,
  source.product_name,
  source.category_id,
  source.current_price,
  source.start_date,
  TIMESTAMP('9999-12-31 00:00:00'),
  true
FROM stg_dim_product AS source
LEFT JOIN retailpulse.gold.dim_product AS target
  ON target.product_id = source.product_id
  AND target.is_current = true
WHERE target.product_id IS NULL
   OR target.product_name <> source.product_name
   OR target.category_id <> source.category_id
   OR target.current_price <> source.current_price;
```

## Step-By-Step Execution Plan

### Initial Load

1. Create the catalog, schema, and target table.
2. Load or refresh `retailpulse.silver.products`.
3. Create the staging view `stg_dim_product` from the source snapshot.
4. If `retailpulse.gold.dim_product` is empty, insert all staged rows as current rows.
5. Set:
   - `start_date = current_timestamp()`
   - `end_date = TIMESTAMP('9999-12-31 00:00:00')`
   - `is_current = true`

### Incremental Changes

1. Refresh `retailpulse.silver.products` with the latest product snapshot.
2. Rebuild `stg_dim_product`.
3. Run the expire step to close changed current rows.
4. Run the insert step to add:
   - new products
   - changed product versions
5. Validate that only one current row exists per `product_id`.

## Test Scenarios

1. Initial load
   - Input: a brand new product exists in `retailpulse.silver.products`
   - Expected: one row inserted into `retailpulse.gold.dim_product` with `is_current = true`

2. No change rerun
   - Input: rerun the pipeline with the same `product_id`, `category_id`, and `price`
   - Expected: no additional dimension row is inserted

3. Price change only
   - Input: same `product_id`, same `category_id`, different `price`
   - Expected: current row is expired and a new current row is inserted

4. Category change only
   - Input: same `product_id`, same `price`, different `category_id`
   - Expected: current row is expired and a new current row is inserted

5. Price and category change together
   - Input: same `product_id`, different `price`, different `category_id`
   - Expected: one old row is expired and one new current row is inserted

6. New product added
   - Input: new `product_id` appears in `retailpulse.silver.products`
   - Expected: one new current row is inserted into the dimension

7. Mixed batch with changed and unchanged products
   - Input: one product changes and others stay the same
   - Expected: only changed products generate new SCD2 versions

8. Current row uniqueness check
   - Validation query:
```sql
SELECT product_id, COUNT(*) AS current_count
FROM retailpulse.gold.dim_product
WHERE is_current = true
GROUP BY product_id
HAVING COUNT(*) > 1;
```
   - Expected: no rows returned

9. History chain validation
   - Validation query:
```sql
SELECT product_id, category_id, current_price, start_date, end_date, is_current
FROM retailpulse.gold.dim_product
WHERE product_id = 101
ORDER BY start_date;
```
   - Expected: prior versions are expired and the latest version is current

## Validation Checks

- One and only one row per `product_id` should have `is_current = true`
- New products should create one current row
- Changed products should have one expired row and one current row
- Unchanged products should not generate new versions
