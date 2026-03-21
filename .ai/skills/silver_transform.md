# Silver Transform

# Skill: Silver Transformation

Generate cleaning logic:

* Remove duplicates
* Handle nulls
* Apply data type casting
* Filter invalid records
* When using DLT, separate curated output from quarantine output
* Prefer quarantine tables for operationally recoverable bad records instead of silently dropping everything
* If the deduplication strategy uses aggregate-plus-join logic on a persisted Delta source, prefer triggered batch DLT reads over streaming reads

Output: retailpulse.silver tables

DLT notes for this project:

* Curated DLT silver output: `retailpulse.dlt.silver_orders_dlt`
* Quarantine output: `retailpulse.dlt.silver_orders_quarantine`
* Keep `_rescued_data` as an intermediate validation column; do not rely on it in the final table schema expectations
