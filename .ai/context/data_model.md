# Data Model
## Dimensions

* dim_product(product_id, name, category_id, price, is_current, start_date, end_date)
* dim_category(category_id, category_name)
* dim_customer(customer_id, name, location)
* dim_store(store_id, location)
* dim_date(date_id, date, month, year)

## Facts

* fact_sales(order_id, product_id, customer_id, store_id, date_id, quantity, amount)
* fact_inventory(product_id, store_id, stock)
* fact_returns(product_id, customer_id, return_date, reason)

## DLT Curated Outputs

* silver_orders_dlt(order_id, customer_id, product_id, quantity, price, source_file_name, ingest_ts)
* dim_product_current_dlt(product_sk, product_id, product_name, category_id, current_price, start_date, end_date, is_current)
* dim_customer_dlt(customer_id, customer_name, customer_segment, customer_status)
* dim_date_dlt(date_id, full_date, day, month, year, week_of_year)
* silver_products_dlt(product_id, product_name, category_id, price)
* fact_sales_dlt(order_id, product_sk, customer_id, date_id, quantity, price, sales_amount, order_ts)

## DLT Quarantine Outputs

* silver_orders_quarantine(order_id, customer_id, product_id, quantity, price, source_file_name, ingest_ts, _rescued_data, dq_reason)
* fact_sales_quarantine(order_id, customer_id, product_id, quantity, price, order_ts, dq_reason)

## DLT Modeling Notes

* `*_dlt` tables are the main DLT-managed curated outputs used for validated consumption.
* `*_quarantine` tables store rejected or unresolved rows for remediation and replay workflows.
* In the current implementation, `fact_sales_dlt` only contains rows that successfully resolve all required dimensions.
* Some DLT dimension expectations are soft expectations, which means violations are tracked in quality metrics without automatically dropping the row.
