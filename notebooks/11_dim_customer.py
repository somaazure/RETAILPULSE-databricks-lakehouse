# Databricks notebook source
# DBTITLE 1,Build Customer Dimension
"""Build the customer dimension from curated Silver orders."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

SILVER_ORDERS_TABLE = "retailpulse.silver.orders"
GOLD_DIM_CUSTOMER_TABLE = "retailpulse.gold.dim_customer"

spark.sql("CREATE CATALOG IF NOT EXISTS retailpulse")
spark.sql("CREATE SCHEMA IF NOT EXISTS retailpulse.gold")

dim_customer_df = (
    spark.table(SILVER_ORDERS_TABLE)
    .filter(F.col("customer_id").isNotNull())
    .select(F.col("customer_id").cast("int").alias("customer_id"))
    .dropDuplicates(["customer_id"])
    .withColumn("customer_name", F.concat(F.lit("Customer-"), F.col("customer_id")))
    .withColumn(
        "customer_segment",
        F.when((F.col("customer_id") % 3) == 0, F.lit("Premium"))
        .when((F.col("customer_id") % 3) == 1, F.lit("Standard"))
        .otherwise(F.lit("Basic")),
    )
    .withColumn("customer_status", F.lit("Active"))
)

(
    dim_customer_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DIM_CUSTOMER_TABLE)
)

print(f"Created customer dimension table: {GOLD_DIM_CUSTOMER_TABLE}")
print(f"row count: {dim_customer_df.count()}")

# COMMAND ----------

# DBTITLE 1,ER Diagram Visualization
"""Visualize the RetailPulse ER Diagram with relationship cardinality, including sales fact, in a dark theme."""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

plt.style.use('dark_background')

fig, ax = plt.subplots(figsize=(20, 15))
fig.patch.set_facecolor('#1a1a1a')
ax.set_facecolor('#1a1a1a')
ax.set_xlim(0, 12)
ax.set_ylim(0, 11)
ax.axis('off')

# Table definitions including sales_fact
tables = {
    'bronze.orders': {
        'pos': (0.5, 8.5),
        'width': 2,
        'height': 1.8,
        'fields': ['order_id (PK)', 'customer_id (FK)', 'product_id (FK)', 'quantity', 'price', 'order_timestamp', '_rescued_data', 'source_file_name', 'ingest_ts'],
        'color': '#8B4513',
        'text_color': 'white'
    },
    'silver.orders': {
        'pos': (3.5, 8.5),
        'width': 2,
        'height': 1.6,
        'fields': ['order_id (PK)', 'customer_id (FK)', 'product_id (FK)', 'quantity', 'price', 'order_timestamp', 'source_file_name', 'ingest_ts'],
        'color': '#708090',
        'text_color': 'white'
    },
    'silver.products': {
        'pos': (7, 8.8),
        'width': 1.9,
        'height': 1.1,
        'fields': ['product_id (PK)', 'product_name', 'category_id (FK)', 'price'],
        'color': '#708090',
        'text_color': 'white'
    },
    'gold.dim_customer': {
        'pos': (0.5, 5.2),
        'width': 2,
        'height': 1.2,
        'fields': ['customer_id (PK)', 'customer_name', 'customer_segment', 'customer_status'],
        'color': '#DAA520',
        'text_color': 'black'
    },
    'gold.dim_product': {
        'pos': (3.5, 5.2),
        'width': 2,
        'height': 1.6,
        'fields': ['product_sk (PK)', 'product_id (NK)', 'product_name', 'category_id (FK)', 'current_price', 'start_date', 'end_date', 'is_current'],
        'color': '#DAA520',
        'text_color': 'black'
    },
    'gold.dim_date': {
        'pos': (7, 5.2),
        'width': 1.9,
        'height': 1.2,
        'fields': ['date_id (PK)', 'full_date', 'day', 'month', 'year', 'week_of_year'],
        'color': '#DAA520',
        'text_color': 'black'
    },
    'gold.fact_sales': {
        'pos': (5.2, 2.2),
        'width': 3.1,
        'height': 1.9,
        'fields': ['order_id (PK)','product_sk (FK)','customer_id (FK)','date_id (FK)','quantity','price','sales_amount','order_ts'],
        'color': '#2E8B57',
        'text_color': 'white'
    }
}

# Draw tables
for table_name, details in tables.items():
    x, y = details['pos']
    w, h = details['width'], details['height']
    box = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.05", 
                          edgecolor='cyan', facecolor=details['color'], 
                          linewidth=2.5, alpha=0.9)
    ax.add_patch(box)
    ax.text(x + w/2, y + h - 0.15, table_name, 
            ha='center', va='top', fontsize=11, fontweight='bold',
            color=details['text_color'])
    ax.plot([x + 0.1, x + w - 0.1], [y + h - 0.3, y + h - 0.3], 
            color=details['text_color'], linewidth=1, alpha=0.5)
    field_y = y + h - 0.4
    for field in details['fields']:
        ax.text(x + 0.1, field_y, field, ha='left', va='top', 
                fontsize=8, color=details['text_color'])
        field_y -= 0.15

# Relationship lines (including fact_sales)
relationships = [
    # bronze.orders to dim_customer (M:1)
    {'from': (1.5, 8.5), 'to': (1.5, 6.4), 'label': 'customer_id', 
     'from_card': 'M', 'to_card': '1', 'type': 'fk'},
    # bronze.orders to silver.orders (1:1) ETL
    {'from': (2.5, 9.2), 'to': (3.5, 9.2), 'label': 'ETL flow', 
     'from_card': '1', 'to_card': '1', 'type': 'flow'},
    # silver.orders to dim_customer (M:1)
    {'from': (4, 8.5), 'to': (2, 6.4), 'label': 'customer_id', 
     'from_card': 'M', 'to_card': '1', 'type': 'fk'},
    # silver.orders to dim_product (M:1)
    {'from': (5.5, 8.5), 'to': (4.5, 6.5), 'label': 'product_id', 
     'from_card': 'M', 'to_card': '1', 'type': 'fk'},
    # silver.orders to dim_date (M:1)
    {'from': (5.8, 8.5), 'to': (8, 6.4), 'label': 'order_timestamp', 
     'from_card': 'M', 'to_card': '1', 'type': 'fk'},
    # silver.products to dim_product (1:M) SCD
    {'from': (7.95, 8.8), 'to': (5.3, 6.8), 'label': 'product_id', 
     'from_card': '1', 'to_card': 'M', 'type': 'flow'},
    # SALES FACT RELATIONSHIPS
    # fact_sales to dim_customer (M:1)
    {'from': (6, 2.2), 'to': (1.5, 5.2 + 1.2), 'label': 'customer_id', 
     'from_card': 'M', 'to_card': '1', 'type': 'fk'},
    # fact_sales to dim_product (M:1)
    {'from': (7.1, 2.2), 'to': (4.5, 5.2 + 1.6), 'label': 'product_sk', 
     'from_card': 'M', 'to_card': '1', 'type': 'fk'},
    # fact_sales to dim_date (M:1)
    {'from': (7.7, 2.2), 'to': (8, 5.2 + 1.2), 'label': 'date_id', 
     'from_card': 'M', 'to_card': '1', 'type': 'fk'}
]

for rel in relationships:
    rel_type = rel.get('type', 'fk')
    line_style = '--' if rel_type == 'flow' else '-'
    arrow_color = '#00CED1' if rel_type == 'flow' else '#FF6B6B'
    arrow = FancyArrowPatch(rel['from'], rel['to'],
                           arrowstyle='->', mutation_scale=25, 
                           linewidth=2.5, color=arrow_color,
                           linestyle=line_style, alpha=0.8)
    ax.add_patch(arrow)
    from_x, from_y = rel['from']
    to_x, to_y = rel['to']
    if from_x == to_x:
        from_offset_x, from_offset_y = 0.15, 0.1 if from_y > to_y else -0.1
        to_offset_x, to_offset_y = 0.15, -0.1 if from_y > to_y else 0.1
    else:
        from_offset_x, from_offset_y = 0.1 if from_x < to_x else -0.1, 0.15
        to_offset_x, to_offset_y = -0.1 if from_x < to_x else 0.1, 0.15
    ax.text(from_x + from_offset_x, from_y + from_offset_y, rel['from_card'], 
            ha='center', va='center', fontsize=10, fontweight='bold',
            color='#FFD700', 
            bbox=dict(boxstyle='circle,pad=0.2', facecolor='black', 
                     edgecolor='#FFD700', linewidth=2))
    ax.text(to_x + to_offset_x, to_y + to_offset_y, rel['to_card'], 
            ha='center', va='center', fontsize=10, fontweight='bold',
            color='#FFD700',
            bbox=dict(boxstyle='circle,pad=0.2', facecolor='black', 
                     edgecolor='#FFD700', linewidth=2))
    mid_x = (from_x + to_x) / 2
    mid_y = (from_y + to_y) / 2
    ax.text(mid_x, mid_y, rel['label'], 
            ha='center', va='bottom', fontsize=8, color='white',
            bbox=dict(boxstyle='round,pad=0.3', facecolor='#2a2a2a', 
                     edgecolor='cyan', linewidth=1.5, alpha=0.9))

legend_elements = [
    mpatches.Patch(facecolor='#8B4513', edgecolor='cyan', label='Bronze Layer (Raw)', linewidth=2),
    mpatches.Patch(facecolor='#708090', edgecolor='cyan', label='Silver Layer (Cleansed)', linewidth=2),
    mpatches.Patch(facecolor='#2E8B57', edgecolor='cyan', label='Gold Fact Table (Sales)', linewidth=2),
    mpatches.Patch(facecolor='#DAA520', edgecolor='cyan', label='Gold Dimension Table', linewidth=2),
    mpatches.Patch(facecolor='none', edgecolor='#FF6B6B', label='FK Relationship (Solid)', linewidth=2),
    mpatches.Patch(facecolor='none', edgecolor='#00CED1', label='Data Flow (Dashed)', linewidth=2, linestyle='--')
]

ax.legend(handles=legend_elements, loc='upper right', fontsize=10, 
         facecolor='#2a2a2a', edgecolor='cyan', framealpha=0.9)

ax.text(6, 10.5, 'RetailPulse Project - Entity Relationship Diagram', 
        ha='center', va='top', fontsize=19, fontweight='bold', color='cyan')

notes_text = '''PK = Primary Key | FK = Foreign Key | NK = Natural Key\n1 = One | M = Many\nSolid arrows = Foreign Key relationships | Dashed arrows = Data flow/transformation\n\nRelationship Types:\n• bronze.orders → dim_customer: Many-to-One (M:1)\n• silver.orders → dim_customer: Many-to-One (M:1)\n• silver.orders → dim_product: Many-to-One (M:1)\n• silver.orders → dim_date: Many-to-One (M:1)\n• silver.products → dim_product: One-to-Many (1:M) [SCD Type 2]\n• fact_sales → dim_customer/product/date: Many-to-One (M:1)'''
ax.text(0.5, 1.2, notes_text,
        ha='left', va='bottom', fontsize=9, style='italic', color='white',
        bbox=dict(boxstyle='round,pad=0.5', facecolor='#2a2a2a', 
                 edgecolor='#FFD700', linewidth=2, alpha=0.9))

plt.tight_layout()
display(plt.gcf())
plt.show()

# COMMAND ----------

# Step 1: Mark current version as not current
spark.sql("""
  UPDATE retailpulse.gold.dim_product
  SET end_date = '2024-06-23', is_current = FALSE
  WHERE product_id = 1 AND is_current = TRUE
""")

# Step 2: Insert new version (sk auto-generated by Delta, do not specify product_sk)
spark.sql("""
  INSERT INTO retailpulse.gold.dim_product (product_id, product_name, category_id, current_price, start_date, end_date, is_current)
  VALUES (1, 'Laptop X', 5, 899.99, '2024-06-24', NULL, TRUE)
""")

# Step 3: Validate SCD2 mapping for product_id = 1
result = spark.sql("""
  SELECT product_sk, product_id, product_name, current_price, start_date, end_date, is_current
  FROM retailpulse.gold.dim_product
  WHERE product_id = 1
  ORDER BY start_date
""")
result.show(truncate=False)
print(f"Total versions for product_id=1: {result.count()}")