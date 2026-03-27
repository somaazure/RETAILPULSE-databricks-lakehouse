# Databricks notebook source
# DBTITLE 1,Check DLT Schema File in Volume
# Step 1: List contents of the checkpoint directory
checkpoint_path = "/Volumes/retailpulse/bronze/orders_files/checkpoints/dlt_orders_schema_business_ts"

print("Contents of checkpoint directory:")
files = dbutils.fs.ls(checkpoint_path)
for file in files:
    print(f"  {file.name} - Size: {file.size} bytes")

# Step 2: Navigate to _schemas subdirectory
schema_path = checkpoint_path + "/_schemas"
print(f"\nContents of _schemas directory:")
schema_files = dbutils.fs.ls(schema_path)
for file in schema_files:
    print(f"  {file.name} - Size: {file.size} bytes")

# Step 3: Read schema files (usually numbered files like 0, 1, 2, etc.)
if schema_files:
    # Try to read the latest schema file (highest number or most recent)
    schema_file_path = schema_files[-1].path  # Last file in the list
    print(f"\nReading schema from: {schema_file_path}")
    
    try:
        # Try reading as JSON text
        schema_content = spark.read.text(schema_file_path).collect()
        print("\nSchema content (JSON):")
        for row in schema_content:
            print(row[0])
    except Exception as e:
        print(f"Could not read as text: {e}")
        
        # Try reading as Parquet
        try:
            print("\nTrying to read as Parquet format...")
            schema_df = spark.read.parquet(schema_file_path)
            print("\nSchema metadata:")
            schema_df.show(truncate=False)
        except Exception as e2:
            print(f"Could not read as Parquet: {e2}")
else:
    print("\nNo schema files found in _schemas directory.")