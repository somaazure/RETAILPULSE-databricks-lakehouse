# Databricks notebook source
# DBTITLE 1,Schema Definition for Gold Tables
"""Extract schema information for retailpulse.gold tables to help the agent generate accurate SQL."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Get all gold tables and their schemas
gold_tables = ['fact_sales', 'dim_customer', 'dim_product', 'dim_date']

schema_info = {}

for table_name in gold_tables:
    full_table = f"retailpulse.gold.{table_name}"
    try:
        df = spark.table(full_table)
        schema_info[table_name] = {
            'full_name': full_table,
            'columns': [(field.name, str(field.dataType)) for field in df.schema.fields]
        }
        print(f"\n{table_name.upper()}:")
        print(f"  Table: {full_table}")
        print(f"  Columns:")
        for col_name, col_type in schema_info[table_name]['columns']:
            print(f"    - {col_name}: {col_type}")
    except Exception as e:
        print(f"Could not load {full_table}: {e}")

# Create a formatted schema description for the LLM
schema_description = "\n\n".join([
    f"Table: {info['full_name']}\nColumns: {', '.join([f'{name} ({dtype})' for name, dtype in info['columns']])}"
    for table, info in schema_info.items()
])

print("\n" + "="*80)
print("SCHEMA DESCRIPTION FOR LLM:")
print("="*80)
print(schema_description)

# COMMAND ----------

# DBTITLE 1,SQL Query Execution Tool
"""Tool function to execute SQL queries and return results."""

import json
from typing import Dict, List, Any

def run_sql_query(sql_query: str, limit: int = 100) -> Dict[str, Any]:
    """
    Execute a SQL query and return results in a structured format.
    
    Args:
        sql_query: SQL query to execute
        limit: Maximum number of rows to return (default: 100)
    
    Returns:
        Dictionary with 'success', 'data', 'columns', 'row_count', and 'error' fields
    """
    try:
        # Execute the query
        result_df = spark.sql(sql_query)
        
        # Limit results
        limited_df = result_df.limit(limit)
        
        # Convert to pandas for easier display
        pandas_df = limited_df.toPandas()
        
        # Get column names
        columns = list(pandas_df.columns)
        
        # Convert to list of dictionaries
        data = pandas_df.to_dict('records')
        
        return {
            'success': True,
            'data': data,
            'columns': columns,
            'row_count': len(data),
            'total_rows': result_df.count() if len(data) < limit else f"{len(data)}+",
            'error': None,
            'sql': sql_query
        }
    except Exception as e:
        return {
            'success': False,
            'data': None,
            'columns': None,
            'row_count': 0,
            'total_rows': 0,
            'error': str(e),
            'sql': sql_query
        }

# Test the tool
test_result = run_sql_query("SELECT * FROM retailpulse.gold.dim_customer LIMIT 5")
print("Test Query Result:")
print(f"  Success: {test_result['success']}")
print(f"  Row Count: {test_result['row_count']}")
print(f"  Columns: {test_result['columns']}")
if test_result['success']:
    print("\nSample Data:")
    for i, row in enumerate(test_result['data'][:3], 1):
        print(f"  Row {i}: {row}")

# COMMAND ----------

# DBTITLE 1,Natural Language to SQL Converter
"""Convert natural language questions to SQL using LLM prompting."""

import os
import re
from typing import Optional

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print("OpenAI library not available. Install with: %pip install openai")

def nl_to_sql(question: str, schema_desc: str) -> str:
    """
    Convert natural language question to SQL query.
    
    Args:
        question: Natural language question
        schema_desc: Database schema description
    
    Returns:
        Generated SQL query
    """
    
    # Prompt template for SQL generation
    prompt = f"""You are an expert SQL query generator for a retail analytics database.

Database Schema:
{schema_desc}

Table Relationships:
- fact_sales.customer_id -> dim_customer.customer_id
- fact_sales.product_sk -> dim_product.product_sk
- fact_sales.date_id -> dim_date.date_id

Instructions:
1. Generate a valid SQL query for the question below
2. Use proper JOINs when referencing multiple tables
3. Return ONLY the SQL query, no explanations
4. Use retailpulse.gold.* for table names
5. Format numbers and dates appropriately

Question: {question}

SQL Query:"""
    
    # Try using OpenAI API if available
    if OPENAI_AVAILABLE:
        try:
            # Check for API key in environment or Databricks secrets
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                try:
                    api_key = dbutils.secrets.get(scope="openai", key="api_key")
                except:
                    pass
            
            if api_key:
                client = OpenAI(api_key=api_key)
                response = client.chat.completions.create(
                    model="gpt-4",
                    messages=[
                        {"role": "system", "content": "You are an expert SQL generator. Return only SQL queries without explanations."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1,
                    max_tokens=500
                )
                sql = response.choices[0].message.content.strip()
                # Clean up the SQL
                sql = re.sub(r'^```sql\n?', '', sql)
                sql = re.sub(r'\n?```$', '', sql)
                return sql.strip()
        except Exception as e:
            print(f"OpenAI API error: {e}. Falling back to rule-based generation.")
    
    # Fallback: Simple rule-based SQL generation
    return generate_rule_based_sql(question)

def generate_rule_based_sql(question: str) -> str:
    """
    Simple rule-based SQL generation for common patterns.
    """
    question_lower = question.lower()
    
    # Pattern: "how many customers"
    if "how many customer" in question_lower:
        return "SELECT COUNT(*) as customer_count FROM retailpulse.gold.dim_customer"
    
    # Pattern: "total sales"
    if "total sales" in question_lower:
        return "SELECT SUM(sales_amount) as total_sales FROM retailpulse.gold.fact_sales"
    
    # Pattern: "top products"
    if "top" in question_lower and "product" in question_lower:
        limit = 10
        match = re.search(r'top (\d+)', question_lower)
        if match:
            limit = int(match.group(1))
        return f"""SELECT p.product_name, SUM(f.sales_amount) as total_sales
FROM retailpulse.gold.fact_sales f
JOIN retailpulse.gold.dim_product p ON f.product_sk = p.product_sk
GROUP BY p.product_name
ORDER BY total_sales DESC
LIMIT {limit}"""
    
    # Pattern: "sales by customer"
    if "sales by customer" in question_lower:
        return """SELECT c.customer_name, SUM(f.sales_amount) as total_sales
FROM retailpulse.gold.fact_sales f
JOIN retailpulse.gold.dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_name
ORDER BY total_sales DESC
LIMIT 20"""
    
    # Pattern: "sales by month" or "monthly sales"
    if ("sales by month" in question_lower) or ("monthly sales" in question_lower):
        return """SELECT d.year, d.month, SUM(f.sales_amount) as total_sales
FROM retailpulse.gold.fact_sales f
JOIN retailpulse.gold.dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month"""
    
    # Default: show sample from fact_sales
    return "SELECT * FROM retailpulse.gold.fact_sales LIMIT 10"

# Test the converter
test_questions = [
    "How many customers do we have?",
    "What are the top 5 products by sales?",
    "Show me monthly sales"
]

print("Testing NL to SQL Converter:\n")
for q in test_questions:
    sql = nl_to_sql(q, schema_description)
    print(f"Question: {q}")
    print(f"SQL: {sql}")
    print("-" * 80)

# COMMAND ----------

# DBTITLE 1,Agent Execution Function
"""Main agent function that orchestrates NL to SQL conversion and execution."""

import pandas as pd
from typing import Dict, Any

class NLtoSQLAgent:
    """
    Natural Language to SQL Agent for RetailPulse Gold tables.
    """
    
    def __init__(self, schema_info: Dict[str, Any], schema_description: str):
        self.schema_info = schema_info
        self.schema_description = schema_description
        self.query_history = []
    
    def ask(self, question: str, execute: bool = True, display_results: bool = True) -> Dict[str, Any]:
        """
        Process a natural language question and optionally execute the generated SQL.
        
        Args:
            question: Natural language question
            execute: Whether to execute the SQL (default: True)
            display_results: Whether to display results as table (default: True)
        
        Returns:
            Dictionary with question, SQL, results, and execution metadata
        """
        print(f"\n{'='*80}")
        print(f"QUESTION: {question}")
        print(f"{'='*80}")
        
        # Step 1: Convert NL to SQL
        print("\n[Step 1] Converting natural language to SQL...")
        sql_query = nl_to_sql(question, self.schema_description)
        print(f"\nGenerated SQL:\n{sql_query}")
        
        result = {
            'question': question,
            'sql': sql_query,
            'executed': False,
            'results': None,
            'error': None
        }
        
        # Step 2: Execute SQL if requested
        if execute:
            print("\n[Step 2] Executing SQL query...")
            query_result = run_sql_query(sql_query)
            result['executed'] = True
            result['results'] = query_result
            
            if query_result['success']:
                print(f"\n✓ Query executed successfully!")
                print(f"  Rows returned: {query_result['row_count']}")
                print(f"  Columns: {', '.join(query_result['columns'])}")
                
                # Display results as table if requested
                if display_results and query_result['data']:
                    print("\n[Results]")
                    df = pd.DataFrame(query_result['data'])
                    display(df)
            else:
                print(f"\n✗ Query execution failed!")
                print(f"  Error: {query_result['error']}")
                result['error'] = query_result['error']
        
        # Add to history
        self.query_history.append(result)
        
        return result
    
    def show_history(self, limit: int = 5):
        """
        Display query history.
        """
        print(f"\nQuery History (last {limit}):")
        for i, entry in enumerate(self.query_history[-limit:], 1):
            print(f"\n{i}. {entry['question']}")
            print(f"   SQL: {entry['sql'][:100]}..." if len(entry['sql']) > 100 else f"   SQL: {entry['sql']}")
            if entry['executed']:
                status = "✓ Success" if entry['results']['success'] else "✗ Failed"
                print(f"   Status: {status}")

# Initialize the agent
agent = NLtoSQLAgent(schema_info, schema_description)

print("\n" + "="*80)
print("NL-to-SQL Agent Initialized!")
print("="*80)
print("\nUsage: agent.ask('your natural language question')")
print("\nExample questions:")
print("  - How many customers do we have?")
print("  - What are the top 10 products by sales?")
print("  - Show me monthly sales trends")
print("  - Which customers have spent the most?")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Example Usage - Demo Questions
"""Demonstrate the NL-to-SQL agent with example questions."""

# Example 1: Count query
print("\n" + "#"*80)
print("# EXAMPLE 1: Simple Count Query")
print("#"*80)
result1 = agent.ask("How many customers do we have?")

# Example 2: Top products
print("\n\n" + "#"*80)
print("# EXAMPLE 2: Top Products by Sales")
print("#"*80)
result2 = agent.ask("What are the top 5 products by sales?")

# Example 3: Sales trends
print("\n\n" + "#"*80)
print("# EXAMPLE 3: Monthly Sales Trends")
print("#"*80)
result3 = agent.ask("Show me monthly sales")

# Example 4: Customer analysis
print("\n\n" + "#"*80)
print("# EXAMPLE 4: Sales by Customer")
print("#"*80)
result4 = agent.ask("Show me sales by customer")

# Example 5: Total sales
print("\n\n" + "#"*80)
print("# EXAMPLE 5: Total Sales Amount")
print("#"*80)
result5 = agent.ask("What is the total sales amount?")

# Show query history
print("\n\n" + "="*80)
print("QUERY HISTORY SUMMARY")
print("="*80)
agent.show_history()

# Summary statistics
print("\n\n" + "="*80)
print("AGENT PERFORMANCE SUMMARY")
print("="*80)
total_queries = len(agent.query_history)
successful_queries = sum(1 for q in agent.query_history if q.get('executed') and q.get('results', {}).get('success', False))
print(f"Total queries: {total_queries}")
print(f"Successful executions: {successful_queries}")
print(f"Success rate: {(successful_queries/total_queries*100):.1f}%" if total_queries > 0 else "N/A")
print("="*80)

print("\n\nAgent ready for interactive use!")
print("Try: agent.ask('your question here')")

# COMMAND ----------

agent.ask('Top Customers by Quantity Purchased')

# COMMAND ----------

agent.ask('Average Order Value')

# COMMAND ----------

# DBTITLE 1,Create Unity Catalog Agent Schema
"""Create Unity Catalog schema for agent functions."""

# Step 1: Create the agent schema in Unity Catalog
spark.sql("CREATE SCHEMA IF NOT EXISTS retailpulse.agent COMMENT 'Natural Language to SQL Agent Functions'")

print("✓ Created schema: retailpulse.agent")
print("\nSchema will contain the following functions:")
print("  - ask_question()        : Main NL-to-SQL agent function")
print("  - get_top_products()    : Get top N products by sales")
print("  - get_customer_sales()  : Get sales by customer")
print("  - get_monthly_trends()  : Get monthly sales trends")
print("  - get_total_sales()     : Get total sales amount")
print("  - get_customer_count()  : Get total customer count")

# Verify schema exists
result = spark.sql("SHOW SCHEMAS IN retailpulse LIKE 'agent'").collect()
if result:
    print("\n✓ Schema verification successful!")
else:
    print("\n✗ Schema creation failed")

# Show existing functions (if any)
print("\nExisting functions in retailpulse.agent:")
try:
    functions = spark.sql("SHOW FUNCTIONS IN retailpulse.agent").collect()
    if functions:
        for func in functions:
            print(f"  - {func.function}")
    else:
        print("  (none yet - will create in next cells)")
except Exception as e:
    print(f"  (none yet - will create in next cells)")

# COMMAND ----------

# DBTITLE 1,Register Pre-built Analytics Functions
"""Register pre-built SQL functions in Unity Catalog for common analytics queries."""

# Set the current catalog
spark.sql("USE CATALOG retailpulse")
print("Set current catalog to: retailpulse\n")

# Function 1: Get top N products by sales (using ROW_NUMBER to avoid LIMIT parameter issue)
spark.sql("""
CREATE OR REPLACE FUNCTION retailpulse.agent.get_top_products(n INT)
RETURNS TABLE(
  product_name STRING,
  total_sales DECIMAL(20,2),
  rank INT
)
COMMENT 'Returns top N products by total sales amount'
RETURN 
  SELECT product_name, total_sales, rank
  FROM (
    SELECT 
      p.product_name, 
      SUM(f.sales_amount) as total_sales,
      ROW_NUMBER() OVER (ORDER BY SUM(f.sales_amount) DESC) as rank
    FROM retailpulse.gold.fact_sales f
    JOIN retailpulse.gold.dim_product p ON f.product_sk = p.product_sk
    GROUP BY p.product_name
  )
  WHERE rank <= n
""")
print("✓ Created function: get_top_products(n)")

# Function 2: Get sales by customer
spark.sql("""
CREATE OR REPLACE FUNCTION retailpulse.agent.get_customer_sales()
RETURNS TABLE(
  customer_name STRING,
  total_sales DECIMAL(20,2),
  order_count BIGINT
)
COMMENT 'Returns sales summary by customer'
RETURN 
  SELECT 
    c.customer_name, 
    SUM(f.sales_amount) as total_sales,
    COUNT(DISTINCT f.order_id) as order_count
  FROM retailpulse.gold.fact_sales f
  JOIN retailpulse.gold.dim_customer c ON f.customer_id = c.customer_id
  GROUP BY c.customer_name
  ORDER BY total_sales DESC
""")
print("✓ Created function: get_customer_sales()")

# Function 3: Get monthly sales trends
spark.sql("""
CREATE OR REPLACE FUNCTION retailpulse.agent.get_monthly_trends()
RETURNS TABLE(
  year INT,
  month INT,
  total_sales DECIMAL(20,2),
  order_count BIGINT
)
COMMENT 'Returns monthly sales trends with order counts'
RETURN 
  SELECT 
    d.year,
    d.month,
    SUM(f.sales_amount) as total_sales,
    COUNT(DISTINCT f.order_id) as order_count
  FROM retailpulse.gold.fact_sales f
  JOIN retailpulse.gold.dim_date d ON f.date_id = d.date_id
  GROUP BY d.year, d.month
  ORDER BY d.year, d.month
""")
print("✓ Created function: get_monthly_trends()")

# Function 4: Get total sales (scalar function)
spark.sql("""
CREATE OR REPLACE FUNCTION retailpulse.agent.get_total_sales()
RETURNS DECIMAL(20,2)
COMMENT 'Returns total sales amount across all transactions'
RETURN 
  SELECT COALESCE(SUM(sales_amount), 0)
  FROM retailpulse.gold.fact_sales
""")
print("✓ Created function: get_total_sales()")

# Function 5: Get customer count (scalar function)
spark.sql("""
CREATE OR REPLACE FUNCTION retailpulse.agent.get_customer_count()
RETURNS BIGINT
COMMENT 'Returns total number of customers'
RETURN 
  SELECT COUNT(*)
  FROM retailpulse.gold.dim_customer
""")
print("✓ Created function: get_customer_count()")

# Function 6: Get sales by customer segment
spark.sql("""
CREATE OR REPLACE FUNCTION retailpulse.agent.get_segment_sales()
RETURNS TABLE(
  customer_segment STRING,
  total_sales DECIMAL(20,2),
  customer_count BIGINT,
  avg_sales_per_customer DECIMAL(20,2)
)
COMMENT 'Returns sales metrics by customer segment'
RETURN 
  SELECT 
    c.customer_segment,
    SUM(f.sales_amount) as total_sales,
    COUNT(DISTINCT c.customer_id) as customer_count,
    SUM(f.sales_amount) / COUNT(DISTINCT c.customer_id) as avg_sales_per_customer
  FROM retailpulse.gold.fact_sales f
  JOIN retailpulse.gold.dim_customer c ON f.customer_id = c.customer_id
  GROUP BY c.customer_segment
  ORDER BY total_sales DESC
""")
print("✓ Created function: get_segment_sales()")

# Function 7: Get top 5 products (fixed version without parameter)
spark.sql("""
CREATE OR REPLACE FUNCTION retailpulse.agent.get_top_5_products()
RETURNS TABLE(
  product_name STRING,
  total_sales DECIMAL(20,2)
)
COMMENT 'Returns top 5 products by total sales amount'
RETURN 
  SELECT product_name, total_sales
  FROM (
    SELECT 
      p.product_name, 
      SUM(f.sales_amount) as total_sales,
      ROW_NUMBER() OVER (ORDER BY SUM(f.sales_amount) DESC) as rank
    FROM retailpulse.gold.fact_sales f
    JOIN retailpulse.gold.dim_product p ON f.product_sk = p.product_sk
    GROUP BY p.product_name
  )
  WHERE rank <= 5
""")
print("✓ Created function: get_top_5_products()")

# Function 8: Get top 10 products
spark.sql("""
CREATE OR REPLACE FUNCTION retailpulse.agent.get_top_10_products()
RETURNS TABLE(
  product_name STRING,
  total_sales DECIMAL(20,2)
)
COMMENT 'Returns top 10 products by total sales amount'
RETURN 
  SELECT product_name, total_sales
  FROM (
    SELECT 
      p.product_name, 
      SUM(f.sales_amount) as total_sales,
      ROW_NUMBER() OVER (ORDER BY SUM(f.sales_amount) DESC) as rank
    FROM retailpulse.gold.fact_sales f
    JOIN retailpulse.gold.dim_product p ON f.product_sk = p.product_sk
    GROUP BY p.product_name
  )
  WHERE rank <= 10
""")
print("✓ Created function: get_top_10_products()")

print("\n" + "="*80)
print("All functions registered successfully!")
print("="*80)

# List all functions
print("\nRegistered functions in retailpulse.agent:")
functions = spark.sql("SHOW USER FUNCTIONS IN retailpulse.agent").collect()
for func in functions:
    print(f"  • {func.function}")

# COMMAND ----------

# DBTITLE 1,Test Unity Catalog Functions
"""Test the registered Unity Catalog functions."""

print("="*80)
print("TESTING UNITY CATALOG AGENT FUNCTIONS")
print("="*80)

# Test 1: Scalar function - get_customer_count()
print("\n1. Get Customer Count (Scalar Function)")
print("-" * 80)
result = spark.sql("SELECT retailpulse.agent.get_customer_count() as customer_count")
result.show()

# Test 2: Scalar function - get_total_sales()
print("\n2. Get Total Sales (Scalar Function)")
print("-" * 80)
result = spark.sql("SELECT retailpulse.agent.get_total_sales() as total_sales")
result.show()

# Test 3: Table function - get_top_products()
print("\n3. Get Top 5 Products (Table Function)")
print("-" * 80)
result = spark.sql("SELECT * FROM retailpulse.agent.get_top_products(5)")
result.show(truncate=False)

# Test 4: Table function - get_monthly_trends()
print("\n4. Get Monthly Sales Trends (Table Function)")
print("-" * 80)
result = spark.sql("SELECT * FROM retailpulse.agent.get_monthly_trends()")
result.show(truncate=False)

# Test 5: Table function - get_segment_sales()
print("\n5. Get Sales by Customer Segment (Table Function)")
print("-" * 80)
result = spark.sql("SELECT * FROM retailpulse.agent.get_segment_sales()")
result.show(truncate=False)

# Test 6: get_customer_sales() with limit
print("\n6. Get Top 10 Customers by Sales (Table Function)")
print("-" * 80)
result = spark.sql("SELECT * FROM retailpulse.agent.get_customer_sales() LIMIT 10")
result.show(truncate=False)

print("\n" + "="*80)
print("USAGE EXAMPLES")
print("="*80)
print("\n# From SQL:")
print("  SELECT * FROM retailpulse.agent.get_top_products(10)")
print("  SELECT retailpulse.agent.get_total_sales()")
print("  SELECT * FROM retailpulse.agent.get_monthly_trends()")
print("\n# From Python:")
print("  spark.sql('SELECT * FROM retailpulse.agent.get_top_products(5)').show()")
print("  df = spark.table('retailpulse.agent.get_customer_sales()')")
print("\n# From Dashboards/BI Tools:")
print("  Use the function names directly in SQL queries!")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Register Python UDF for NL-to-SQL
"""Register Python UDF for natural language to SQL conversion."""

from pyspark.sql.types import StringType
import re

# Create a simplified NL-to-SQL function for UDF
def nl_to_sql_udf(question: str) -> str:
    """
    Simplified NL-to-SQL converter for use as a UDF.
    Returns the generated SQL query as a string.
    """
    if not question:
        return "SELECT 'Invalid question' as error"
    
    question_lower = question.lower()
    
    # Pattern: customer count
    if "how many customer" in question_lower or "customer count" in question_lower:
        return "SELECT COUNT(*) as customer_count FROM retailpulse.gold.dim_customer"
    
    # Pattern: total sales
    if "total sales" in question_lower:
        return "SELECT SUM(sales_amount) as total_sales FROM retailpulse.gold.fact_sales"
    
    # Pattern: top products
    if "top" in question_lower and "product" in question_lower:
        limit = 10
        match = re.search(r'top (\d+)', question_lower)
        if match:
            limit = int(match.group(1))
        return f"SELECT * FROM retailpulse.agent.get_top_products({limit})"
    
    # Pattern: monthly trends/sales
    if "monthly" in question_lower or "by month" in question_lower:
        return "SELECT * FROM retailpulse.agent.get_monthly_trends()"
    
    # Pattern: customer sales
    if "customer" in question_lower and "sales" in question_lower:
        return "SELECT * FROM retailpulse.agent.get_customer_sales() LIMIT 20"
    
    # Pattern: segment analysis
    if "segment" in question_lower:
        return "SELECT * FROM retailpulse.agent.get_segment_sales()"
    
    # Default
    return "SELECT 'Question pattern not recognized. Try: top products, monthly sales, customer count' as message"

# Register the UDF in Spark session (temporary)
spark.udf.register("nl_to_sql_temp", nl_to_sql_udf, StringType())

# Note: For persistent Unity Catalog Python UDFs, you need to use CREATE FUNCTION with Python
# This requires the function code to be in a module or inline
try:
    spark.sql("""
    CREATE OR REPLACE FUNCTION retailpulse.agent.nl_to_sql(question STRING)
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT 'Convert natural language questions to SQL queries'
    AS $$
    import re
    if not question:
        return "SELECT 'Invalid question' as error"
    question_lower = question.lower()
    if "how many customer" in question_lower or "customer count" in question_lower:
        return "SELECT COUNT(*) as customer_count FROM retailpulse.gold.dim_customer"
    if "total sales" in question_lower:
        return "SELECT SUM(sales_amount) as total_sales FROM retailpulse.gold.fact_sales"
    if "top" in question_lower and "product" in question_lower:
        limit = 10
        match = re.search(r'top (\\d+)', question_lower)
        if match:
            limit = int(match.group(1))
        return f"SELECT * FROM retailpulse.agent.get_top_products({limit})"
    if "monthly" in question_lower or "by month" in question_lower:
        return "SELECT * FROM retailpulse.agent.get_monthly_trends()"
    if "customer" in question_lower and "sales" in question_lower:
        return "SELECT * FROM retailpulse.agent.get_customer_sales() LIMIT 20"
    if "segment" in question_lower:
        return "SELECT * FROM retailpulse.agent.get_segment_sales()"
    return "SELECT 'Question pattern not recognized. Try: top products, monthly sales, customer count' as message"
    $$
    """)
    print("✓ Created Python UDF: nl_to_sql(question)")
except Exception as e:
    print(f"⚠ Could not create persistent Python UDF: {e}")
    print("  Using temporary session UDF instead (nl_to_sql_temp)")

print("\n" + "="*80)
print("NL-TO-SQL UDF REGISTERED")
print("="*80)
print("\nUsage:")
print("  SELECT retailpulse.agent.nl_to_sql('How many customers do we have?')")
print("  SELECT retailpulse.agent.nl_to_sql('Show me top 5 products')")
print("\nNote: The UDF returns the SQL query as a string.")
print("      To execute it, you would need to use dynamic SQL or the Python agent.")
print("="*80)

# Test the UDF
print("\nTesting NL-to-SQL UDF:")
test_questions = [
    "How many customers do we have?",
    "Show me top 5 products",
    "What are the monthly sales trends?"
]

for q in test_questions:
    try:
        result = spark.sql(f"SELECT retailpulse.agent.nl_to_sql('{q}') as generated_sql").collect()[0]['generated_sql']
        print(f"\nQ: {q}")
        print(f"SQL: {result}")
    except:
        # Fallback to temp UDF
        result = spark.sql(f"SELECT nl_to_sql_temp('{q}') as generated_sql").collect()[0]['generated_sql']
        print(f"\nQ: {q}")
        print(f"SQL: {result}")