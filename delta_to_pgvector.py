import pandas as pd
from sentence_transformers import SentenceTransformer
import psycopg2
from pyspark.sql import SparkSession

# STEP 1: Initialize Spark (in Databricks this is already available as 'spark')
spark = SparkSession.builder.getOrCreate()

# STEP 2: Read Delta Table
delta_table_path = "/mnt/delta/my_table"  # change this to your path or use table name
df = spark.read.format("delta").load(delta_table_path)

# Convert to Pandas
pdf = df.select("id", "text_column").toPandas()  # change column names as needed

# STEP 3: Generate Embeddings
model = SentenceTransformer('all-MiniLM-L6-v2')  # or use another model
pdf['embedding'] = pdf['text_column'].apply(lambda x: model.encode(x).tolist())

# STEP 4: Connect to PostgreSQL
conn = psycopg2.connect(
    host="your_host",
    dbname="your_db",
    user="your_user",
    password="your_password",
    port="5432"
)
cursor = conn.cursor()

# Optional: Create table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS my_table (
    id INTEGER PRIMARY KEY,
    text TEXT,
    embedding VECTOR(384)
)
""")
conn.commit()

# STEP 5: Insert Data into PostgreSQL
for _, row in pdf.iterrows():
    cursor.execute(
        "INSERT INTO my_table (id, text, embedding) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING",
        (int(row['id']), row['text_column'], row['embedding'])
    )

# Finalize
conn.commit()
cursor.close()
conn.close()

print("✅ Data loaded into pgvector successfully.")
