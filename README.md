# delta-table-to-postgres-pgvector
This exercise shows a complete python script that connects to a Databricks Delta Table, Loads the data into Pandas,  generates embeddings using sentence-transformers, inserts into a PostgreSQL table with pgvector.


### Note:

pip install pandas sentence-transformers psycopg2-binary

Replace text_column with your actual column containing text.

Adjust vector dimensions (e.g., VECTOR(384)) if you use a different model.

Use ON CONFLICT (id) DO NOTHING to avoid duplicate key issues (optional).

For large datasets, consider batch inserts for performance.
