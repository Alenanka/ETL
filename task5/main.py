import psycopg2
tables = ['customer', 'lineitem', 'lineitem', 'nation', 'orders', 'partsupp', 'part', 'region', 'supplier']

conn_string= "host='localhost' port=54320 dbname='my_database' user='root' password='postgres'"
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
     for table in tables:
         q = f"COPY {table} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
         with open(f'resultsfile{table}.csv', 'w') as f:
            cursor.copy_expert(q, f)

conn_string2= "host='localhost' port=5433 dbname='my_database' user='root' password='postgres'"
with psycopg2.connect(conn_string2) as conn, conn.cursor() as cursor:
    for table in tables:
        q = f"COPY {table} from STDIN WITH DELIMITER ',' CSV HEADER;"
        with open(f'resultsfile{table}.csv', 'r') as f:
            cursor.copy_expert(q, f)