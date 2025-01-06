import os

import psycopg2

conn = psycopg2.connect(database="stats", host="localhost", port=5430, password="postgres", user="postgres")
cursor = conn.cursor()

imdb_sql_file = open("workloads/stats_CEB/stats_CEB.sql")
queries = imdb_sql_file.readlines()
imdb_sql_file.close()

if os.path.exists("/Users/hanyuxing/pgsql/13.1/data/join_est_record_job.txt"):
    os.remove("/Users/hanyuxing/pgsql/13.1/data/join_est_record_job.txt")

cursor.execute("SET debug_card_est=true")
cursor.execute("SET print_sub_queries=true")
# cursor.execute('SET print_single_tbl_queries=true')

for no, query in enumerate(queries):
    sql_txt = "EXPLAIN (FORMAT JSON)" + query.split("||")[1]
    
    cursor.execute(sql_txt)
    res = cursor.fetchall()
    print(res)
    cursor.execute("SET query_no=0")
    print(f"{no}-th query - {sql_txt}")
    # break

cursor.close()
conn.close()
