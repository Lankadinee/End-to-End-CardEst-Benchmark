import json
import os
import time

"""
84477.1438941475
10110734.523138978
1684633.681482328
2033773.2520982015
"""
import psycopg2

conn = psycopg2.connect(database="stats", host="localhost", port=5431, password="postgres", user="postgres", connect_timeout=10)
conn.autocommit = True
cursor = conn.cursor()

imdb_sql_file = open("/home/titan/phd/megadrive/End-to-End-CardEst-Benchmark/workloads/stats_CEB/stats_CEB.sql")
queries = imdb_sql_file.readlines()
imdb_sql_file.close()

if os.path.exists("/Users/hanyuxing/pgsql/13.1/data/join_est_record_job.txt"):
    os.remove("/Users/hanyuxing/pgsql/13.1/data/join_est_record_job.txt")

cursor.execute("SET debug_card_est=true")
cursor.execute("SET print_sub_queries=true;")

""" ReadMe
stats=# SET ml_cardest_enabled=true; ## for single table
stats=# SET ml_joinest_enabled=true; ## for multi-table
stats=# SET query_no=0; ##for single table
stats=# SET join_est_no=0; ##for multi-table
stats=# SET ml_cardest_fname='stats_CEB_sub_queries_bayescard.txt'; ## for single table
stats=# SET ml_joinest_fname='stats_CEB_sub_queries_bayescard.txt'; ## for multi-table
"""

# Single table queries
# cursor.execute('SET print_single_tbl_queries=true')
cursor.execute("SET ml_cardest_enabled=true")
cursor.execute("SET ml_cardest_fname='stats_CEB_sub_queries_bayescard.txt'")
cursor.execute("SET query_no=0")

# Join queries
# cursor.execute("SET ml_joinest_enabled=true;")
# cursor.execute("SET join_est_no=0;")
# cursor.execute("SET ml_joinest_fname='stats_CEB_join_queries_bayescard.txt';")
conn.commit()

# conn = psycopg2.connect(database="stats", host="localhost", port=5431, password="postgres", user="postgres")
# cursor = conn.cursor()

time.sleep(1)
for no, query in enumerate(queries):
    sql_txt = "EXPLAIN (FORMAT JSON)SELECT COUNT(*) FROM users as u WHERE u.UpVotes>=0;"
    # EXPLAIN (FORMAT JSON)SELECT COUNT(*) FROM badges as b, users as u WHERE b.UserId= u.Id AND u.UpVotes>=0;
    # sql_txt = "EXPLAIN (FORMAT JSON)" + query.split("||")[1]
    print(f"Executing {no}-th query: {sql_txt}")
    cursor.execute(sql_txt)
    res = cursor.fetchall()
    res_json = res[0][0][0]
    print(json.dumps(res_json, indent=4))
    total_cost = res_json["Plan"]["Total Cost"]
    print("Total Cost: ", total_cost)
    # cursor.execute("SET query_no=0")
    print(f"{no}-th query - {sql_txt}")
    if no == 2:
        break

cursor.close()
conn.close()
