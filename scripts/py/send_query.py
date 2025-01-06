import json
import os
from pathlib import Path
import time

"""
84477.1438941475
10110734.523138978
1684633.681482328
2033773.2520982015
"""
import pandas as pd
import psycopg2
import argparse

RUN_ESTIMATES = True 

METADATA = {
    "power": {
        "database_name": "power",
        "sql_file": "/home/student.unimelb.edu.au/lrathuwadu/phd/End-to-End-CardEst-Benchmark/workloads/power/power_query.sql",
        "export_filename": "power_cost.xlsx",
        "cardest_filename": "power_estimations.txt"
    },
    "forest": {
        "database_name": "forest",
        "sql_file": "/home/student.unimelb.edu.au/lrathuwadu/phd/End-to-End-CardEst-Benchmark/workloads/forest/forest_query.sql",
        "export_filename": "forest_cost.xlsx",
        "cardest_filename": "forest_estimations.txt"
    },
}


def main(dataset):
    EXPORT_FILENAME = METADATA[dataset]["export_filename"]
    database_name = METADATA[dataset]["database_name"]
    cardest_filename = METADATA[dataset]["cardest_filename"]
    sql_file = METADATA[dataset]["sql_file"]

    conn = psycopg2.connect(database=database_name, host="localhost", port=5431, password="postgres", user="postgres")
    # conn.autocommit = True
    cursor = conn.cursor()

    # imdb_sql_file = open("/home/titan/phd/megadrive/End-to-End-CardEst-Benchmark/workloads/stats_CEB/sub_plan_queries/stats_CEB_single_table_sub_query.sql")
    imdb_sql_file = open(sql_file)
    queries = imdb_sql_file.readlines()
    imdb_sql_file.close()

    if os.path.exists("/Users/hanyuxing/pgsql/13.1/data/join_est_record_job.txt"):
        os.remove("/Users/hanyuxing/pgsql/13.1/data/join_est_record_job.txt")

    cursor.execute("SET debug_card_est=true;")
    cursor.execute("SET print_sub_queries=true;")

    """ ReadMe
    stats=# SET ml_cardest_enabled=true; ## for single table
    stats=# SET ml_joinest_enabled=true; ## for multi-table
    stats=# SET query_no=0; ##for single table
    stats=# SET join_est_no=0; ##for multi-table
    stats=# SET ml_cardest_fname='stats_CEB_sub_queries_bayescard.txt'; ## for single table
    stats=# SET ml_joinest_fname='stats_CEB_sub_queries_bayescard.txt'; ## for multi-table
    """

    if RUN_ESTIMATES:
        # Single table queries
        # cursor.execute('SET print_single_tbl_queries=true')
        cursor.execute("SET ml_cardest_enabled=true;")
        cursor.execute(f"SET ml_cardest_fname='{cardest_filename}';")
        cursor.execute("SET query_no=0;")
        # conn.commit()

    # Join queries
    # cursor.execute("SET ml_joinest_enabled=true;")
    # cursor.execute("SET join_est_no=0;")
    # cursor.execute("SET ml_joinest_fname='stats_CEB_join_queries_bayescard.txt';")
    # conn.commit()

    # conn = psycopg2.connect(database="stats", host="localhost", port=5431, password="postgres", user="postgres")
    # cursor = conn.cursor()

    time.sleep(1)
    dict_list = []
    for no, query in enumerate(queries):
        # sql_txt = "EXPLAIN (FORMAT JSON)SELECT COUNT(*) FROM users as u WHERE u.UpVotes>=0;"
        # EXPLAIN (FORMAT JSON)SELECT COUNT(*) FROM badges as b, users as u WHERE b.UserId= u.Id AND u.UpVotes>=0;
        sql_txt = "EXPLAIN (FORMAT JSON) " + query.split('\n')[0]
        print(f"Executing {no}-th query: {sql_txt}")
        cursor.execute(sql_txt)
        res = cursor.fetchall()
        res_json = res[0][0][0]
        print(json.dumps(res_json, indent=4))
        total_cost = res_json["Plan"]["Total Cost"]
        # scan_type = res_json["Plan"]["Plans"][0]["Node Type"]
        print("Total Cost: ", total_cost)
        
        if RUN_ESTIMATES:
            dict_list.append({"index": no, "query": query, "total_cost_estimates": total_cost})
        else:
            dict_list.append({"total_cost_true": total_cost})

    df = pd.DataFrame(dict_list)

    if Path(EXPORT_FILENAME).exists():
        df_file = pd.read_excel(EXPORT_FILENAME)
        df = pd.concat([df_file, df], axis=1)

    """Write to a csv file"""
    df.to_excel(EXPORT_FILENAME, index=False)
        

    cursor.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute SQL queries and export cost estimates.")
    parser.add_argument("dataset", choices=METADATA.keys(), help="The dataset to use (power or forest).")
    args = parser.parse_args()

    main(args.dataset)
