import json
import os
from pathlib import Path
import sys
import time

from tqdm import tqdm

from get_list_of_files import get_all_unprocessed_txt_files

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
        "sql_file": "/workspace/phd/End-to-End-CardEst-Benchmark/workloads/power/power_query.sql",
        "export_filename": "power_cost_sample_5k.xlsx",
        "cardest_filename": "power_estimations_sample_5k.txt"
    },
    "forest": {
        "database_name": "forest",
        "sql_file": "/workspace/phd/End-to-End-CardEst-Benchmark/workloads/forest/forest_query.sql",
        "export_filename": "forest_estimations_low_precision.xlsx",
        "cardest_filename": "forest_estimations_low_precision.txt"
    },
    "census": {
        "database_name": "census",
        "sql_file": "/workspace/phd/End-to-End-CardEst-Benchmark/workloads/census/census_query.sql",
        "export_filename": "census_estimations_sample_auto_max_25000_cost.csv",
        "cardest_filename": "census_estimations_sample_auto_max_25000.txt"
    },
    "dmv": {
        "database_name": "dmv",
        "sql_file": "/home/student.unimelb.edu.au/lrathuwadu/phd/End-to-End-CardEst-Benchmark/workloads/dmv/dmv_query.sql",
        "export_filename": "dmv_cost_true.xlsx",
        "cardest_filename": "dmv_true_card.txt"
    },
    
    "tpch_sf2_z1": {
        "database_name": "tpch_sf2_z1",
        "sql_file": "/workspace/phd/End-to-End-CardEst-Benchmark/workloads/tpch_sf2_z1/tpch_sf2_z1.sql",
        "export_filename": "tpch_sf2_z1_lineitem_cost_true_db.xlsx",
        "cardest_filename": "hist_tpch_sf2_z1_lineitem_1000_results.txt"
    },
    
    "tpch_sf2_z2": {
        "database_name": "tpch_sf2_z2",
        "sql_file": "/workspace/phd/End-to-End-CardEst-Benchmark/workloads/tpch_sf2_z2/tpch_sf2_z2.sql",
        "export_filename": "tpch_sf2_z2_lineitem_cost_true.xlsx",
        "cardest_filename": "tpch_sf2_z2_lineitem_true_card.txt"
    },
    
    "tpch_sf2_z3": {
        "database_name": "tpch_sf2_z3",
        "sql_file": "/workspace/phd/End-to-End-CardEst-Benchmark/workloads/tpch_sf2_z3/tpch_sf2_z3.sql",
        "export_filename": "tpch_sf2_z3_lineitem_cost_true.xlsx",
        "cardest_filename": "tpch_sf2_z3_lineitem_true_card.txt"
    },
    "tpch_sf2_z4": {
        "database_name": "tpch_sf2_z4",
        "sql_file": "/workspace/phd/End-to-End-CardEst-Benchmark/workloads/tpch_sf2_z4/tpch_sf2_z4.sql",
        "export_filename": "tpch_sf2_z4_lineitem_cost_true.xlsx",
        "cardest_filename": "tpch_sf2_z4_lineitem_true_card.txt"
    },
    "synthetic_correlated_2": {
        "database_name": "synthetic_correlated_2",
        "sql_file": "/workspace/phd/End-to-End-CardEst-Benchmark/workloads/synthetic_correlated_2/synthetic_correlated_2.sql",
        "export_filename": "synthetic_correlated_2_true_card_cost.xlsx",
        "cardest_filename": "synthetic_correlated_2_true_card.txt"
    },
    "synthetic_correlated_3": {
        "database_name": "synthetic_correlated_3",
        "sql_file": "/workspace/phd/End-to-End-CardEst-Benchmark/workloads/synthetic_correlated_3/synthetic_correlated_3.sql",
        "export_filename": "synthetic_correlated_3_cost_proposed.xlsx",
        "cardest_filename": "synthetic_correlated_3_estimations_sample.txt"
    },
    "synthetic_correlated_4": {
        "database_name": "synthetic_correlated_4",
        "sql_file": "/workspace/phd/End-to-End-CardEst-Benchmark/workloads/synthetic_correlated_4/synthetic_correlated_4.sql",
        "export_filename": "synthetic_correlated_4_cost_proposed.xlsx",
        "cardest_filename": "synthetic_correlated_4_estimations_sample.txt"
    },
    "synthetic_correlated_6": {
        "database_name": "synthetic_correlated_6",
        "sql_file": "/workspace/phd/End-to-End-CardEst-Benchmark/workloads/synthetic_correlated_6/synthetic_correlated_6.sql",
        "export_filename": "synthetic_correlated_6_cost_proposed.xlsx",
        "cardest_filename": "synthetic_correlated_6_estimations_sample.txt"
    },
    "synthetic_correlated_8": {
        "database_name": "synthetic_correlated_8",
        "sql_file": "/workspace/phd/End-to-End-CardEst-Benchmark/workloads/synthetic_correlated_8/synthetic_correlated_8.sql",
        "export_filename": "synthetic_correlated_8_cost_proposed.xlsx",
        "cardest_filename": "synthetic_correlated_8_estimations_sample.txt"
    },
    
    "synthetic_correlated_10": {
        "database_name": "synthetic_correlated_10",
        "sql_file": "/workspace/phd/End-to-End-CardEst-Benchmark/workloads/synthetic_correlated_10/synthetic_correlated_10.sql",
        "export_filename": "synthetic_correlated_10_cost_proposed.xlsx",
        "cardest_filename": "synthetic_correlated_10_estimations_sample.txt"
    },
}

current_dir = Path(__file__).resolve().parent

def create_connection(database_name):
    conn = psycopg2.connect(database=database_name, host="localhost", port=5431, password="postgres", user="postgres")
    conn.autocommit = True
    cursor = conn.cursor()
    return conn, cursor

def main(dataset):
    database_name = METADATA[dataset]["database_name"]
    sql_file = METADATA[dataset]["sql_file"]
    cardest_filename = METADATA[dataset]["cardest_filename"]
    export_filepath = METADATA[dataset]["export_filename"]
    
    conn, cursor = create_connection(database_name)

    print(f"Exporting to {export_filepath}")


    # imdb_sql_file = open("/home/titan/phd/megadrive/End-to-End-CardEst-Benchmark/workloads/stats_CEB/sub_plan_queries/stats_CEB_single_table_sub_query.sql")
    imdb_sql_file = open(sql_file)
    queries = imdb_sql_file.readlines()
    imdb_sql_file.close()

    if os.path.exists("/Users/hanyuxing/pgsql/13.1/data/join_est_record_job.txt"):
        os.remove("/Users/hanyuxing/pgsql/13.1/data/join_est_record_job.txt")

    cursor.execute("SET debug_card_est=true;")
    cursor.execute("SET print_sub_queries=true;")
    # cursor.execute("VACUUM FULL;")
    # cursor.execute("ANALYZE;")

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
    for no, query in tqdm(enumerate(queries), total=len(queries), leave=True, file=sys.stdout):
        # sql_txt = "EXPLAIN (FORMAT JSON)SELECT COUNT(*) FROM users as u WHERE u.UpVotes>=0;"
        # EXPLAIN (FORMAT JSON)SELECT COUNT(*) FROM badges as b, users as u WHERE b.UserId= u.Id AND u.UpVotes>=0;
        sql_txt = "EXPLAIN (FORMAT JSON) " + query.split('\n')[0]
        cursor.execute(sql_txt)
        res = cursor.fetchall()
        # print(f"Executing {no}-th query: {sql_txt}")
        # retry_count = 0
        # while True:
        #     try:
        #         if retry_count > 0:
        #             conn, cursor = create_connection(database_name)
        #         cursor.execute(sql_txt)
        #         res = cursor.fetchall()
        #         break
        #     except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
        #         print(e)
        #         cursor.close()
        #         conn.close()
        #         retry_count += 1
        #         if retry_count > 5:
        #             print("Failed to execute the query.")
        #             break
        #         print("Retrying... ", retry_count)
        #         time.sleep(3)
        #         continue
        res_json = res[0][0][0]
        # print(json.dumps(res_json, indent=4))
        # """Save the json output to a file"""
        # with open(f"/workspace/phd/End-to-End-CardEst-Benchmark/query_plans/3/query_{no}.json", "w") as f:
        #     f.write(json.dumps(res_json, indent=4))
        total_cost = res_json["Plan"]["Total Cost"]
        scan_type = res_json["Plan"]["Plans"][0]["Node Type"]
        rows = res_json["Plan"]["Plans"][0]["Plan Rows"]
        # print("Total Cost: ", total_cost)
        
        if RUN_ESTIMATES:
            dict_list.append({"index": no, "query": query, "total_cost_estimates": total_cost, "access_path": scan_type, "rows": rows})
        else:
            dict_list.append({"total_cost_true": total_cost, "true_access_path": scan_type})

    # print("Used estimates from ", cardest_filename)
    df = pd.DataFrame(dict_list)

    # if export_filepath.exists():
    #     df_file = pd.read_csv(export_filepath)
    #     df = pd.concat([df_file, df], axis=1)

    """Write to a csv file"""
    df.to_csv(export_filepath, index=False)
            

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main("census")
