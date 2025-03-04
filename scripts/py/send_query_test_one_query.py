import json
import os
from pathlib import Path
import random
import sys
import time

from loguru import logger
import numpy as np
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
RETRY_CONNECTION_WHEN_FAILED = False

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
        "export_filename": "census_cost_true.xlsx",
        "cardest_filename": "census_true_card.txt"
    },
    "dmv": {
        "database_name": "dmv",
        "sql_file": "/workspace/phd/End-to-End-CardEst-Benchmark/workloads/dmv/dmv_query.sql",
        "export_filename": "dmv_cost_true.xlsx",
        "cardest_filename": "dmv_true_card.txt"
    },
    
    "tpch_sf2_z1": {
        "database_name": "tpch_sf2_z1",
        "sql_file": "/workspace/phd/End-to-End-CardEst-Benchmark/workloads/tpch_sf2_z1/tpch_sf2_z1.sql",
        "export_filename": "tpch_sf2_z1_lineitem_cost_true.xlsx",
        "cardest_filename": "tpch_sf2_z1_lineitem_true_card.txt"
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
        "export_filename": "synthetic_correlated_2_cost_proposed.xlsx",
        "cardest_filename": "synthetic_correlated_2_estimations_sample.txt"
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
    "tpch_lineitem_10": {
        "database_name": "tpch_lineitem_10",
        "sql_file": "/workspace/phd/End-to-End-CardEst-Benchmark/workloads/tpch_lineitem_10/tpch_lineitem_10.sql",
        "export_filename": "tpch_lineitem_10_cost_true.xlsx",
        "cardest_filename": "tpch_lineitem_10_true_card.txt"
    },
    "tpch_lineitem_20": {
        "database_name": "tpch_lineitem_20",
        "sql_file": "/workspace/phd/End-to-End-CardEst-Benchmark/workloads/tpch_lineitem_20/tpch_lineitem_20.sql",
        "export_filename": "tpch_lineitem_20_cost_true.xlsx",
        "cardest_filename": "tpch_lineitem_20_true_card.txt"
    },
    "custom": {
        "database_name": "custom",
        "sql_file": "/workspace/phd/End-to-End-CardEst-Benchmark/workloads/tpch_lineitem_20/tpch_lineitem_20.sql",
        "export_filename": "tpch_lineitem_20_cost_true.xlsx",
        "cardest_filename": "tpch_lineitem_20_true_card.txt"
    },
}

current_dir = Path(__file__).resolve().parent

def create_connection(database_name, cardest_filename=False, query_no=0):
    print(f"Connecting to {database_name}...")
    conn = psycopg2.connect(database=database_name, host="localhost", port=5431, password="postgres", user="postgres")
    conn.autocommit = True
    cursor = conn.cursor()

    if cardest_filename:
        print(f"Using {cardest_filename} for estimates. Setting up the config...")
        cursor.execute("SET debug_card_est=true;")
        cursor.execute("SET print_sub_queries=true;")

        if RUN_ESTIMATES:
            # Single table queries
            # cursor.execute('SET print_single_tbl_queries=true')
            cursor.execute("SET ml_cardest_enabled=true;")
            cursor.execute(f"SET ml_cardest_fname='{cardest_filename}';")
            cursor.execute(f"SET query_no={query_no};")
    
    time.sleep(2)
    return conn, cursor

def main(dataset, container_name):
    for cardest_filename in get_all_unprocessed_txt_files(container_name, "/var/lib/pgsql/13.1/data/"):
        run_one_file(dataset, cardest_filename)

def run_one_file(dataset, cardest_filename):
    database_name = METADATA[dataset]["database_name"]
    # sql_file = METADATA[dataset]["sql_file"]

    conn, cursor = create_connection(database_name)
    print(f"Processing {cardest_filename}")

    export_dirpath = current_dir / f'../plan_cost/{database_name}/'
    export_filepath = export_dirpath / f'{cardest_filename.split(".")[0] + "_cost_variable.csv"}'
    print(f"Exporting to {export_filepath}")
    if not export_dirpath.exists():
        export_dirpath.mkdir(parents=True)
        
    if export_filepath.exists():
        print(f"File {export_filepath} already exists. Deleting...")
        export_filepath.unlink()


        # imdb_sql_file = open("/home/titan/phd/megadrive/End-to-End-CardEst-Benchmark/workloads/stats_CEB/sub_plan_queries/stats_CEB_single_table_sub_query.sql")
    # imdb_sql_file = open(sql_file)
    # queries = imdb_sql_file.readlines()
    # imdb_sql_file.close()

    if os.path.exists("/Users/hanyuxing/pgsql/13.1/data/join_est_record_job.txt"):
        os.remove("/Users/hanyuxing/pgsql/13.1/data/join_est_record_job.txt")

    """ ReadMe
        stats=# SET ml_cardest_enabled=true; ## for single table
        stats=# SET ml_joinest_enabled=true; ## for multi-table
        stats=# SET query_no=0; ##for single table
        stats=# SET join_est_no=0; ##for multi-table
        stats=# SET ml_cardest_fname='stats_CEB_sub_queries_bayescard.txt'; ## for single table
        stats=# SET ml_joinest_fname='stats_CEB_sub_queries_bayescard.txt'; ## for multi-table
        """
    cursor.execute("SET debug_card_est=true;")
    cursor.execute("SET print_sub_queries=true;")

    if RUN_ESTIMATES:
            # Single table queries
            # cursor.execute('SET print_single_tbl_queries=true')
        cursor.execute("SET ml_cardest_enabled=true;")
        cursor.execute(f"SET ml_cardest_fname='{cardest_filename}';")
        cursor.execute("SET query_no=0;")

        # Join queries
        # cursor.execute("SET ml_joinest_enabled=true;")
        # cursor.execute("SET join_est_no=0;")
        # cursor.execute("SET ml_joinest_fname='stats_CEB_join_queries_bayescard.txt';")
        # conn.commit()

        # conn = psycopg2.connect(database="stats", host="localhost", port=5431, password="postgres", user="postgres")
        # cursor = conn.cursor()

    time.sleep(1)
    dict_list = []
    # ub_values = np.linspace(0, 48, 100)
    # for no, val in tqdm(enumerate(ub_values)):
    # for no in tqdm(range(1, 1927)):
    distance = 100
    for no in tqdm(range(1, 1000 - distance)):
        lb = random.randint(1, 100)
        ub = 785 + random.randint(1, 100)
            # sql_txt = "EXPLAIN (FORMAT JSON)SELECT COUNT(*) FROM users as u WHERE u.UpVotes>=0;"
            # EXPLAIN (FORMAT JSON)SELECT COUNT(*) FROM badges as b, users as u WHERE b.UserId= u.Id AND u.UpVotes>=0;
        # sql_txt = f"EXPLAIN (FORMAT JSON)SELECT COUNT(*) FROM power WHERE Global_intensity <= {val};"
        sql_txt = f"EXPLAIN (FORMAT JSON)SELECT COUNT(*) FROM custom WHERE Value_1 <= {ub} AND Value_1 >= {lb};"
            # cursor.execute(sql_txt)
            # res = cursor.fetchall()
            # print(f"Executing {no}-th query: {sql_txt}")
        retry_count = 0
        while True:
            try:
                if retry_count > 0:
                    conn, cursor = create_connection(database_name, cardest_filename, query_no=no)
                cursor.execute(sql_txt)
                res = cursor.fetchall()
                break
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                print(e)
                logger.error("Connection error")
                cursor.close()
                conn.close()
                retry_count += 1
                if retry_count > 5 or not RETRY_CONNECTION_WHEN_FAILED:
                    print("Failed to execute the query.")
                    break
                print("Retrying... ", retry_count)
                time.sleep(3)
                continue
        res_json = res[0][0][0]
        print(f"{no} - {json.dumps(res_json, indent=4)}")
            # """Save the json output to a file"""
            # with open(f"/workspace/phd/End-to-End-CardEst-Benchmark/query_plans/3/query_{no}.json", "w") as f:
            #     f.write(json.dumps(res_json, indent=4))
        total_cost = res_json["Plan"]["Total Cost"]
        scan_type = res_json["Plan"]["Plans"][0]["Node Type"]
        # rows_1 = res_json["Plan"]["Plan Rows"]
        rows_2 = res_json["Plan"]["Plans"][0]["Plan Rows"]
            # print("Total Cost: ", total_cost)
        real_card = ub - lb
            
        if RUN_ESTIMATES:
            dict_list.append({
                "index": no, 
                "total_cost_estimates": total_cost, 
                "access_path": scan_type, 
                "rows_2": rows_2,
                "real_card": real_card
                })
        else:
            dict_list.append({"total_cost_true": total_cost, "true_access_path": scan_type})

        # print("Used estimates from ", cardest_filename)
    df = pd.DataFrame(dict_list)

    if export_filepath.exists():
        df_file = pd.read_csv(export_filepath)
        df = pd.concat([df_file, df], axis=1)

    """Write to a csv file"""
    df.to_csv(export_filepath.as_posix(), index=False)
            

    cursor.close()
    conn.close()


if __name__ == "__main__":
    # print("Starting the process...")
    # parser = argparse.ArgumentParser(description="Execute SQL queries and export cost estimates.")
    # parser.add_argument("--database_name", choices=METADATA.keys(), help="The dataset to use (power or forest).")
    # parser.add_argument("--container_name", help="Name of the container to run the queries.")
    # parser.add_argument("--filename", default="NA", help="Cardest filename")

    # args = parser.parse_args()

    # if args.filename == "NA":
    #     main(args.database_name, args.conatiner_name)
    # else:
    run_one_file("custom", "custom_estimates_1_1000.txt")
