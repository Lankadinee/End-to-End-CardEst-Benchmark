import os
import random
import time
from pathlib import Path

import pandas as pd
import psycopg2
from get_list_of_files import get_all_unprocessed_txt_files
from loguru import logger
from tqdm import tqdm

RUN_ESTIMATES = True
RETRY_CONNECTION_WHEN_FAILED = False
EXPERIMANT_NO = 3

METADATA = {
    "custom": {
        "database_name": "custom",
        "sql_file": "./workloads/tpch_lineitem_20/tpch_lineitem_20.sql",
    }
}

logger.info(f"Starting the experiment no: {EXPERIMANT_NO}")

current_dir = Path(__file__).resolve().parent


def get_sql_query(index):
    """
    # sql_txt = "EXPLAIN (FORMAT JSON)SELECT COUNT(*) FROM users as u WHERE u.UpVotes>=0;"
    # EXPLAIN (FORMAT JSON)SELECT COUNT(*) FROM badges as b, users as u WHERE b.UserId= u.Id AND u.UpVotes>=0;
    # sql_txt = f"EXPLAIN (FORMAT JSON)SELECT COUNT(*) FROM power WHERE Global_intensity <= {val};"
    """
    if EXPERIMANT_NO == 1:
        lb = random.randint(1, 100)
        ub = 785 + random.randint(1, 100)
        sql_txt = f"EXPLAIN (FORMAT JSON)SELECT COUNT(*) FROM custom WHERE Value_1 <= {ub} AND Value_1 >= {lb};"
    elif EXPERIMANT_NO == 2:
        lb = 50
        ub = 818 + lb
        sql_txt = f"EXPLAIN (FORMAT JSON)SELECT COUNT(*) FROM custom WHERE Value_1 <= {ub} AND Value_1 >= {lb};"
    elif EXPERIMANT_NO == 3:
        lb = 0
        ub = 100 + index * 100
        sql_txt = f"EXPLAIN (FORMAT JSON)SELECT COUNT(*) FROM custom WHERE Value_1 <= {ub} AND Value_1 >= {lb};"

    return sql_txt, lb, ub


def create_connection(database_name, cardest_filename=False, query_no=0):
    logger.info(f"Connecting to {database_name}...")
    conn = psycopg2.connect(
        database=database_name,
        host="localhost",
        port=5431,
        password="postgres",
        user="postgres",
    )
    conn.autocommit = True
    cursor = conn.cursor()

    if cardest_filename:
        logger.info(f"Using {cardest_filename} for estimates. Setting up the config...")
        cursor.execute("SET debug_card_est=true;")
        cursor.execute("SET logger.info_sub_queries=true;")

        if RUN_ESTIMATES:
            # Single table queries
            # cursor.execute('SET logger.info_single_tbl_queries=true')
            cursor.execute("SET ml_cardest_enabled=true;")
            cursor.execute(f"SET ml_cardest_fname='{cardest_filename}';")
            cursor.execute(f"SET query_no={query_no};")

    time.sleep(2)
    return conn, cursor


def main(dataset, container_name):
    for cardest_filename in get_all_unprocessed_txt_files(
        container_name, "/var/lib/pgsql/13.1/data/"
    ):
        run_one_file(dataset, cardest_filename)


def run_one_file(dataset, cardest_filename):
    database_name = METADATA[dataset]["database_name"]
    # sql_file = METADATA[dataset]["sql_file"]

    conn, cursor = create_connection(database_name)
    logger.info(f"Processing {cardest_filename}")
    cardest_filepath = Path(cardest_filename)

    export_dirpath = current_dir / f"../plan_cost/{database_name}/"
    export_filepath = export_dirpath / f'{cardest_filepath.stem + "_cost_variable.csv"}'
    logger.info(f"Exporting to {export_filepath}")
    if not export_dirpath.exists():
        export_dirpath.mkdir(parents=True)

    if export_filepath.exists():
        logger.info(f"File {export_filepath} already exists. Deleting...")
        export_filepath.unlink()

    if os.path.exists("/Users/hanyuxing/pgsql/13.1/data/join_est_record_job.txt"):
        os.remove("/Users/hanyuxing/pgsql/13.1/data/join_est_record_job.txt")

    # cursor.execute("SET debug_card_est=true;")
    # cursor.execute("SET logger.info_sub_queries=true;")

    if RUN_ESTIMATES:
        # Single table queries
        # cursor.execute('SET logger.info_single_tbl_queries=true')
        cursor.execute("SET ml_cardest_enabled=true;")
        cursor.execute(f"SET ml_cardest_fname='{cardest_filename}';")
        cursor.execute("SET query_no=0;")

    time.sleep(1)
    dict_list = []

    loop_values = list(range(1, 10))
    for no in tqdm(loop_values):
        sql_txt, lb, ub = get_sql_query(no)
        retry_count = 0
        while True:
            try:
                if retry_count > 0:
                    conn, cursor = create_connection(
                        database_name, cardest_filename, query_no=no
                    )
                cursor.execute(sql_txt)
                res = cursor.fetchall()
                break
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                logger.info(e)
                logger.error("Connection error")
                cursor.close()
                conn.close()
                retry_count += 1
                if retry_count > 5 or not RETRY_CONNECTION_WHEN_FAILED:
                    logger.info("Failed to execute the query.")
                    break
                logger.info("Retrying... ", retry_count)
                time.sleep(3)
                continue
        res_json = res[0][0][0]
        # logger.info(f"{no} - {json.dumps(res_json, indent=4)}")
        total_cost = res_json["Plan"]["Total Cost"]
        scan_type = res_json["Plan"]["Plans"][0]["Node Type"]
        # rows_1 = res_json["Plan"]["Plan Rows"]
        rows_2 = res_json["Plan"]["Plans"][0]["Plan Rows"]
        real_card = ub - lb

        if RUN_ESTIMATES:
            dict_list.append(
                {
                    "index": no,
                    "total_cost_estimates": total_cost,
                    "access_path": scan_type,
                    "input_card_est": rows_2,
                    "real_card": real_card,
                    "upper_bound": ub,
                    "lower_bound": lb,
                }
            )
        else:
            dict_list.append(
                {
                    "index": no,
                    "total_cost_estimates": total_cost,
                    "access_path": scan_type,
                    "input_card_est": rows_2,
                    "real_card": real_card,
                    "upper_bound": ub,
                    "lower_bound": lb,
                }
            )

        # logger.info("Used estimates from ", cardest_filename)
    df = pd.DataFrame(dict_list)

    if export_filepath.exists():
        df_file = pd.read_csv(export_filepath)
        df = pd.concat([df_file, df], axis=1)

    """Write to a csv file"""
    df.to_csv(export_filepath.as_posix(), index=False)

    # Show stats
    logger.info("Total number of queries processed: ", len(df))
    # Show unique access paths
    unique_access_paths_counts = df["access_path"].value_counts()

    logger.info("Table Scan type\tCount")
    for path, count in unique_access_paths_counts.items():
        logger.info(f"{path}\t: {count}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    run_one_file("custom", "/var/lib/pgsql/13.1/data/row_estimate.csv")
