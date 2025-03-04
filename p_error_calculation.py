import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd


def calculate_p_error(estimates_cost_file_path, true_cost_file_path):
    df_estimates = pd.read_csv(estimates_cost_file_path)
    df_true = pd.read_csv(true_cost_file_path)
    
    cost_estimates = df_estimates["total_cost_estimates"].values
    cost_true = df_true["total_cost_estimates"].values

    p_errors = np.maximum(cost_estimates, cost_true) / np.minimum(cost_estimates, cost_true)

    access_path_estimates = df_estimates["access_path"].values
    access_path_true = df_true["access_path"].values

    """Check whether the access paths are the same and get a count of the same access paths"""
    same_access_paths = 0
    for i in range(len(access_path_estimates)):
        if access_path_estimates[i] == access_path_true[i]:
            same_access_paths += 1

    # df["p_error"] = df.apply(
    #     lambda row: max(row["total_cost_estimates"], row["total_cost_true"]) / 
    #     (min(row["total_cost_estimates"], row["total_cost_true"])), axis=1)
    
    """Calculate the percentiles of p_error"""
    percentiles_values = [50, 90, 95, 99, 100]
    
    result_file_path = Path(estimates_cost_file_path).with_suffix(".txt")
    with open(result_file_path, "a") as f:
        f.write(f"Estimates cost file: {estimates_cost_file_path}\n")
        f.write(f"True cost file: {true_cost_file_path}\n")
        f.write(f"Total number of queries: {len(p_errors)}\n")
        f.write(f"Total number of same access paths: {same_access_paths}\n")
        f.write(f"Percentiles of p_error\n")
        for percentile in percentiles_values:
            value = np.percentile(p_errors, percentile)
            print(f"Percentile ({percentile}th): {value}")
            f.write(f"{percentile}th percentile: {value}\n")
        f.write("\n")

    print(f"Same access paths: {(same_access_paths)}")

def calculate_p_error_for_db(database_name, csv_file_path="scripts/plan_cost"):
    dir_path = Path(f"{csv_file_path}/{database_name}")
    if not dir_path.exists():
        return -1
    
    files = list(dir_path.glob("*true_card_cost.csv"))
    assert len(files) == 1
    true_cost_file_path = files[0]

    all_dict_list = []
    for estimates_cost_file_path in dir_path.glob("*.csv"):
        if true_cost_file_path == estimates_cost_file_path:
            continue
        print(f"Calculating p_error for {estimates_cost_file_path}")

        calculate_p_error(estimates_cost_file_path.as_posix(), true_cost_file_path.as_posix())   

if __name__ == "__main__":
    # estimates_cost_file_path = "/home/titan/phd/megadrive/End-to-End-CardEst-Benchmark/plan costs/census/census_cost_mscn.xlsx"
    # true_cost_file_path = "/home/titan/phd/megadrive/End-to-End-CardEst-Benchmark/plan costs/census/census_cost_true.xlsx"
    # calculate_p_error(estimates_cost_file_path, true_cost_file_path)


    parser = argparse.ArgumentParser(description="Execute SQL queries and export cost estimates.")
    parser.add_argument("--database_name", default="synthetic_correlated_2", type=str, help="The path to the estimates cost file.")
    parser.add_argument("--csv_file_path", default="scripts/plan_cost", type=str, help="The path to the true cost file.")
    args = parser.parse_args()
    calculate_p_error_for_db(args.database_name, args.csv_file_path)
    # estimates_cost_file_path = args.estimates_cost_file_path
    # true_cost_file_path = args.true_cost_file_path
    
