#!/bin/bash

# Check if a container name/ID is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <container_name_or_id>"
    exit 1
fi

DATABASE_NAME=$1
CONTAINER_NAME=$2

echo "Attach and Run: Creating Database: $DATABASE_NAME"
echo "Attach and Run: Container Name: $CONTAINER_NAME"

if [ $DATABASE_NAME == 'forest' ]; then
    echo "Create Forest Database"
    bash scripts/sh/attach_and_run_forest.sh $CONTAINER_NAME
elif [ $DATABASE_NAME == 'stats' ]; then
    echo "Create Stats Database"
    bash scripts/sh/attach_and_run_stats.sh $CONTAINER_NAME
elif [ $DATABASE_NAME == 'census' ]; then
    echo "Create Census Database"
    bash scripts/sh/attach_and_run_census.sh $CONTAINER_NAME
elif [ $DATABASE_NAME == 'power' ]; then
    echo "Create Power Database"
    bash scripts/sh/attach_and_run_power.sh $CONTAINER_NAME
elif [ $DATABASE_NAME == 'dmv' ]; then
    echo "Create DMV Database"
    bash scripts/sh/attach_and_run_dmv.sh $CONTAINER_NAME
elif [[ "$DATABASE_NAME" == "tpch_sf2_z1" || "$DATABASE_NAME" == "tpch_sf2_z2" || "$DATABASE_NAME" == "tpch_sf2_z3" || "$DATABASE_NAME" == "tpch_sf2_z4" || "$DATABASE_NAME" == "tpch_lineitem_10" || "$DATABASE_NAME" == "tpch_lineitem_20" ]]; then
    echo "Create TPCH Database"
    bash scripts/sh/attach_and_run_tpch_lineitem.sh $CONTAINER_NAME $DATABASE_NAME
elif [[ "$DATABASE_NAME" == "synthetic_correlated_2" || "$DATABASE_NAME" == "synthetic_correlated_3" || "$DATABASE_NAME" == "synthetic_correlated_4" || "$DATABASE_NAME" == "synthetic_correlated_6"  || "$DATABASE_NAME" == "synthetic_correlated_8" || "$DATABASE_NAME" == "synthetic_correlated_10" ]]; then
    echo "Create Synthetic Correlated Database"
    bash scripts/sh/attach_and_run_synthetic_correlated.sh $CONTAINER_NAME $DATABASE_NAME
elif [ $DATABASE_NAME == 'custom' ]; then
    echo "Create Custom Database"
    bash scripts/sh/attach_and_run_custom.sh $CONTAINER_NAME
else
    echo "Usage: $0 <container_name_or_id> <database_name>"
    exit 1
fi