#!/bin/bash

# Export the password for non-interactive login
export PGPASSWORD=postgres

# Create a temporary SQL script file with all commands
SQL_SCRIPT="/tmp/commands.sql"
cat <<EOF >$SQL_SCRIPT
CREATE DATABASE custom;
\c custom;
\i /tmp/single_table_datasets/custom/custom.sql;
\i /tmp/scripts/sql/custom_load.sql;
\i /tmp/scripts/sql/custom_index.sql;
EOF

# Copy the script to the container
mkdir -p /tmp/single_table_datasets
mkdir -p /tmp/scripts


cp -r single_table_datasets/* /tmp/single_table_datasets/
cp -r scripts/* /tmp/scripts/

cp $SQL_SCRIPT /tmp/commands.sql
echo "Copied $SQL_SCRIPT to /tmp/commands.sql"

# Commands to execute inside the container
psql -d template1 -h localhost -U postgres -p 5431 -f /tmp/commands.sql

# Cleanup: Remove the temporary SQL script
rm -f $SQL_SCRIPT
