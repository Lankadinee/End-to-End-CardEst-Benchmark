#!/bin/bash

# Check if a container name/ID is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <container_name_or_id>"
    exit 1
fi

CONTAINER_ID=$1

# Export the password for non-interactive login
export PGPASSWORD=postgres

# Create a temporary SQL script file with all commands
SQL_SCRIPT="/tmp/commands.sql"
cat <<EOF >$SQL_SCRIPT
CREATE DATABASE stats;
\c stats
\i datasets/stats_simplified/stats.sql
\i scripts/sql/stats_load.sql
\i scripts/sql/stats_index.sql
EOF

# Copy the script to the container
docker cp $SQL_SCRIPT $CONTAINER_ID:/tmp/commands.sql

# Copy Scripts and datasets directory to the container
docker cp datasets $CONTAINER_ID:datasets
docker cp scripts $CONTAINER_ID:scripts

# Commands to execute inside the container
COMMANDS="psql -d template1 -h localhost -U postgres -f /tmp/commands.sql"

# Attach to the container and run commands
docker exec -it $CONTAINER_ID bash -c "$COMMANDS"

# Cleanup: Remove the temporary SQL script
rm -f $SQL_SCRIPT
