#!/usr/bin/env bash
set -x

PG_TARBALL_DIR="postgresql-13.1"

if [ ! -d "$PG_TARBALL_DIR" ]; then
	wget https://ftp.postgresql.org/pub/source/v13.1/postgresql-13.1.tar.bz2
	tar xvf postgresql-13.1.tar.bz2 && cd postgresql-13.1
	patch -s -p1 < ../benchmark.patch && cd ..
	# tar cvf postgresql-13.1.tar.gz postgresql-13.1
else
    echo "PostgreSQL $PG_VERSION archive already exists. Skipping download."
fi
