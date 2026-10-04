#!/bin/sh
# hot standby: clone the primary with pg_basebackup (-R writes standby.signal + primary_conninfo)
set -e
export PGPASSWORD="$REPLICATION_PASSWORD"
mkdir -p "$PGDATA"
chown -R postgres:postgres "$PGDATA"
chmod 700 "$PGDATA"
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  until gosu postgres pg_basebackup -d "host=db-primary port=5432 user=replicator application_name=replica1" \
        -D "$PGDATA" -X stream -R -c fast; do
    echo "replica: waiting for primary..."; sleep 2
  done
fi
exec gosu postgres postgres -c hot_standby=on -c max_connections=200 -c listen_addresses='*'
