#!/bin/bash

echo "host all all 0.0.0.0/0 md5" >> /gpdata/master/gpseg-1/pg_hba.conf
export MASTER_DATA_DIRECTORY=/gpdata/master/gpseg-1
source /usr/local/greenplum-db/greenplum_path.sh
gpstart -a
psql -d template1 -c "alter user gpadmin password 'pivotal'"
set -e

. /usr/local/bin/setEnv.sh
{
psql -c "CREATE DATABASE ${GREENPLUM_DB};" 

psql -c "CREATE USER ${GREENPLUM_USER_MONITORING} with password '${GREENPLUM_DB_PWD_MONITORING}';"

psql -c "GRANT ALL PRIVILEGES ON DATABASE ${GREENPLUM_DB} to ${GREENPLUM_USER_MONITORING};"

echo "Database ${GREENPLUM_DB} created"
} || {
echo "Database ${GREENPLUM_DB} exist"
}

