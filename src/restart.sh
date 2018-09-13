#!/bin/bash
#Quick restart script for dev use
iquery -aq "unload_library('de_rle')" > /dev/null 2>&1
set -e

DBNAME="mydb"
#This is easily sym-linkable: ~/scidb
SCIDB_INSTALL=`dirname ~/.`"/scidb"
export SCIDB_THIRDPARTY_PREFIX="/opt/scidb/18.1"

mydir=`dirname $0`
pushd $mydir
make SCIDB=$SCIDB_INSTALL

scidb.py stopall $DBNAME 
cp libde_rle.so ${SCIDB_INSTALL}/lib/scidb/plugins/
scidb.py startall $DBNAME 

iquery -aq "load_library('de_rle')"
