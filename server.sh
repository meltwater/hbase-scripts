#!/bin/sh

HBASE=/opt/mapr/hbase/hbase-0.90.4/bin/hbase
LOGPATH=/var/log/hbase/logs
PORT=2000

echo "$HBASE org.jruby.Main $( dirname $0 )/migrate.rb -m server -c 15000 -p $PORT >>$LOGPATH/migrate_server_$PORT.log 2>&1 &"
$HBASE org.jruby.Main $( dirname $0 )/migrate.rb -m server -c 15000 -p $PORT >>$LOGPATH/migrate_server_$PORT.log 2>&1 &
