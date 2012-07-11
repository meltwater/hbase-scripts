#!/bin/sh

MONTH=$1
DAY=$2
SERVER=$3

HBASE=/opt/mapr/hbase/hbase-0.90.4/bin/hbase

if [ -z "$MONTH" -o -z "$DAY" -o -z "$SERVER" ]; then
    echo "Usage: $0 month day server"
    exit 1
fi

ssh $SERVER 'ps auxw | grep migrate.r[b] || /opt/software/hbase-scripts/server.sh'
echo "started server, sleeping 2.."
sleep 2

(
for HOUR in $( seq 23 -1 0 ); do
    DATE=$( date -d "$( printf '2012%02d%02d %02d:00:00' $MONTH $DAY $HOUR )" "+%s" )
    ${HBASE} org.jruby.Main migrate.rb -m client -t $DATE -i 3599 -s $SERVER -p 2000 >>/var/log/hbase/migrate_${MONTH}_${DAY}_${HOUR}.log 2>&1
done
) &
