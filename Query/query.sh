#!/bin/bash
set -e
POSTGRES_HOST=postgres://docker:docker@localhost:5333/meteo_data;
QUERY=$(cat ./Query/query.sql);
MYDATE=$(date +"%Y%m%d");

RASULTSPATH=./query_results;
if [ ! -d $RASULTSPATH ]
then
    mkdir -p $RASULTSPATH;
    chmod +x -R $RASULTSPATH;
fi;

RESFILENAME=$RASULTSPATH"/result_file"$MYDATE".txt";

echo $MYDATE;
echo $RESFILENAME;
echo "===============";
echo $QUERY;
echo "===============";
echo psql $POSTGRES_HOST -X --single-transaction --quiet -c $QUERY;
echo "===============";
psql $POSTGRES_HOST --quiet -c "$QUERY" > $RESFILENAME 2>&1;
echo "===============";
echo "=======RESULT==";
cat $RESFILENAME;
echo "===============";
echo "result in $RESFILENAME";
exit
