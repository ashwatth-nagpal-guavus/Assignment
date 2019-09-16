data location

input table is read from hive
output table is stored in hive in database names ashwatth
xml data file is at location /tmp/ashwatth/
dictionary for content-type is at location /tmp/ashwatth/tempMap


use following command to run jar

/usr/hdp/2.6.5.0-292/spark2/bin/spark-submit --class <class-name> --master yarn --deploy-mode cluster --num-executors 4 --executor-memory 5G --driver-memory 8G --executor-cores 5 /tmp/ashwatth/guavusAssignment-1.0-SNAPSHOT-jar-with-dependencies.jar
