SPARK_HOME=/home/ansibler/work/spark/spark-1.1.0-bin-cdh4
$SPARK_HOME/bin/spark-submit  \
  --class "com.zetdata.hero.trial.SimpleApp" \
   --master spark://10.10.0.114:7077 \
   --num-executors 3 --driver-memory 1024m  --executor-memory 1024m \
   target/scala-2.10/spark_word_segement.jar \
   hdfs://10.10.0.114/tmp/zetjob/admin/job320/blk1773/dump_dir \
   hdfs://10.10.0.114/tmp/zetjob/admin/job320/blk1774/dump_dir \
   hdfs://10.10.0.114/tmp/jiaqi-sbt/match_scoreSchema
# --master spark://10.10.0.114:7077  \
 
