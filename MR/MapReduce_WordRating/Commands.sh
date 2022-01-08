OUT_DIR="assignment"
OUT_DIR_2="result_assignment"


hdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.jab.name="Streaming wordCount" \
    -D mapreduce.job.reduces=8 \
    -files mapper1.py,reducer1.py \
    -mapper "python mapper1.py" \
    -combiner "python reducer1.py" \
    -reducer "python reducer1.py" \
    -input /data/wiki/en_articles_part \
    -output ${OUT_DIR} > /dev/null


hdfs dfs -rm -r -skipTrash ${OUT_DIR_2} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.jab.name="Streaming wordCount Rating" \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D map.output.key.field.separator=\t \
    -D mapreduce.partition.keycomparator.options=-k1nr \
    -D mapreduce.job.reduces=1 \
    -files mapper_rating.py,reducer_rating.py \
    -mapper "python mapper_sort_job.py" \
    -reducer "python reducer_reduce_job.py" \
    -input ${OUT_DIR} \
    -output ${OUT_DIR_2} > /dev/null

hdfs dfs -cat ${OUT_DIR_2}/part-00000 | sed -n '7p;8q'
hdfs dfs -rm -r -skipTrash ${OUT_DIR_2}* > /dev/null