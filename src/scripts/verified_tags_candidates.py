import os
import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

from tools import input_paths

os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

def main():
    date = sys.argv[1]
    depth = int(sys.argv[2])
    threshold = int(sys.argv[3])
    base_input_path = sys.argv[4]
    tags_verified_path = sys.argv[5]
    base_output_path = sys.argv[6]

    conf = SparkConf().setAppName('VerifiedTagsCandidatesJob-{}-d{}-cut{}'.format(date, depth, threshold))
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)
    #sql = SparkSession.builder.master('yarn').appName('VerifiedTagsCandidatesJob-{}-d{}-cut{}'.format(date, depth, threshold)).getOrCreate()

    events = sql.read.parquet(*input_paths(date, depth, base_input_path, 'message'))

    cur_tags = events\
    .filter(F.col("event.tags").\
    isNotNull()).\
    select([F.explode("event.tags"), F.col('event.message_from')]).distinct()

    df_tags = sql.read.parquet(tags_verified_path)

    new_tags = cur_tags.join(df_tags,  cur_tags.col==df_tags.tag, 'leftanti').groupBy("col").\
    count().select(F.col('col').alias('tag'), F.col('count').alias('suggested_count')).filter(F.col('suggested_count')>=threshold)

    new_tags.write.mode('overwrite').parquet(os.path.join(base_output_path, 'verified_tags_candidates_d{}/date={}'.format(depth, date)))


if __name__ == "__main__":
    main()
