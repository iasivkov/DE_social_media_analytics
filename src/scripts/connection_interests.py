from datetime import datetime, timedelta
import sys
import os

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window

from tools import mode, input_paths


def connection_interests(date, depth, base_input_path, base_output_path, session):
    
    sql = session

    like_top = sql.read.parquet(os.path.join(base_output_path.replace('connection', 'user'), 'date={}'.format(date)))
    messages = sql.read.parquet(*input_paths(date, depth, base_input_path, 'message'))

    like_top = like_top.withColumnRenamed('user_id', 'user_top_id')

    messages_to = messages.select(['event.message_from', 'event.message_to']).withColumnRenamed('message_to', 'user_top_id').\
    withColumnRenamed('message_from', 'user_id').distinct().dropna()

    contacts_like_to = messages_to.\
    distinct().\
    join(like_top, 'user_top_id', 'left').drop('user_top_id')

    messages_from = messages.select(['event.message_from', 'event.message_to']).withColumnRenamed('message_from', 'user_top_id').\
    withColumnRenamed('message_to', 'user_id').distinct().dropna()

    contacts_like_from = messages_from.\
    join(like_top, 'user_top_id', 'left').drop('user_top_id')

    contacts_like = contacts_like_to.union(contacts_like_from).\
    select(['user_id', 'like_tag_top_1', 'like_tag_top_2', 'like_tag_top_3',
            'dislike_tag_top_1', 'dislike_tag_top_2', 'dislike_tag_top_3'])

    cols = ['like_tag_top_1', 'like_tag_top_2', 'like_tag_top_3',
        'dislike_tag_top_1', 'dislike_tag_top_2', 'dislike_tag_top_3']
    agg_expr = [mode(F.collect_list(F.col(column))).alias(column) for column in cols]
    final_df_contacts = contacts_like.groupBy('user_id').agg(*agg_expr)

    df_tags = sql.read.parquet('/user/iasivkov/data/snapshots/tags_verified/actual')
    channel_tags = messages.filter(F.col('event.message_channel_to').isNotNull()).\
    filter(F.col("event.tags").isNotNull()).\
    select([F.explode("event.tags").alias('tag'), F.col('event.message_channel_to')]).join(df_tags, 'tag', 'inner')

    channel_tags_agg = channel_tags.groupBy(['message_channel_to', 'tag']).count()

    events = sql.read.parquet(base_input_path)
    user_channel = events.filter('event_type=="subscription"').filter(F.col('event.subscription_channel').isNotNull()).\
    select(['event.subscription_channel', 'event.user']).withColumnRenamed('user', 'user_id')
    #.join(final_df_contacts.select('user_id'), 'user_id', 'inner')
    
    user_channel_tag = user_channel.join(channel_tags_agg, user_channel.subscription_channel==channel_tags.message_channel_to, 'inner').\
    drop('subscription_channel').drop('message_channel_to')

    window = Window.partitionBy('user_id').orderBy([F.desc('count'),F.desc('tag')])
    user_channel_tag = user_channel_tag.withColumn("top", F.row_number().over(window))
    user_channel_tag = user_channel_tag.filter(user_channel_tag.top<=3)

    df_top_1 = user_channel_tag.filter(user_channel_tag.top==1).withColumnRenamed('tag','tag_top_1')
    df_top_2 = user_channel_tag.filter(user_channel_tag.top==2).withColumnRenamed('tag','tag_top_2')
    df_top_3 = user_channel_tag.filter(user_channel_tag.top==3).withColumnRenamed('tag','tag_top_3')


    final_df_channel = user_channel.select('user_id').distinct().join(df_top_1, 'user_id', how='left').\
    join(df_top_2, 'user_id', how='left').\
    join(df_top_3, 'user_id', how='left').\
    select(['user_id', 'tag_top_1', 'tag_top_2', 'tag_top_3']).dropna(subset=['tag_top_1', 'tag_top_2', 'tag_top_3'], how='all').\
    withColumn('date', F.lit(date))
    
    return final_df_contacts.join(final_df_channel, 'user_id', how='full_outer')
    
def main():
    date = sys.argv[1]
    depth = int(sys.argv[2])
    base_input_path = sys.argv[3]
    base_output_path = sys.argv[4]
    
    os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
    os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
    
    conf = SparkConf().setAppName('ConnectionsJob-{}-d{}'.format(date, depth))
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)
    
    connection_interests(date, depth, base_input_path, base_output_path, sql).write.mode('overwrite').partitionBy('date').parquet(os.path.join(base_output_path))
    
if __name__ == "__main__":
    main()
