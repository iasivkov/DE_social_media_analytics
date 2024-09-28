import sys
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window
from datetime import datetime, timedelta

from tools import input_paths
    
def calculate_user_interests(date, depth, session):
    
    base_input_path = '/user/iasivkov/data/events'
    sql = session
    #print(input_paths(date, depth, base_input_path, 'reaction'))
    reactions = sql.read.parquet(*input_paths(date, depth, base_input_path, 'reaction'))
    messages = sql.read.parquet(base_input_path).filter(F.col('event_type')=='message')
    react_messages = reactions.select(['event.message_id', 'event.reaction_from', 'event.reaction_type']).\
    join(messages.select(['event.message_id', 'event.tags']),'message_id')
    
    react_messages = react_messages.\
    select([F.explode("tags"), F.col('reaction_from'), F.col('reaction_type'), F.col('message_id')])
    
    
    user_react = react_messages.groupBy(['reaction_from', 'reaction_type', 'col']).count()

    window = Window.partitionBy(['reaction_from', 'reaction_type']).orderBy([F.desc('count'),F.desc('col')])
    user_react = user_react.withColumn("top", F.row_number().over(window))

    df_top_1_like = user_react.filter((user_react.top==1) & (user_react.reaction_type=='like')).withColumnRenamed('col','like_tag_top_1')
    df_top_2_like = user_react.filter((user_react.top==2) & (user_react.reaction_type=='like')).withColumnRenamed('col','like_tag_top_2')
    df_top_3_like = user_react.filter((user_react.top==3) & (user_react.reaction_type=='like')).withColumnRenamed('col','like_tag_top_3')

    df_top_1_dislike = user_react.filter((user_react.top==1) & (user_react.reaction_type=='dislike')).withColumnRenamed('col','dislike_tag_top_1')
    df_top_2_dislike = user_react.filter((user_react.top==2) & (user_react.reaction_type=='dislike')).withColumnRenamed('col','dislike_tag_top_2')
    df_top_3_dislike = user_react.filter((user_react.top==3) & (user_react.reaction_type=='dislike')).withColumnRenamed('col','dislike_tag_top_3')

    final_df_reaction = user_react.select('reaction_from').distinct().join(df_top_1_like, 'reaction_from', how='left').\
    join(df_top_2_like, 'reaction_from', how='left').\
    join(df_top_3_like, 'reaction_from', how='left').\
    join(df_top_1_dislike, 'reaction_from', how='left').\
    join(df_top_2_dislike, 'reaction_from', how='left').\
    join(df_top_3_dislike, 'reaction_from', how='left').\
    select(['reaction_from', 'like_tag_top_1', 'like_tag_top_2', 'like_tag_top_3', 'dislike_tag_top_1', 'dislike_tag_top_2', 'dislike_tag_top_3']).\
    withColumnRenamed('reaction_from','user_id')
    
    
    messages = sql.read.parquet(*input_paths(date, depth, base_input_path, 'message'))
    user_tags = messages\
    .filter(F.col("event.tags").\
    isNotNull()).\
    select([F.explode("event.tags"), F.col('event.message_from')])

    user_messages = user_tags.groupBy(['message_from', 'col']).count()

    window = Window.partitionBy('message_from').orderBy([F.desc('count'),F.desc('col')])
    user_messages = user_messages.withColumn("top", F.row_number().over(window))
    user_messages.filter(user_messages.top<=3)

    df_top_1 = user_messages.filter(user_messages.top==1).withColumnRenamed('col','tag_top_1')
    df_top_2 = user_messages.filter(user_messages.top==2).withColumnRenamed('col','tag_top_2')
    df_top_3 = user_messages.filter(user_messages.top==3).withColumnRenamed('col','tag_top_3')


    final_df_message = user_messages.select('message_from').distinct().join(df_top_1, 'message_from', how='left').\
    join(df_top_2, 'message_from', how='left').\
    join(df_top_3, 'message_from', how='left').\
    select(['message_from', 'tag_top_1', 'tag_top_2', 'tag_top_3']).withColumnRenamed('message_from','user_id').\
    withColumn('date', F.lit(date))
    
    return final_df_message.join(final_df_reaction, 'user_id', how='full_outer')
    
def main():
    date = sys.argv[1]
    depth = int(sys.argv[2])
    base_input_path = sys.argv[3]
    base_output_path = sys.argv[4]
    
    os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
    os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
    
    #sql = SparkSession.builder.master('yarn').appName('UserTagsJob-{}-d{}'.format(date, depth)).getOrCreate()
    conf = SparkConf().setAppName('InterestsJob-{}-d{}'.format(date, depth))
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)
    
    calculate_user_interests(date, depth, sql).write.mode('overwrite').partitionBy('date').parquet(os.path.join(base_output_path))
    
if __name__ == "__main__":
    main()
