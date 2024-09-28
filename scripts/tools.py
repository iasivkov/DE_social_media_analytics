from datetime import datetime, timedelta
import os

from pyspark.sql.functions import udf

@udf
def mode(x):
    from collections import Counter
    if x:
        return Counter(sorted(x, reverse=True)).most_common(1)[0][0]
    else:
        return None
    
def input_paths(date, depth, base_input_path, event_type):
      
    date = datetime.strptime(date, "%Y-%m-%d")
    paths_list = []
    for i in range(depth):
        cur_date = (date - timedelta(days=i)).strftime("%Y-%m-%d")
        
        path = os.path.join(base_input_path, 'date={}/event_type={}'.format(cur_date, event_type))
        if not os.system('hdfs dfs -stat {}'.format(path)):
            paths_list.append(path)
        
    return paths_list

def input_paths_like(date, depth, base_input_path):
      
    date = datetime.strptime(date, "%Y-%m-%d")
    paths_list = []
    for i in range(depth):
        cur_date = (date - timedelta(days=i)).strftime("%Y-%m-%d")
        
        path = os.path.join(base_input_path, 'date={}/'.format(cur_date))
        if not os.system('hdfs dfs -stat {}'.format(path)):
            paths_list.append(path)
        
    return paths_list