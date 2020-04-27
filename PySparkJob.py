import io
import sys

import pyspark.sql.functions as fn
from pyspark.sql import SparkSession, Window

def readFile(spark, input_file):
    '''read file and add new features to dataframe'''
    parquetFile = spark.read.parquet(input_file)
    parquetFile = parquetFile.distinct() # drop dublicates
    w = Window.partitionBy('ad_id')
    parquetFile = parquetFile.withColumn('day_count', fn.approx_count_distinct('date').over(w))      
    parquetFile = parquetFile.withColumn('is_cpm', (fn.col('ad_cost_type') == 'CPM').cast('integer'))
    parquetFile = parquetFile.withColumn('is_cpc', (fn.col('ad_cost_type') == 'CPC').cast('integer'))

    parquetFile = parquetFile.withColumn('is_click', (fn.col('event') == 'click').cast('integer'))
    parquetFile = parquetFile.withColumn('is_view', (fn.col('event') == 'view').cast('integer'))

    parquetFile = parquetFile.withColumn('CTR', fn.when(fn.sum('is_view').over(w) == 0, 0).otherwise(                                     
                                                        fn.sum('is_click').over(w) / fn.sum('is_view').over(w)))
    return parquetFile


def process(spark, input_file, target_path):               
    
    parquetFile = readFile(spark, input_file) 

    colList = ['ad_id', 
               'target_audience_count',
               'has_video', 
               'is_cpm',
               'is_cpc',
               'ad_cost',
               'day_count',
               'CTR'          
              ]
    parquetFile = parquetFile[colList] # del useless columns   

    # # split parquet file and then write to separate folders
    train, test, validate = parquetFile.randomSplit([0.5, 0.25, 0.25])    
    train.coalesce(1).write.option('header', 'true').parquet(target_path + '/' + 'train')
    test.coalesce(1).write.option('header', 'true').parquet(target_path + '/' + 'test')
    validate.coalesce(1).write.option('header', 'true').parquet(target_path + '/' + 'validate')    

def main(argv):
    input_path = argv[0]
    print("Input path to file: " + input_path)
    target_path = argv[1]
    print("Target path: " + target_path)
    spark = _spark_session()
    process(spark, input_path, target_path)


def _spark_session():
    return SparkSession.builder.appName('PySparkJob').getOrCreate()

if __name__ == "__main__":
    arg = sys.argv[1:] 
    print(arg)
    if len(arg) != 2:
        sys.exit("Input and Target path are require.")
    else:
        main(arg)

