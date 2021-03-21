from pyspark import SparkConf, sql, SparkContext
from configparser import ConfigParser

def load_par():
    parser = ConfigParser()
    parser.read('./cfg.ini')
    file_desc = parser.get('', 'model')
    return file_desc

def gen_df (path_file):
    '''

    :param path_file:
    :return:
    '''

    df_gen = sqlContext\
        .read\
        .format('com.databricks.spark.csv')\
        .options(header='true', inferschema='true')\
        .load("./tables/accounts/part-00000-tid-4727103262398151985-402ef528-4c81-4834-b4ad-541c0da72cd8-5636803-1-c000.csv")


if __name__ == '__main__':
    conf = SparkConf().setAppName("Prepare CSV")
    sc = SparkContext(conf=conf)
    sqlContext = sql.SQLContext(sc)

    df_acc = sqlContext\
        .read\
        .format('com.databricks.spark.csv')\
        .options(header='true', inferschema='true')\
        .load("./tables/accounts/part-00000-tid-4727103262398151985-402ef528-4c81-4834-b4ad-541c0da72cd8-5636803-1-c000.csv")

    df_city = sqlContext\
        .read\
        .format('com.databricks.spark.csv')\
        .options(header='true', inferschema='true')\
        .load("./tables/city/part-00000-tid-3016927596101492886-d03e2169-f4e8-4c57-a2b3-a56d5168275d-5636806-1-c000.csv")


    df.show()

