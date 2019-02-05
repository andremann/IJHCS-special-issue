from pyspark.sql import SparkSession
import json
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType, FloatType
from pyspark.sql.functions import udf


APP_NAME = 'IJHCS special issue'

def main():
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    with open('./queries/ijhcs.hql') as query_file:
        query = query_file.read()
        papers = spark.sql(query)
        papers.write.mode('overwrite').saveAsTable('amannocci.ijhcs')
        spark.sql('select * from amannocci.ijhcs').write.mode('overwrite').json('/user/amannocci/IJHCS.json')

    with open('./queries/ijhcs_ref.hql') as query_file:
        query = query_file.read()
        papers = spark.sql(query)
        papers.write.mode('overwrite').saveAsTable('amannocci.ijhcs_ref')
        spark.sql('select * from amannocci.ijhcs_ref').write.mode('overwrite').json('/user/amannocci/IJHCS_ref.json')

    with open('./queries/ijhcs_cite.hql') as query_file:
        query = query_file.read()
        papers = spark.sql(query)
        papers.write.mode('overwrite').saveAsTable('amannocci.ijhcs_cite')
        spark.sql('select * from amannocci.ijhcs_cite').write.mode('overwrite').json('/user/amannocci/IJHCS_cite.json')


if __name__ == '__main__':
    main()
