from pyspark.sql import SparkSession
import json
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType, FloatType
from pyspark.sql.functions import udf


APP_NAME = 'IJHCS special issue (exctract CHI)'

def main():
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    with open('./queries/chi.hql') as query_file:
        query = query_file.read()
        papers = spark.sql(query)
        papers.write.mode('overwrite').saveAsTable('amannocci.chi')
        spark.sql('select * from amannocci.chi').write.mode('overwrite').json('/user/amannocci/CHI.json')

    with open('./queries/chi_ref.hql') as query_file:
        query = query_file.read()
        papers = spark.sql(query)
        papers.write.mode('overwrite').saveAsTable('amannocci.chi_ref')
        spark.sql('select * from amannocci.chi_ref').write.mode('overwrite').json('/user/amannocci/CHI_ref.json')

    with open('./queries/chi_cite.hql') as query_file:
        query = query_file.read()
        papers = spark.sql(query)
        papers.write.mode('overwrite').saveAsTable('amannocci.chi_cite')
        spark.sql('select * from amannocci.chi_cite').write.mode('overwrite').json('/user/amannocci/CHI_cite.json')


if __name__ == '__main__':
    main()
