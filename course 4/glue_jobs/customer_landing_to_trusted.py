import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Landing Table
CustomerLandingTable_node1721366085249 = glueContext.create_dynamic_frame.from_catalog(database="stedi_data", table_name="customer_landing", transformation_ctx="CustomerLandingTable_node1721366085249")

# Script generated for node SQL Query
SqlQuery754 = '''
SELECT * FROM myDataSource WHERE sharewithresearchasofdate IS NOT NULL
'''
SQLQuery_node1721367035260 = sparkSqlQuery(glueContext, query = SqlQuery754, mapping = {"myDataSource":CustomerLandingTable_node1721366085249}, transformation_ctx = "SQLQuery_node1721367035260")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1721367265098 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1721367035260, database="stedi_data", table_name="customer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1721367265098")

job.commit()