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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1721395702610 = glueContext.create_dynamic_frame.from_catalog(database="stedi_data", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1721395702610")

# Script generated for node Customer Trusted
CustomerTrusted_node1721395564726 = glueContext.create_dynamic_frame.from_catalog(database="stedi_data", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1721395564726")

# Script generated for node SQL Query
SqlQuery590 = '''
select DISTINCT customer_trusted.* from customer_trusted left join accelerometer_trusted on customer_trusted.email = accelerometer_trusted.user
'''
SQLQuery_node1721395727749 = sparkSqlQuery(glueContext, query = SqlQuery590, mapping = {"accelerometer_trusted":AccelerometerTrusted_node1721395702610, "customer_trusted":CustomerTrusted_node1721395564726}, transformation_ctx = "SQLQuery_node1721395727749")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1721396000145 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1721395727749, database="stedi_data", table_name="customer_curated", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1721396000145")

job.commit()