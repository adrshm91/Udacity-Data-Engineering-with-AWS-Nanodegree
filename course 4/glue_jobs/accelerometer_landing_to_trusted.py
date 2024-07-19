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

# Script generated for node Customer Trusted
CustomerTrusted_node1721392505880 = glueContext.create_dynamic_frame.from_catalog(database="stedi_data", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1721392505880")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1721391763352 = glueContext.create_dynamic_frame.from_catalog(database="stedi_data", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1721391763352")

# Script generated for node SQL Query
SqlQuery649 = '''
select accelerometer_landing.user, accelerometer_landing.timestamp, accelerometer_landing.x, accelerometer_landing.y, accelerometer_landing.z from accelerometer_landing INNER JOIN customer_trusted on accelerometer_landing.user = customer_trusted.email

'''
SQLQuery_node1721392315067 = sparkSqlQuery(glueContext, query = SqlQuery649, mapping = {"accelerometer_landing":AccelerometerLanding_node1721391763352, "customer_trusted":CustomerTrusted_node1721392505880}, transformation_ctx = "SQLQuery_node1721392315067")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1721392845566 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1721392315067, database="stedi_data", table_name="accelerometer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1721392845566")

job.commit()