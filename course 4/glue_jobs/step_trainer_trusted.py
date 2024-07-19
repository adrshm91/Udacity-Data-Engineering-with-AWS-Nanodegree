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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1721397822057 = glueContext.create_dynamic_frame.from_catalog(database="stedi_data", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1721397822057")

# Script generated for node Customer Curated
CustomerCurated_node1721397968430 = glueContext.create_dynamic_frame.from_catalog(database="stedi_data", table_name="customer_curated", transformation_ctx="CustomerCurated_node1721397968430")

# Script generated for node SQL Query
SqlQuery594 = '''
select * from step_trainer_landing inner join customer_curated on step_trainer_landing.serialnumber = customer_curated.serialnumber

'''
SQLQuery_node1721397983748 = sparkSqlQuery(glueContext, query = SqlQuery594, mapping = {"step_trainer_landing":StepTrainerLanding_node1721397822057, "customer_curated":CustomerCurated_node1721397968430}, transformation_ctx = "SQLQuery_node1721397983748")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1721398520916 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1721397983748, database="stedi_data", table_name="step_trainer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1721398520916")

job.commit()