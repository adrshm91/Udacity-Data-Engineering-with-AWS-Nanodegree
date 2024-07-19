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
AccelerometerTrusted_node1721398835937 = glueContext.create_dynamic_frame.from_catalog(database="stedi_data", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1721398835937")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1721398800388 = glueContext.create_dynamic_frame.from_catalog(database="stedi_data", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1721398800388")

# Script generated for node SQL Query
SqlQuery890 = '''
select * from step_trainer_trusted inner join accelerometer_trusted on step_trainer_trusted.sensorreadingtime = accelerometer_trusted.timestamp
'''
SQLQuery_node1721398854323 = sparkSqlQuery(glueContext, query = SqlQuery890, mapping = {"accelerometer_trusted":AccelerometerTrusted_node1721398835937, "step_trainer_trusted":StepTrainerTrusted_node1721398800388}, transformation_ctx = "SQLQuery_node1721398854323")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1721399012170 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1721398854323, database="stedi_data", table_name="machine_learning_curated", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1721399012170")

job.commit()