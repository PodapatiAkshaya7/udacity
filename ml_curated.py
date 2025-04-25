import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node step_trainer Trusted
step_trainerTrusted_node1745577743867 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainerTrusted_node1745577743867")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1745577783652 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1745577783652")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct step_trainer_trusted.sensorreadingtime,
step_trainer_trusted.serialnumber,
step_trainer_trusted.distancefromobject,
accelerometer_Trusted.user,
accelerometer_Trusted.x,
accelerometer_Trusted.y,
accelerometer_Trusted.z
from step_trainer_trusted
join accelerometer_Trusted
on accelerometer_Trusted.timestamp=step_trainer_trusted.sensorreadingtime;
'''
SQLQuery_node1745577827324 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"Accelerometer_Trusted":AccelerometerTrusted_node1745577783652, "step_trainer_trusted":step_trainerTrusted_node1745577743867}, transformation_ctx = "SQLQuery_node1745577827324")

# Script generated for node ml_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745577827324, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745577709251", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
ml_curated_node1745578171713 = glueContext.getSink(path="s3://stedi-akshaya1/ml/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="ml_curated_node1745578171713")
ml_curated_node1745578171713.setCatalogInfo(catalogDatabase="stedi",catalogTableName="ml_curated")
ml_curated_node1745578171713.setFormat("json")
ml_curated_node1745578171713.writeFrame(SQLQuery_node1745577827324)
job.commit()
