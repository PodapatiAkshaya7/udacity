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

# Script generated for node Customer Curated
CustomerCurated_node1745577060460 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-akshaya1/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1745577060460")

# Script generated for node step_trainer_landing
step_trainer_landing_node1745577104717 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-akshaya1/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1745577104717")

# Script generated for node SQL Query
SqlQuery0 = '''
select step_trainer_landing.*
from step_trainer_landing
inner join customer_curated
on step_trainer_landing.serialNumber = customer_curated.serialNumber;

'''
SQLQuery_node1745577179186 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_landing":step_trainer_landing_node1745577104717, "customer_curated":CustomerCurated_node1745577060460}, transformation_ctx = "SQLQuery_node1745577179186")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745577179186, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745577036610", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1745577425551 = glueContext.getSink(path="s3://stedi-akshaya1/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1745577425551")
step_trainer_trusted_node1745577425551.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1745577425551.setFormat("json")
step_trainer_trusted_node1745577425551.writeFrame(SQLQuery_node1745577179186)
job.commit()
