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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1745575702684 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1745575702684")

# Script generated for node Customer Trusted
CustomerTrusted_node1745575740229 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1745575740229")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct customer_trusted.*
from accelerometer_trusted 
inner join customer_trusted
on accelerometer_trusted.user = customer_trusted.email;

'''
SQLQuery_node1745575768121 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_trusted":CustomerTrusted_node1745575740229, "accelerometer_trusted":AccelerometerTrusted_node1745575702684}, transformation_ctx = "SQLQuery_node1745575768121")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745575768121, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745575692357", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1745575980967 = glueContext.getSink(path="s3://stedi-akshaya1/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1745575980967")
customer_curated_node1745575980967.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
customer_curated_node1745575980967.setFormat("json")
customer_curated_node1745575980967.writeFrame(SQLQuery_node1745575768121)
job.commit()
