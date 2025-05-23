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

# Script generated for node customer_landing
customer_landing_node1745573855787 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-akshaya1/customer/landing/"], "recurse": True}, transformation_ctx="customer_landing_node1745573855787")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource
where sharewithresearchasofdate is not null;
'''
SQLQuery_node1745573903334 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":customer_landing_node1745573855787}, transformation_ctx = "SQLQuery_node1745573903334")

# Script generated for node customer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745573903334, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745573847549", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_trusted_node1745573934814 = glueContext.getSink(path="s3://stedi-akshaya1/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1745573934814")
customer_trusted_node1745573934814.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
customer_trusted_node1745573934814.setFormat("json")
customer_trusted_node1745573934814.writeFrame(SQLQuery_node1745573903334)
job.commit()
