import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node customer_trusted
customer_trusted_node1745575109846 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-akshaya1/customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1745575109846")

# Script generated for node Accelerometer_landing
Accelerometer_landing_node1745575182497 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-akshaya1/accelerometer/landing/"], "recurse": True}, transformation_ctx="Accelerometer_landing_node1745575182497")

# Script generated for node Join
Join_node1745575227918 = Join.apply(frame1=Accelerometer_landing_node1745575182497, frame2=customer_trusted_node1745575109846, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1745575227918")

# Script generated for node Drop Fields
DropFields_node1745575250862 = DropFields.apply(frame=Join_node1745575227918, paths=["email", "phone"], transformation_ctx="DropFields_node1745575250862")

# Script generated for node Accelerometer_trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1745575250862, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745574153423", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Accelerometer_trusted_node1745575314082 = glueContext.getSink(path="s3://stedi-akshaya1/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Accelerometer_trusted_node1745575314082")
Accelerometer_trusted_node1745575314082.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
Accelerometer_trusted_node1745575314082.setFormat("json")
Accelerometer_trusted_node1745575314082.writeFrame(DropFields_node1745575250862)
job.commit()
