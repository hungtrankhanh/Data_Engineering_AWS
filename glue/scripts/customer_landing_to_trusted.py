import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer_Landing
Customer_Landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://data-lake-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Customer_Landing_node1",
)

# Script generated for node Custome_Filter
Custome_Filter_node2 = Filter.apply(
    frame=Customer_Landing_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Custome_Filter_node2",
)

# Script generated for node Customer_Trusted
Customer_Trusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Custome_Filter_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://data-lake-house/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Customer_Trusted_node3",
)

job.commit()
